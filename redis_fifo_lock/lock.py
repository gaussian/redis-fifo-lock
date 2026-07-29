"""
A FIFO mutex on Redis.

    lock = FifoLock(client, "invoices")
    async with lock.hold() as lease:
        ...                       # exactly one caller is here at a time
        do_work(fence=lease.fence)

What it guarantees, against a single Redis:

* **Mutual exclusion**, provided every holder's process can run a coroutine at
  least once per ``lease_ms`` and can reach Redis.
* **Strict FIFO by arrival at Redis** — not by the order callers started, which
  is not knowable and never can be.
* **Crash recovery within ``lease_ms``, independent of how long the lock is
  held.** A ten-minute critical section and a two-minute crash-detection window
  coexist, because a live holder renews its own lease.

What it refuses to promise, and cannot:

* **Exclusion under network partition, SIGSTOP, or any freeze longer than
  ``lease_ms``.** No lease-based lock provides this — not this one, not Redlock.
  The holder keeps running while its lease expires underneath it. The only
  defence is to pass ``lease.fence`` to whatever you are protecting and have
  *that* reject stale tokens. This library cannot enforce it for you.
* **Anything across a Redis failover that loses writes**, including FIFO order.
  If you need that, you need consensus: etcd, ZooKeeper.
"""

import asyncio
import logging
import uuid
from typing import Optional

import redis.asyncio as redis

from redis_fifo_lock.common import blpop_block_seconds, effective_socket_timeout
from redis_fifo_lock.scripts import ACQUIRE, BEAT, RELEASE

logger = logging.getLogger(__name__)

DEFAULT_LEASE_MS = 60_000
DEFAULT_POLL_MS = 30_000

#: Renewals per lease. Four means three may fail before anyone thinks we died.
BEATS_PER_LEASE = 4

#: Total time a lock may be renewed before the lease is allowed to lapse.
#:
#: A caller can vanish without its process dying — a job abandoned halfway, an
#: exception between acquire and release, a task dropped on the floor. Renewal
#: would then keep the lock alive on behalf of somebody who is never coming
#: back, and the only cure would be restarting the process. This caps that at
#: something an operator can wait out.
DEFAULT_MAX_HOLD_MS = 1_800_000  # 30 minutes

#: How late a renewal may be scheduled before we suspect the event loop is
#: being starved. Renewal is what keeps a live holder from being evicted, so
#: silence here is worth a warning: it means the lease assumption is eroding.
RENEWAL_LATENESS_ALARM = 0.5  # fraction of the lease


class LeaseLost(Exception):
    """
    Raised by ``release()`` when the lock was no longer ours.

    This is not housekeeping. It means the lease expired while the critical
    section was still running, so another caller may have been inside at the
    same time. Whatever the section did should be treated as suspect.
    """


class Lease:
    """A held lock. ``fence`` strictly increases with every grant, ever."""

    __slots__ = ("owner", "fence", "pos", "lost", "_beat")

    def __init__(self, owner: str, fence: int, pos: int):
        self.owner = owner
        self.fence = fence
        #: Queue position assigned on arrival. Grants follow ascending pos —
        #: this is the number FIFO is actually defined against, and it is not
        #: the fence, which is minted later at grant time.
        self.pos = pos
        #: Set if a renewal is refused — i.e. we are no longer the holder.
        self.lost = asyncio.Event()
        self._beat = None

    def __iter__(self):
        # Keeps `owner, fence = await lock.acquire()` working.
        return iter((self.owner, self.fence))

    def __repr__(self):
        return f"<Lease owner={self.owner[:8]} fence={self.fence}>"


class FifoLock:
    """
    Args:
        r: Your async Redis client. Used as given; no configuration is imposed.
        name: Namespace for this lock. Required on purpose — a shared default
            means two unrelated services on one Redis silently share a lock.
        lease_ms: How long a grant survives without renewal. This bounds crash
            detection, **not** how long you may hold the lock. It must exceed
            the longest stretch during which your holder cannot run a coroutine
            — a blocking CPU call, a GC pause, a container CPU throttle. That is
            the one setting whose value can cost you correctness.
        max_hold_ms: Total time the lock may be renewed before the lease is
            allowed to lapse (default 30 minutes). This is the backstop for a
            caller that vanishes without its process dying — an abandoned job,
            an exception between acquire and release. Set it above your longest
            legitimate critical section; without it, one abandoned caller wedges
            this lock name until the process restarts.
        poll_ms: How often a waiter re-checks with the server. Latency only: the
            doorbell normally wakes a waiter immediately, and this is the
            backstop for a doorbell that was lost. Raising it cuts steady-state
            load roughly linearly — at 10,000 waiters, 2.5s costs ~4,000
            script calls/s where 30s costs ~330.
    """

    def __init__(
        self,
        r: redis.Redis,
        name: str,
        lease_ms: int = DEFAULT_LEASE_MS,
        poll_ms: int = DEFAULT_POLL_MS,
        max_hold_ms: int = DEFAULT_MAX_HOLD_MS,
    ):
        if not name:
            raise ValueError("name is required; a shared default lock name is a bug")
        if max_hold_ms <= lease_ms:
            raise ValueError("max_hold_ms must be longer than lease_ms")

        self.r = r
        self.name = name
        self.lease_ms = lease_ms
        self.poll_ms = poll_ms
        self.max_hold_ms = max_hold_ms

        # One hash tag so every key lands in the same Redis Cluster slot.
        self._prefix = f"{{{name}}}:"
        self._keys = [self._prefix + s for s in ("q", "h", "seq", "fence", "last")]

        self._acquire = r.register_script(ACQUIRE)
        self._release = r.register_script(RELEASE)
        self._beat = r.register_script(BEAT)
        self._socket_timeout = effective_socket_timeout(r)
        self._eviction_checked = False

    # -- plumbing -----------------------------------------------------------

    def _sig_key(self, owner: str) -> str:
        return f"{self._prefix}s:{owner}"

    def _args(self, owner: str, extra: str):
        return [self._prefix, owner, self.lease_ms, extra]

    @staticmethod
    def _txt(value) -> str:
        """Scripts reply in bytes or str depending on the caller's client."""
        return value.decode() if isinstance(value, bytes) else value

    async def _call(self, script, owner: str, extra: str):
        reply = await script(keys=self._keys, args=self._args(owner, extra))
        if isinstance(reply, list):
            return [self._txt(v) for v in reply]
        return reply

    async def _check_eviction_policy(self) -> None:
        """
        Refuse to run where Redis may evict the lock out from under us.

        The lease key is one of the only keys here that carries a TTL, which
        makes it a *preferred* victim under any ``volatile-*`` policy rather
        than merely a possible one. Evicting it silently frees a held lock and
        hands it to the next waiter while the holder is still working — with
        the queue intact, so nothing looks wrong.

        This is not hypothetical: ElastiCache ships ``volatile-lru`` by default,
        where Redis itself defaults to ``noeviction``. Client connections count
        toward the memory ceiling too, so a large waiter population creates the
        very pressure that triggers it.

        Managed Redis often restricts or renames CONFIG, so a policy we cannot
        read is a warning, not a refusal.
        """
        if self._eviction_checked:
            return
        self._eviction_checked = True
        try:
            policy = (await self.r.config_get("maxmemory-policy")).get(
                "maxmemory-policy"
            )
        except Exception:
            logger.warning(
                "lock %s: could not read maxmemory-policy; ensure this Redis "
                "cannot evict keys, or the lock can be silently released",
                self.name,
            )
            return
        if policy and policy != "noeviction":
            # Warn rather than refuse. Eviction is a real hazard, but it is not
            # a silent one here: losing the lease key makes the next renewal
            # fail, so the holder learns within one beat and release() raises.
            # Refusing to start would take an application down over a setting
            # its authors may not control, for a failure the lock already
            # reports.
            logger.error(
                "lock %s: redis maxmemory-policy is %r. This lock's lease key "
                "carries a TTL, so it is an eviction candidate under memory "
                "pressure, and evicting it releases a held lock. Prefer "
                "'noeviction' on this instance, or give the lock its own.",
                self.name,
                policy,
            )

    # -- public API ---------------------------------------------------------

    async def acquire(self, timeout: Optional[float] = None) -> Lease:
        """
        Join the queue and wait for the lock.

        Args:
            timeout: Seconds to wait for the lock. None waits forever. This
                bounds the wait, never the hold.

        Raises:
            asyncio.TimeoutError: the wait ran out. Your ticket is withdrawn.
        """
        await self._check_eviction_policy()

        owner = uuid.uuid4().hex
        sig_key = self._sig_key(owner)
        loop = asyncio.get_event_loop()
        deadline = None if timeout is None else loop.time() + timeout
        pos = ""

        try:
            status, fence, reported = await self._call(self._acquire, owner, pos)
            last_poll = loop.time()

            while status != "HELD":
                # Only a WAIT reply carries a real position. Feeding anything
                # else back would re-enqueue us at a bogus score.
                pos = reported

                remaining = None
                if deadline is not None:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError(
                            "acquire timed out waiting for the lock"
                        )

                block = blpop_block_seconds(
                    self.poll_ms, self._socket_timeout, remaining
                )
                try:
                    rang = await self.r.blpop(sig_key, timeout=block)
                except redis.TimeoutError:
                    # The client's read deadline, not ours. Same meaning as a
                    # nil reply: nobody rang. See d27aa06.
                    rang = None

                # A stock redis-py 8 client forces BLPOP to return every ~2.5s
                # whatever poll_ms says, so waking is not the same event as
                # polling. Without this, poll_ms would be a knob that does
                # nothing and 10,000 waiters would hammer the server.
                if rang is None and (loop.time() - last_poll) < self.poll_ms / 1000:
                    continue

                last_poll = loop.time()
                status, fence, reported = await self._call(self._acquire, owner, pos)

            lease = Lease(owner, int(fence), int(reported or pos or 0))
            lease._beat = asyncio.create_task(self._renew(lease))
            return lease

        except BaseException:
            # Includes CancelledError: never leave a ticket, or a lock we were
            # granted on the way out, behind us. Shielded so a cancellation
            # cannot abort the cleanup mid-flight; if the loop is tearing down
            # and the cleanup never lands, the abandoned ticket costs the next
            # waiter one lease and then clears itself.
            cleanup = asyncio.ensure_future(self._abandon(owner))
            try:
                await asyncio.shield(cleanup)
            except (asyncio.CancelledError, Exception):
                pass
            raise

    async def release(self, lease: Lease) -> None:
        """
        Give up the lock and hand it to the next in line.

        Raises:
            LeaseLost: we were not the holder any more. Someone else may have
                been in the critical section at the same time.
        """
        self._stop_renew(lease)
        status, _ = await self._call(self._release, lease.owner, str(lease.fence))
        if status == "STALE":
            raise LeaseLost(
                f"lease {lease.fence} expired before release; another caller may "
                f"have entered the critical section while it was still running"
            )

    def hold(self, timeout: Optional[float] = None):
        """``async with lock.hold() as lease:`` — acquires, always releases."""
        return _Hold(self, timeout)

    async def stats(self) -> dict:
        """Who holds it, and how many are waiting. For humans at 3am."""
        holder = await self.r.get(self._keys[1])
        holder = self._txt(holder) if holder else None
        owner, _, fence = (holder or "").partition(":")
        return {
            "held": holder is not None,
            "owner": owner or None,
            "fence": int(fence) if fence else None,
            "lease_ms_remaining": await self.r.pttl(self._keys[1]),
            "waiting": await self.r.zcard(self._keys[0]),
        }

    # -- internals ----------------------------------------------------------

    async def _abandon(self, owner: str) -> None:
        """Withdraw a ticket. If we were granted the lock meanwhile, release it."""
        try:
            status, fence = await self._call(self._release, owner, "")
            if status == "HELD":
                await self._call(self._release, owner, fence)
        except Exception:
            # Best effort: the lease expires on its own if this cannot land.
            pass

    async def _renew(self, lease: Lease) -> None:
        """
        Keep our own lease alive while we work.

        Without this, "held for a long time" and "died a long time ago" look
        identical to the server, and the only way to protect a long critical
        section is to make crash detection equally slow.

        Renewal stops after ``max_hold_ms``. A caller can disappear without its
        process dying, and renewing forever on behalf of someone who is never
        coming back wedges the lock until the process restarts.
        """
        interval = self.lease_ms / 1000 / BEATS_PER_LEASE
        loop = asyncio.get_event_loop()
        started = loop.time()
        alarm = self.lease_ms / 1000 * RENEWAL_LATENESS_ALARM

        while True:
            due = loop.time() + interval
            await asyncio.sleep(interval)

            late = loop.time() - due
            if late > alarm:
                # We are what stops a live holder being evicted, so being
                # starved here is the early warning for two callers running at
                # once. Either the critical section is blocking the loop or the
                # lease is sized too tightly for this workload.
                logger.error(
                    "lock %s: renewal ran %.1fs late against a %.1fs lease; "
                    "something is blocking the event loop",
                    self.name,
                    late,
                    self.lease_ms / 1000,
                )

            if loop.time() - started > self.max_hold_ms / 1000:
                logger.error(
                    "lock %s: held for over %.0fs without release; abandoning "
                    "the lease so the queue can move on. The caller most likely "
                    "went away without releasing.",
                    self.name,
                    self.max_hold_ms / 1000,
                )
                lease.lost.set()
                return

            try:
                ok = await self._call(self._beat, lease.owner, str(lease.fence))
            except Exception:
                # A blip. lease_ms leaves room for several more attempts.
                continue
            if not ok:
                lease.lost.set()
                return

    def _stop_renew(self, lease: Lease) -> None:
        if lease._beat is not None:
            lease._beat.cancel()
            lease._beat = None


class _Hold:
    __slots__ = ("lock", "timeout", "lease")

    def __init__(self, lock: FifoLock, timeout):
        self.lock = lock
        self.timeout = timeout
        self.lease = None

    async def __aenter__(self) -> Lease:
        self.lease = await self.lock.acquire(timeout=self.timeout)
        return self.lease

    async def __aexit__(self, exc_type, exc, tb):
        await self.lock.release(self.lease)
        return False
