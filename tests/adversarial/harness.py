"""
Machinery for the adversarial suite.

The suite's job is to *kill* a FIFO mutex, so nothing in here knows how any
particular lock is built. An implementation is plugged in behind `GateAdapter`
and judged only on the two properties that matter: at most one caller is inside
the critical section at a time, and grants follow the order Redis saw the
tickets arrive.

Everything is bounded by an explicit timeout. A lock that wedges must fail a
test, never hang the suite.
"""

import asyncio
import os
import time
import uuid
from typing import Optional

import redis.asyncio as redis

from redis_fifo_lock.lock import FifoLock


def redis_url() -> str:
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
    if not url.startswith("redis://") and not url.startswith("rediss://"):
        url = f"redis://{url}/15"
    return url


class Grant:
    """One acquisition: whatever the implementation needs to release it, plus
    the server-assigned position used to check FIFO."""

    __slots__ = ("handle", "order", "label")

    def __init__(self, handle, order, label=None):
        self.handle = handle
        self.order = order
        self.label = label


class GateAdapter:
    """
    Uniform surface over a lock implementation.

    Subclass per architecture; the tests never import a lock class directly.
    """

    #: Longest the implementation may take to reclaim a crashed holder.
    recovery_bound_s: float = 10.0

    #: Longest the implementation may take to reclaim a lock whose caller went
    #: away while its process kept running. Separate from recovery_bound_s
    #: because an implementation that renews a lease in the background cannot
    #: tell that case apart from honest work, and needs a longer backstop.
    abandon_bound_s: float = 10.0

    async def acquire(self, timeout: Optional[float] = None) -> Grant:
        raise NotImplementedError

    async def release(self, grant: Grant) -> None:
        raise NotImplementedError

    async def queue_size(self) -> int:
        """Outstanding tickets. Must return to 0 once every holder releases."""
        raise NotImplementedError

    async def abandon(self, grant: Grant) -> None:
        """
        Simulate the holder's process dying: stop doing anything that keeps the
        lock alive, and never release. The lock must come back on its own.
        """
        raise NotImplementedError

    async def cleanup(self) -> None:
        raise NotImplementedError


class FifoLockAdapter(GateAdapter):
    """The ZSET + lease design."""

    def __init__(self, client, name: str, wake_ms: int, dead_holder_ms: int):
        self.r = client
        self.name = name
        max_hold_ms = dead_holder_ms * 6
        self.lock = FifoLock(
            client,
            name,
            lease_ms=dead_holder_ms,
            poll_ms=wake_ms,
            # Scaled to the suite's timescale exactly like every other interval
            # here. Must sit above the longest legitimate hold the suite uses
            # (4 recovery windows), as 30 minutes does over a 60s lease.
            max_hold_ms=max_hold_ms,
        )
        self.recovery_bound_s = dead_holder_ms / 1000.0 * 3 + 2
        # A live process renewing its own lease can only be reclaimed once the
        # renewal cap expires, so this bound is necessarily longer.
        self.abandon_bound_s = max_hold_ms / 1000.0 + dead_holder_ms / 1000.0 + 5

    async def acquire(self, timeout=None) -> Grant:
        lease = await self.lock.acquire(timeout=timeout)
        # Order by ARRIVAL (pos), never by fence. The fence is minted at grant
        # time, so ordering by it is monotonic by construction and the FIFO
        # assertion could never fail — a test that cannot fail proves nothing.
        return Grant(lease, lease.pos)

    async def release(self, grant: Grant) -> None:
        await self.lock.release(grant.handle)

    async def queue_size(self) -> int:
        return await self.r.zcard(f"{{{self.name}}}:q")

    async def abandon(self, grant: Grant) -> None:
        # Kill the renewal exactly as losing the process would, then walk away.
        self.lock._stop_renew(grant.handle)

    async def cleanup(self) -> None:
        keys = await self.r.keys(f"{{{self.name}}}:*")
        if keys:
            await self.r.delete(*keys)


class Monitor:
    """
    Records who is inside the critical section and when.

    asyncio is single-threaded, so if a second caller enters while another is
    still inside, `enter()` sees it directly — no sampling race, no polling.
    """

    def __init__(self):
        self.active = {}
        self.overlaps = []
        self.grants = []
        self.started = time.monotonic()

    def enter(self, label, grant: Grant):
        now = time.monotonic() - self.started
        if self.active:
            self.overlaps.append(
                {
                    "entered": label,
                    "already_inside": sorted(self.active),
                    "at": round(now, 3),
                }
            )
        self.active[label] = now
        self.grants.append((grant.order, label, round(now, 3)))

    def exit(self, label):
        self.active.pop(label, None)

    # -- assertions ---------------------------------------------------------

    def assert_exclusive(self):
        assert not self.overlaps, (
            f"{len(self.overlaps)} mutual-exclusion violation(s); "
            f"first: {self.overlaps[0]}"
        )

    def assert_fifo(self):
        """
        Grants must follow the order Redis assigned the tickets.

        Deliberately not task-creation order: the order coroutines are spawned
        in says nothing about the order their XADD/ZADD reached the server, and
        asserting on it produces a permanently flaky suite.
        """
        seen = [(order, label) for order, label, _ in self.grants]
        inversions = [
            (seen[i], seen[i + 1])
            for i in range(len(seen) - 1)
            if seen[i][0] > seen[i + 1][0]
        ]
        assert not inversions, (
            f"{len(inversions)} FIFO inversion(s) by server-assigned position; "
            f"first: {inversions[0]}"
        )

    def summary(self) -> str:
        return (
            f"grants={len(self.grants)} overlaps={len(self.overlaps)} "
            f"still_inside={sorted(self.active)}"
        )


async def hold(
    adapter: GateAdapter,
    monitor: Monitor,
    label: str,
    hold_s: float,
    acquire_timeout: Optional[float],
    deadline_s: float,
):
    """
    Acquire, sit in the critical section for `hold_s`, release.

    Returns the grant, or None if the acquire timed out (a legitimate outcome
    the tests exercise on purpose). Any wedge surfaces as a TimeoutError from
    the bounding wait_for rather than as a hung suite.
    """
    try:
        grant = await asyncio.wait_for(
            adapter.acquire(timeout=acquire_timeout), timeout=deadline_s
        )
    except (asyncio.TimeoutError, TimeoutError):
        return None

    grant.label = label
    monitor.enter(label, grant)
    try:
        await asyncio.sleep(hold_s)
    finally:
        monitor.exit(label)
        await asyncio.wait_for(adapter.release(grant), timeout=deadline_s)
    return grant


async def make_client():
    client = await redis.from_url(redis_url(), decode_responses=False)
    await client.ping()
    return client


async def make_consumer_client():
    """
    A client built the way the only production consumer builds one.

    That consumer creates a single process-global client with responses decoded
    to ``str`` and no client-side deadline on either the connect or the read.
    Every other client in this repository — unit, integration and adversarial —
    is created with ``decode_responses=False``, so nothing else in the suite
    ever hands the gate the types it actually sees in production.
    """
    client = await redis.from_url(
        redis_url(),
        decode_responses=True,
        socket_timeout=None,
        socket_connect_timeout=None,
    )
    await client.ping()
    return client


def unique_name(prefix: str) -> str:
    return f"adv:{prefix}:{uuid.uuid4().hex[:8]}"
