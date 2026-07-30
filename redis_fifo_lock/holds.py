"""
Re-entrant, coalescing holds on named FIFO locks.

``FifoLock`` is a primitive: one acquire, one release, same caller. Real
consumers often split the two — a job acquires, a different job releases,
with concurrency in between. Left to each consumer, that gap grows the same
scaffolding every time: a refcount so re-entry joins the hold instead of
deadlocking on it, coalescing so concurrent acquires of one name share a
lease, and a sweep so a scope that ends without releasing cleans up. That
scaffolding is lock lifecycle, not consumer logic, so it lives here.

One ``HoldSet`` per scope that shares holds (a request, a pipeline run):

    holds = HoldSet(client_factory=make_client)
    lease = await holds.acquire("invoices")   # first caller takes the lock
    lease = await holds.acquire("invoices")   # re-entry: joins, count -> 2
    await holds.release("invoices")           # count -> 1, still held
    await holds.release("invoices")           # released for real
    ...
    await holds.aclose()                      # end of scope: sweep leftovers

Semantics, stated once: **the lock excludes other scopes, not a scope's own
concurrency.** Within one HoldSet, everyone who asks for a name shares the
hold. Across HoldSets, normal FIFO exclusion applies.

Not thread-safe. One event loop per HoldSet, like everything else here — the
synchronous check-and-reserve in ``acquire`` is what makes concurrent
same-name acquires coalesce instead of racing, and it relies on single-loop
execution.
"""

import asyncio
import inspect
import logging
from typing import Callable, Optional

from redis_fifo_lock.lock import (
    DEFAULT_LEASE_MS,
    DEFAULT_MAX_HOLD_MS,
    DEFAULT_POLL_MS,
    FifoLock,
    Lease,
    LeaseLost,
)

logger = logging.getLogger(__name__)


class _Hold:
    __slots__ = ("ready", "count", "lock", "lease")

    def __init__(self, ready: asyncio.Future):
        self.ready = ready
        self.count = 0
        self.lock: Optional[FifoLock] = None
        self.lease: Optional[Lease] = None


class HoldSet:
    """
    Args:
        client: An async Redis client to use as given. Mutually exclusive
            with ``client_factory``; the HoldSet never closes a client it was
            handed.
        client_factory: Zero-argument callable (sync or async) returning an
            async Redis client. Called lazily on first acquire — inside the
            running loop, which is what makes one-HoldSet-per-scope safe under
            frameworks that create a fresh event loop per task. A client the
            HoldSet created is closed by ``aclose()``.
        lease_ms / poll_ms / max_hold_ms: passed to every ``FifoLock`` this
            HoldSet creates; see ``FifoLock`` for their meaning.
    """

    def __init__(
        self,
        client=None,
        *,
        client_factory: Optional[Callable] = None,
        lease_ms: int = DEFAULT_LEASE_MS,
        poll_ms: int = DEFAULT_POLL_MS,
        max_hold_ms: int = DEFAULT_MAX_HOLD_MS,
    ):
        if (client is None) == (client_factory is None):
            raise ValueError("provide exactly one of client or client_factory")
        self._client = client
        self._factory = client_factory
        self._owns_client = client is None
        self._lease_ms = lease_ms
        self._poll_ms = poll_ms
        self._max_hold_ms = max_hold_ms
        self._entries: dict[str, _Hold] = {}

    # -- public -------------------------------------------------------------

    async def acquire(self, name: str, timeout: Optional[float] = None) -> Lease:
        """
        Take or join the hold on ``name``.

        First caller acquires the underlying lock; concurrent and later
        callers join the existing hold and share its lease. Joining callers
        wait (bounded by ``timeout``) if the first acquire is still in flight.

        Raises:
            asyncio.TimeoutError: the wait ran out, or the caller whose
                acquire we coalesced onto failed to acquire.
        """
        entry = self._entries.get(name)

        if entry is None:
            # Reserve BEFORE the first await: check-then-acquire across an
            # await would let two concurrent callers both miss, both acquire
            # distinct owners on one name, and the second would queue behind
            # the first for the whole hold while its bookkeeping clobbered
            # the first's. Synchronous insertion on one loop is atomic.
            entry = _Hold(asyncio.get_running_loop().create_future())
            self._entries[name] = entry
            try:
                lock = FifoLock(
                    await self._get_client(),
                    name,
                    lease_ms=self._lease_ms,
                    poll_ms=self._poll_ms,
                    max_hold_ms=self._max_hold_ms,
                )
                lease = await lock.acquire(timeout=timeout)
            except BaseException:
                self._entries.pop(name, None)
                entry.ready.set_result(False)  # wake joiners; they fail fast
                raise
            entry.lock, entry.lease, entry.count = lock, lease, 1
            entry.ready.set_result(True)
            return lease

        # shield(): wait_for cancels its awaitable on timeout, and the ready
        # future is shared with the acquiring caller.
        try:
            ok = await asyncio.wait_for(asyncio.shield(entry.ready), timeout=timeout)
        except (asyncio.TimeoutError, TimeoutError):
            raise asyncio.TimeoutError(
                f"timed out waiting for the in-flight acquire of {name!r}"
            )
        if not ok or entry.lease is None:
            raise asyncio.TimeoutError(
                f"the acquire of {name!r} this call coalesced onto did not grant"
            )
        entry.count += 1
        return entry.lease

    async def release(self, name: str) -> bool:
        """
        Give back one hold on ``name``.

        Returns True when the underlying lock was actually released (count
        reached zero), False when holds remain or nothing was held — the
        caller decides whether an unmatched release is worth warning about.

        Raises:
            LeaseLost: the lease had lapsed; another scope may have run
                alongside. Propagated, never swallowed.
        """
        entry = self._entries.get(name)
        if entry is None or entry.count <= 0 or entry.lease is None:
            return False
        entry.count -= 1
        if entry.count > 0:
            return False
        self._entries.pop(name, None)
        await entry.lock.release(entry.lease)
        return True

    def count(self, name: str) -> int:
        """Current hold count on ``name`` (0 if not held)."""
        entry = self._entries.get(name)
        return entry.count if entry else 0

    def held(self, name: str) -> Optional[Lease]:
        """The lease backing ``name``'s hold, if any."""
        entry = self._entries.get(name)
        return entry.lease if entry else None

    async def aclose(self) -> None:
        """
        End of scope: release everything still held, close an owned client.

        Anything still held here means the scope ended without matched
        releases — logged loudly per name, then released so no other scope
        waits out a lease for it. Safe to call more than once.
        """
        for name in list(self._entries):
            entry = self._entries.pop(name, None)
            if entry is None or entry.lease is None:
                continue
            try:
                await entry.lock.release(entry.lease)
                logger.warning(
                    "lock %s: still held (count=%d) when its scope ended; "
                    "released by sweep",
                    name,
                    entry.count,
                )
            except LeaseLost:
                logger.error(
                    "lock %s: lease had already lapsed at scope end; work "
                    "done under it may have overlapped another holder",
                    name,
                )
            except Exception:
                logger.exception(
                    "lock %s: failed to release at scope end; the lease will "
                    "lapse on its own",
                    name,
                )
        if self._owns_client and self._client is not None:
            client, self._client = self._client, None
            try:
                await client.aclose()
            except Exception:
                pass

    # -- internals ------------------------------------------------------------

    async def _get_client(self):
        if self._client is None:
            client = self._factory()
            if inspect.isawaitable(client):
                client = await client
            self._client = client
        return self._client
