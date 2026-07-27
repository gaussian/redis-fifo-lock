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

from redis_fifo_lock.async_gate import AsyncStreamGate


def redis_url() -> str:
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
    if not url.startswith("redis://") and not url.startswith("rediss://"):
        url = f"redis://{url}/15"
    return url


def stream_id_order(msg_id) -> tuple:
    """
    Sort key for a Redis stream id.

    ``b"1785-9"`` sorts before ``b"1785-10"`` numerically but after it
    lexicographically, so comparing the raw bytes silently corrupts every FIFO
    assertion built on top of it.
    """
    raw = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
    ms, _, seq = raw.partition("-")
    return (int(ms), int(seq or 0))


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

    async def acquire(self, timeout: Optional[float] = None) -> Grant:
        raise NotImplementedError

    async def release(self, grant: Grant) -> None:
        raise NotImplementedError

    async def queue_size(self) -> int:
        """Outstanding tickets. Must return to 0 once every holder releases."""
        raise NotImplementedError

    async def cleanup(self) -> None:
        raise NotImplementedError


class StreamGateAdapter(GateAdapter):
    """The shipped AsyncStreamGate (Redis Streams + consumer group)."""

    def __init__(self, client, name: str, wake_ms: int, dead_holder_ms: int):
        self.r = client
        self.name = name
        self.gate = AsyncStreamGate(
            client,
            stream=f"{name}:stream",
            group=f"{name}:group",
            sig_prefix=f"{name}:sig:",
            last_key=f"{name}:last",
            blpop_internal_timeout_ms=wake_ms,
            dead_holder_timeout_ms=dead_holder_ms,
            claim_idle_ms=max(dead_holder_ms // 2, 1),
        )
        self.recovery_bound_s = dead_holder_ms / 1000.0 * 3 + 2

    async def acquire(self, timeout=None) -> Grant:
        owner, msg_id = await self.gate.acquire(timeout=timeout)
        return Grant((owner, msg_id), stream_id_order(msg_id))

    async def release(self, grant: Grant) -> None:
        owner, msg_id = grant.handle
        await self.gate.release(
            owner, msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        )

    async def queue_size(self) -> int:
        return await self.r.xlen(self.gate.stream)

    async def cleanup(self) -> None:
        keys = await self.r.keys(f"{self.name}:*")
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


def unique_name(prefix: str) -> str:
    return f"adv:{prefix}:{uuid.uuid4().hex[:8]}"
