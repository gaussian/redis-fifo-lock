"""
HoldSet: re-entrant, coalescing holds — the layer between one-shot FifoLock
and consumers whose acquire and release happen in different places.

The properties under test are the ones a consumer would otherwise hand-roll
and get wrong: re-entry joins instead of deadlocking, concurrent acquires of
one name coalesce onto one lease, releases are counted, a scope-end sweep
frees everything, and none of it weakens cross-scope exclusion.
"""

import asyncio
import uuid

import pytest

from redis_fifo_lock import HoldSet, LeaseLost

from .harness import make_client

pytestmark = [pytest.mark.adversarial, pytest.mark.integration]

LEASE_MS = 2_000
POLL_MS = 300


@pytest.fixture
async def scope():
    """Factory for HoldSets (one per simulated scope) plus key cleanup."""
    clients, names = [], []

    async def factory():
        client = await make_client()
        clients.append(client)
        return HoldSet(
            client,
            lease_ms=LEASE_MS,
            poll_ms=POLL_MS,
            max_hold_ms=LEASE_MS * 8,
        )

    def name():
        n = f"adv:holds:{uuid.uuid4().hex[:8]}"
        names.append(n)
        return n

    yield factory, name

    for client in clients:
        for n in names:
            keys = await client.keys(f"{{{n}}}*")
            if keys:
                await client.delete(*keys)
        await client.aclose()


async def test_reentry_joins_the_hold(scope):
    """A second acquire of a held name must join, share the lease, and keep
    the lock held until the LAST release — not deadlock behind itself."""
    factory, name = scope
    holds = await factory()
    n = name()

    lease1 = await asyncio.wait_for(holds.acquire(n), timeout=10)
    # Pre-HoldSet, this call queues behind the caller's own hold forever.
    lease2 = await asyncio.wait_for(holds.acquire(n, timeout=5), timeout=10)

    assert lease1.fence == lease2.fence, "re-entry took a second lease"
    assert holds.count(n) == 2

    assert await holds.release(n) is False  # one hold remains
    assert holds.held(n) is not None
    assert await holds.release(n) is True  # released for real
    assert holds.held(n) is None


async def test_concurrent_acquires_coalesce_onto_one_lease(scope):
    """Two tasks race the same name in one scope: exactly one underlying
    acquire, both callers granted, count == 2."""
    factory, name = scope
    holds = await factory()
    n = name()

    leases = await asyncio.wait_for(
        asyncio.gather(holds.acquire(n, timeout=10), holds.acquire(n, timeout=10)),
        timeout=20,
    )
    assert leases[0].fence == leases[1].fence, "racing acquires took two leases"
    assert holds.count(n) == 2

    await holds.release(n)
    await holds.release(n)


async def test_cross_scope_exclusion_is_not_weakened(scope):
    """Coalescing is per-scope only. A different HoldSet must still queue."""
    factory, name = scope
    holds_a, holds_b = await factory(), await factory()
    n = name()

    await asyncio.wait_for(holds_a.acquire(n), timeout=10)

    with pytest.raises(asyncio.TimeoutError):
        await holds_b.acquire(n, timeout=0.6)

    await holds_a.release(n)
    lease_b = await asyncio.wait_for(holds_b.acquire(n, timeout=10), timeout=20)
    assert lease_b is not None
    await holds_b.release(n)


async def test_release_without_hold_is_reported_not_raised(scope):
    factory, name = scope
    holds = await factory()
    assert await holds.release(name()) is False


async def test_aclose_sweeps_every_held_name(scope):
    """A scope that ends without matched releases must leave nothing held."""
    factory, name = scope
    holds = await factory()
    n1, n2 = name(), name()

    await holds.acquire(n1)
    await holds.acquire(n2)
    await holds.acquire(n2)  # count 2: sweep must release regardless of count

    await holds.aclose()

    other = await factory()
    for n in (n1, n2):
        lease = await asyncio.wait_for(other.acquire(n, timeout=5), timeout=10)
        assert lease is not None
        await other.release(n)


async def test_failed_first_acquire_wakes_joiners_and_leaves_no_residue(scope):
    """If the acquiring caller times out, a coalesced joiner must fail fast —
    not hang on the shared future — and the name must be re-acquirable."""
    factory, name = scope
    holds_a, holds_b = await factory(), await factory()
    n = name()

    await holds_a.acquire(n)  # another scope holds; B's acquire will time out

    async def first():
        with pytest.raises(asyncio.TimeoutError):
            await holds_b.acquire(n, timeout=0.7)

    async def joiner():
        await asyncio.sleep(0.1)  # arrive while B's first acquire is in flight
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(holds_b.acquire(n, timeout=5), timeout=10)

    await asyncio.gather(first(), joiner())
    assert holds_b.count(n) == 0

    await holds_a.release(n)
    lease = await asyncio.wait_for(holds_b.acquire(n, timeout=10), timeout=20)
    assert lease is not None
    await holds_b.release(n)


async def test_release_of_a_lapsed_hold_raises_lease_lost(scope):
    """LeaseLost must propagate through HoldSet, never be swallowed — it is
    the only signal that another scope may have run alongside."""
    factory, name = scope
    holds = await factory()
    n = name()

    lease = await holds.acquire(n)
    holds.held(n)  # sanity
    # Simulate the process stalling past its lease: stop renewal, let it lapse.
    entry_lock = holds._entries[n].lock
    entry_lock._stop_renew(lease)
    await asyncio.sleep(LEASE_MS / 1000 + 1.5)

    other = await factory()
    await asyncio.wait_for(other.acquire(n, timeout=10), timeout=20)

    with pytest.raises(LeaseLost):
        await holds.release(n)
    await other.release(n)
