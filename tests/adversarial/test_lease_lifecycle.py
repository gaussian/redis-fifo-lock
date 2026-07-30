"""
Lease lifecycle: renewal failure, release semantics, and the public surface
that shipped untested (hold(), LeaseLost, stats()).

Provenance of each test is stated in its docstring: F1 and F3 were written
red-first — they fail against the pre-fix implementation, which is the only
evidence a test is real. The F2 pair and the API tests are regression guards:
they pin behaviour that must survive the F2 reordering (renewal keeps beating
during the release round-trip, stops on every exit path, and a clean release
never raises a false alarm).
"""

import asyncio
import uuid

import pytest

from redis_fifo_lock.lock import FifoLock, LeaseLost

from .harness import make_client

pytestmark = [pytest.mark.adversarial, pytest.mark.integration]


@pytest.fixture
async def make_lock():
    """Factory for locks with test-scaled timings; cleans up keys and client."""
    clients, locks = [], []

    async def factory(lease_ms=1500, poll_ms=300, max_hold_ms=None):
        client = await make_client()
        clients.append(client)
        lock = FifoLock(
            client,
            f"adv:lease:{uuid.uuid4().hex[:8]}",
            lease_ms=lease_ms,
            poll_ms=poll_ms,
            max_hold_ms=max_hold_ms or lease_ms * 8,
        )
        locks.append(lock)
        return lock

    yield factory

    for lock, client in zip(locks, clients):
        try:
            keys = await client.keys(f"{{{lock.name}}}*")
            if keys:
                await client.delete(*keys)
        except Exception:
            pass
        await client.aclose()


async def test_hung_beat_sets_lost_within_a_lease(make_lock):
    """
    F1, red-first: a renewal that HANGS must still surface lease loss.

    A half-dead connection makes the beat await forever. Renewal silently
    stops, the lease lapses on the server, a successor is granted — and the
    original holder is never told, because `lease.lost` fires only on a
    *refused* beat, never on one that simply never returns. The holder must
    conclude loss locally: no confirmed renewal for a full lease means the
    server-side key is gone, whatever the network says.
    """
    lock = await make_lock(lease_ms=1500)
    lease = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)

    real_call = lock._call

    async def hanging_call(script, owner, extra):
        if script is lock._beat:
            await asyncio.sleep(3600)  # the connection is half-dead
        return await real_call(script, owner, extra)

    lock._call = hanging_call
    try:
        # Within ~2 leases the holder must know. Bound generously; a pre-fix
        # implementation never sets it and times out here.
        await asyncio.wait_for(lease.lost.wait(), timeout=6)
    except asyncio.TimeoutError:
        pytest.fail(
            "renewal hung for 4x the lease and lease.lost never fired; the "
            "holder still believes it holds a lock the server expired long ago"
        )
    finally:
        lock._call = real_call
        lock._stop_renew(lease)


async def test_replayed_release_after_interleaved_cycle_is_not_stale(make_lock):
    """
    F3, red-first: a redelivered release must not manufacture a corruption
    alarm.

    Sequence: A releases (OK); B acquires and releases; A's release is
    redelivered (client retry, proxy, at-least-once anything). With a single
    global dedup slot the replay reads B's fence, mismatches, and raises
    LeaseLost — whose documented meaning is "someone may have run alongside
    you". A false integrity alarm from an ordinary network retry.
    """
    lock = await make_lock(lease_ms=2000)

    lease_a = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)
    await lock.release(lease_a)

    lease_b = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)
    await lock.release(lease_b)

    try:
        await lock.release(lease_a)  # the redelivered command
    except LeaseLost:
        pytest.fail(
            "a replayed release of an already-released lease raised LeaseLost "
            "after an intervening acquire/release cycle - a false 'concurrent "
            "holder' alarm from a routine retry"
        )


async def test_release_failure_still_stops_renewal(make_lock):
    """
    F2 guard: every exit path of release() must stop the renewal task.

    If the release round-trip raises and renewal keeps running, an abandoned
    lease is kept alive until max_hold_ms - escalating a bounded <=lease_ms
    hiccup into a wedge measured in minutes.
    """
    lock = await make_lock(lease_ms=1500)
    lease = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)

    real_call = lock._call

    async def failing_release(script, owner, extra):
        if script is lock._release:
            raise ConnectionError("network hiccup at release time")
        return await real_call(script, owner, extra)

    lock._call = failing_release
    try:
        with pytest.raises(ConnectionError):
            await lock.release(lease)
    finally:
        lock._call = real_call

    assert lease._beat is None, (
        "release() raised but the renewal task is still running; the lease "
        "will now be renewed until max_hold_ms with nobody able to release it"
    )
    # Nothing is renewing, so the server frees the lock within one lease.
    successor = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)
    await lock.release(successor)


async def test_clean_release_never_reports_loss(make_lock):
    """
    F2 guard: a beat racing a clean release must not set lease.lost.

    A refused beat means "you are no longer the holder". During release that
    is the expected outcome, not evidence of a concurrent holder; signalling
    it would page someone over a lock that was released correctly.
    """
    lock = await make_lock(lease_ms=1200)
    lease = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)
    await lock.release(lease)

    # Give any in-flight or straggling beat ample time to land and be refused.
    await asyncio.sleep(1.0)
    assert not lease.lost.is_set(), (
        "a clean release() set lease.lost - a false integrity alarm"
    )


async def test_hold_context_manager_acquires_and_releases(make_lock):
    """API: the README's first example, previously untested."""
    lock = await make_lock()

    async with lock.hold(timeout=10) as lease:
        assert lease.fence >= 1
        stats = await lock.stats()
        assert stats["held"] is True
        assert stats["owner"] == lease.owner
        assert stats["fence"] == lease.fence

    stats = await lock.stats()
    assert stats["held"] is False, "hold() exited but the lock is still held"
    assert stats["waiting"] == 0


async def test_release_of_a_genuinely_lost_lease_raises(make_lock):
    """API: LeaseLost fires when it should - the lease truly expired."""
    lock = await make_lock(lease_ms=800)
    lease = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)

    lock._stop_renew(lease)  # the process "dies"
    await asyncio.sleep(1.6)  # lease expires server-side

    other = await asyncio.wait_for(lock.acquire(timeout=10), timeout=20)
    with pytest.raises(LeaseLost):
        await lock.release(lease)
    await lock.release(other)
