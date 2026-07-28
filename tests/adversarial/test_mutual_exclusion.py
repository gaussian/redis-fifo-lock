"""
Adversarial suite: try to make a FIFO mutex grant the lock twice.

These are slow and randomized by design — run them on deploy, not on every
save. The unit suite stays fast; `pytest` excludes this directory unless you
ask for it with `-m adversarial`.

Design rules, each one a direct response to how the real bug survived 88 tests
for eight months:

1. **Hold the lock across many waiter wake-ups.** The old suite's longest
   concurrent hold was 100ms against a 5000ms wake interval, so no waiter ever
   woke while a holder was inside. Every test here holds for several multiples
   of the wake interval.
2. **Assert the property, not the choreography.** A shared counter of who is
   inside, not `len(PEL) <= 1` or any other bookkeeping proxy — a proxy can stay
   true *during* the violation it is supposed to detect.
3. **Order by what the server assigned**, never by the order coroutines were
   created.
4. **Bound everything.** A wedged lock fails a test; it never hangs the suite.
"""

import asyncio
import random

import pytest

from .harness import (
    FifoLockAdapter,
    Monitor,
    hold,
    make_client,
    unique_name,
)

pytestmark = [pytest.mark.adversarial, pytest.mark.integration]

# Small enough to keep the suite quick, large enough that a hold of a few
# hundred ms spans many waiter wake-ups — which is the whole point.
WAKE_MS = 300
DEAD_HOLDER_MS = 3_000

#: Every implementation faces the identical suite. Nothing here knows how any
#: of them works.
IMPLEMENTATIONS = {
    "fifolock": FifoLockAdapter,
}


@pytest.fixture(params=sorted(IMPLEMENTATIONS))
async def gate(request):
    client = await make_client()
    adapter = IMPLEMENTATIONS[request.param](
        client, unique_name(request.param), WAKE_MS, DEAD_HOLDER_MS
    )
    try:
        yield adapter
    finally:
        await adapter.cleanup()
        await client.aclose()


async def test_long_hold_never_overlaps(gate):
    """
    The headline property, and the one nothing has ever tested.

    Four callers each hold for 5x the waiter wake interval, so every queued
    waiter wakes repeatedly while somebody else is inside the critical section.
    """
    monitor = Monitor()
    hold_s = WAKE_MS / 1000 * 5

    await asyncio.gather(
        *[
            hold(gate, monitor, f"w{i}", hold_s, acquire_timeout=60, deadline_s=90)
            for i in range(4)
        ]
    )

    monitor.assert_exclusive()
    assert len(monitor.grants) == 4, monitor.summary()


async def test_burst_acquire_grants_exactly_one(gate):
    """32 callers race from a standing start. Exactly one may be inside."""
    monitor = Monitor()
    hold_s = WAKE_MS / 1000 * 3

    await asyncio.gather(
        *[
            hold(gate, monitor, f"b{i}", hold_s, acquire_timeout=120, deadline_s=180)
            for i in range(32)
        ]
    )

    monitor.assert_exclusive()
    assert len(monitor.grants) == 32, monitor.summary()


async def test_fifo_follows_server_arrival(gate):
    """
    Grants must follow the order Redis assigned the tickets.

    Arrivals are staggered so each ticket's position is unambiguous, then every
    holder sits long enough for the ones behind it to wake several times.
    """
    monitor = Monitor()
    hold_s = WAKE_MS / 1000 * 3
    tasks = []

    for i in range(6):
        tasks.append(
            asyncio.create_task(
                hold(
                    gate, monitor, f"f{i}", hold_s, acquire_timeout=120, deadline_s=180
                )
            )
        )
        await asyncio.sleep(0.05)  # unambiguous arrival order at the server

    await asyncio.gather(*tasks)

    monitor.assert_exclusive()
    monitor.assert_fifo()


@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_randomized_operations_never_overlap(gate, seed):
    """
    Randomized holds, waits and timeouts.

    Hand-picked scenarios are what missed the real bug: each one arranged, by
    accident, for the dangerous interleaving to be impossible. Randomizing hold
    durations across the wake interval — some shorter, some much longer — is
    what makes the bad ordering reachable.
    """
    rng = random.Random(seed)
    monitor = Monitor()
    wake_s = WAKE_MS / 1000

    async def worker(i):
        for _ in range(3):
            await asyncio.sleep(rng.uniform(0, wake_s))
            # Deliberately straddle the wake interval in both directions.
            hold_s = rng.choice([wake_s * 0.2, wake_s * 1.5, wake_s * 4])
            timeout = rng.choice([None, 60])
            await hold(
                gate,
                monitor,
                f"r{seed}.{i}",
                hold_s,
                acquire_timeout=timeout,
                deadline_s=120,
            )

    await asyncio.gather(*[worker(i) for i in range(5)])

    monitor.assert_exclusive()
    monitor.assert_fifo()


async def test_abandoned_waiters_do_not_wedge_the_gate(gate):
    """
    Waiters that give up must leave nothing behind that blocks the queue.

    Six callers with timeouts too short to ever win, then a live caller that
    must still acquire promptly.
    """
    monitor = Monitor()

    await asyncio.gather(
        *[
            hold(
                gate,
                monitor,
                f"doomed{i}",
                hold_s=0.05,
                acquire_timeout=WAKE_MS / 1000 * 0.5,
                deadline_s=30,
            )
            for i in range(6)
        ]
    )

    survivor = await hold(
        gate, monitor, "survivor", hold_s=0.05, acquire_timeout=20, deadline_s=30
    )
    assert survivor is not None, "gate was wedged by waiters that gave up"
    monitor.assert_exclusive()


async def test_hold_far_beyond_the_recovery_window(gate):
    """
    Holding for a long time must not be mistaken for having died.

    This is the property the whole design exists for: a critical section may
    run far longer than the crash-detection window, because a live holder says
    so. Without it, the two are the same number and you must choose between
    supporting long work and noticing a crash promptly.

    Every other test here holds for less than one recovery window, so this is
    the only one that exercises renewal at all.
    """
    monitor = Monitor()
    hold_s = DEAD_HOLDER_MS / 1000 * 2.5

    waiter_started = asyncio.Event()

    async def waiter():
        waiter_started.set()
        await hold(gate, monitor, "waiter", 0.05, acquire_timeout=60, deadline_s=90)

    holder = asyncio.create_task(
        hold(gate, monitor, "holder", hold_s, acquire_timeout=60, deadline_s=90)
    )
    await asyncio.sleep(0.2)
    queued = asyncio.create_task(waiter())
    await waiter_started.wait()

    await asyncio.gather(holder, queued)

    monitor.assert_exclusive()
    assert len(monitor.grants) == 2, monitor.summary()


async def test_crashed_holder_is_reclaimed(gate):
    """
    A holder that stops renewing must lose the lock, and a waiter must get it.

    The mirror of the test above: renewal has to be what keeps the lease, not
    something incidental. If this passes while the previous test fails, the
    implementation cannot tell a long hold from a dead process.
    """
    monitor = Monitor()

    grant = await asyncio.wait_for(gate.acquire(timeout=30), timeout=40)
    monitor.enter("crashed", grant)
    await gate.abandon(grant)  # process dies here; never releases

    successor = await hold(
        gate,
        monitor,
        "successor",
        hold_s=0.05,
        acquire_timeout=gate.recovery_bound_s,
        deadline_s=gate.recovery_bound_s + 10,
    )

    assert successor is not None, (
        f"nobody reclaimed the lock within {gate.recovery_bound_s:.0f}s of the "
        f"holder dying"
    )


async def test_queue_drains_to_empty(gate):
    """
    Nothing may accumulate without bound.

    After every holder has released, the outstanding-ticket count must be back
    to zero. A queue that only ever grows is a slow-motion outage.
    """
    monitor = Monitor()

    for round_ in range(2):
        await asyncio.gather(
            *[
                hold(
                    gate,
                    monitor,
                    f"d{round_}.{i}",
                    hold_s=0.05,
                    acquire_timeout=60,
                    deadline_s=90,
                )
                for i in range(5)
            ]
        )

    monitor.assert_exclusive()
    remaining = await gate.queue_size()
    assert remaining == 0, (
        f"{remaining} tickets left after every holder released — the queue "
        f"never shrinks, so it grows for the lifetime of the deployment"
    )
