"""
Adversarial suite, second front: break the lock the way its *consumer* uses it.

`test_mutual_exclusion.py` attacks the algorithm. This file attacks everything
around the algorithm — the client the caller actually builds, the fact that
acquire and release happen in different coroutines minutes apart, and the many
ways a graph engine abandons work half-done. Each test here is justified by the
published contract and by observed consumer behaviour, never by how any
implementation is built.

The consumer is a Django graph-execution engine. The details that matter:

* One process-global Redis client, created with ``decode_responses=True`` and
  no socket deadlines. **Every other test in this repository builds its client
  with ``decode_responses=False``**, so the gate has never once been exercised
  against the types production feeds it.
* A `LockDelayExecutor` node acquires; a *separate* `UnlockExecutor` node
  releases, in a different task, with arbitrary graph work in between. The
  grant travels between them as plain data in a context dict.
* Gate objects are cached forever in a process-global dict keyed by lock name
  and shared by every concurrent graph execution in the process. Lock names are
  templated (``lock_{workspace_id}``), so one process holds many at once.
* Critical sections run 1 to 1000 seconds — many multiples of any sane
  crash-detection window.
* If the graph errors or branches away after the lock node succeeds,
  `UnlockExecutor` never runs and **nothing** releases the lock.
* A context dict can be replayed and an unlock node can run twice, so the same
  grant can be released more than once.
* Timeouts are a normal path, not an error: with ``continue_if_lock_timeout``
  the caller logs a warning and carries on, then retries later.

Design rules inherited from the sibling file: assert the property and not the
bookkeeping, order by what the server assigned, and bound every single await so
that a wedged lock fails a test instead of hanging the suite.
"""

import asyncio
import time

import pytest

from .harness import (
    FifoLockAdapter,
    Monitor,
    hold,
    make_client,
    make_consumer_client,
    unique_name,
)

pytestmark = [pytest.mark.adversarial, pytest.mark.integration]

WAKE_MS = 300
DEAD_HOLDER_MS = 3_000

#: Every implementation faces the identical suite. Nothing here knows how any
#: of them works.
IMPLEMENTATIONS = {
    "fifolock": FifoLockAdapter,
}


@pytest.fixture(params=sorted(IMPLEMENTATIONS))
def impl(request):
    """The implementation under test, as a (class, label) pair.

    Tests that need to build several gates — several lock names, or several
    independent instances standing in for separate processes — construct them
    from this instead of taking a single ready-made adapter.
    """
    return IMPLEMENTATIONS[request.param], request.param


@pytest.fixture
async def gate(impl):
    cls, label = impl
    client = await make_client()
    adapter = cls(client, unique_name(label), WAKE_MS, DEAD_HOLDER_MS)
    try:
        yield adapter
    finally:
        await adapter.cleanup()
        await client.aclose()


@pytest.fixture
async def consumer_gate(impl):
    """A gate on the exact client configuration production uses."""
    cls, label = impl
    client = await make_consumer_client()
    adapter = cls(client, unique_name(label), WAKE_MS, DEAD_HOLDER_MS)
    try:
        yield adapter
    finally:
        await adapter.cleanup()
        await client.aclose()


# -- helpers ----------------------------------------------------------------


async def acquire_or_none(gate, timeout, deadline):
    """Acquire, or None if it timed out. Never hangs: the wait is bounded."""
    try:
        return await asyncio.wait_for(gate.acquire(timeout=timeout), timeout=deadline)
    except (asyncio.TimeoutError, TimeoutError):
        return None


async def release_quietly(gate, grant, deadline=20):
    """
    Release, reporting any exception rather than raising it.

    Whether releasing a grant you no longer own raises or is a no-op is not
    specified, and both are defensible. What is *not* negotiable is that it must
    not disturb whoever legitimately holds the lock, so the tests assert on that
    and merely record which way the implementation went.
    """
    try:
        await asyncio.wait_for(gate.release(grant), timeout=deadline)
        return None
    except (asyncio.TimeoutError, TimeoutError):
        return "TimeoutError"
    except Exception as exc:  # noqa: BLE001 - deliberately catching everything
        return type(exc).__name__


# -- 1. the client the consumer actually builds ------------------------------


async def test_exclusion_and_fifo_on_the_consumers_client(consumer_gate):
    """
    Every claimed guarantee, on a ``decode_responses=True`` client.

    A library does not get to dictate how the caller builds their Redis client,
    and the one real caller decodes responses. Under decoding, every id, owner
    token and flag the gate reads back from Redis arrives as ``str`` instead of
    ``bytes``. Any place that compares a freshly-read value against a stored one
    of the other type silently evaluates to "not mine" — or, worse, "not held" —
    and the lock stops excluding.

    The existing suite cannot catch this: all 88 unit/integration tests and the
    whole adversarial file build their clients with ``decode_responses=False``,
    which is the one configuration production never uses.
    """
    monitor = Monitor()
    hold_s = WAKE_MS / 1000 * 4
    tasks = []

    for i in range(5):
        tasks.append(
            asyncio.create_task(
                hold(
                    consumer_gate,
                    monitor,
                    f"c{i}",
                    hold_s,
                    acquire_timeout=90,
                    deadline_s=120,
                )
            )
        )
        await asyncio.sleep(0.05)  # unambiguous arrival order at the server

    await asyncio.wait_for(asyncio.gather(*tasks), timeout=150)

    monitor.assert_exclusive()
    monitor.assert_fifo()
    assert len(monitor.grants) == 5, monitor.summary()

    remaining = await asyncio.wait_for(consumer_gate.queue_size(), timeout=20)
    assert remaining == 0, f"{remaining} tickets left behind on a decoded client"


async def test_crash_recovery_on_the_consumers_client(consumer_gate):
    """
    Recovery must still fire when responses are decoded.

    Reclaiming a dead holder means reading somebody *else's* bookkeeping back
    out of Redis and judging it stale. That is precisely the code most likely to
    compare a decoded ``str`` against a ``bytes`` literal, and getting it wrong
    here does not merely slow things down: the lock is never reclaimed and every
    future caller for that lock name blocks forever.

    The sibling suite's recovery test runs only on an undecoded client.
    """
    grant = await asyncio.wait_for(consumer_gate.acquire(timeout=30), timeout=40)
    await asyncio.wait_for(consumer_gate.abandon(grant), timeout=20)

    started = time.monotonic()
    successor = await acquire_or_none(
        consumer_gate,
        timeout=consumer_gate.recovery_bound_s,
        deadline=consumer_gate.recovery_bound_s + 15,
    )
    assert successor is not None, (
        f"a crashed holder was never reclaimed within "
        f"{consumer_gate.recovery_bound_s:.0f}s on a decode_responses=True "
        f"client — the lock name is dead for the life of the deployment"
    )
    assert time.monotonic() - started <= consumer_gate.recovery_bound_s + 5
    await asyncio.wait_for(consumer_gate.release(successor), timeout=20)


# -- 2. release fencing ------------------------------------------------------


async def test_duplicate_release_does_not_steal_from_the_live_holder(gate):
    """
    Releasing a grant twice must not unlock somebody else's critical section.

    The consumer keeps ``{gate, owner, msg_id}`` in a context dict that a graph
    engine may replay, and its unlock node is an ordinary node that can be
    retried. So the same grant genuinely does get released twice, and by then
    the lock has usually moved on to another caller.

    If release only checks "is this lock held?" and not "is it held *by this
    grant*?", the second release frees the lock out from under the current
    holder and a third caller walks straight in — two callers inside at once,
    with no crash, no partition and no timeout involved.

    Nothing in the existing suite ever calls release twice, so no existing test
    can reach this.
    """
    monitor = Monitor()

    first = await asyncio.wait_for(gate.acquire(timeout=30), timeout=40)
    await asyncio.wait_for(gate.release(first), timeout=20)

    holder = await asyncio.wait_for(gate.acquire(timeout=30), timeout=40)
    monitor.enter("holder", holder)

    # The unlock node runs a second time with the stale grant.
    outcome = await release_quietly(gate, first)

    intruder = await acquire_or_none(gate, timeout=2.0, deadline=15)
    if intruder is not None:
        monitor.enter("intruder", intruder)
        await release_quietly(gate, intruder)

    monitor.exit("holder")
    await release_quietly(gate, holder)

    assert intruder is None, (
        f"a duplicate release of an already-released grant handed the lock to a "
        f"second caller while the real holder was still inside "
        f"(duplicate release returned {outcome or 'no error'})"
    )
    monitor.assert_exclusive()


async def test_release_after_recovery_does_not_steal_from_the_successor(gate):
    """
    A holder that already lost the lock must not be able to release it.

    This is the consumer's worst realistic case. A critical section runs for
    minutes; the holder stalls long enough to be judged dead and the lock is
    reclaimed; the graph then recovers and its unlock node runs on schedule with
    a grant that is now ancient history. If that release is not fenced against
    the grant's identity, it evicts the innocent successor mid-work.

    The sibling suite checks that a crashed holder *is* reclaimed. It never asks
    what happens when the corpse gets up and calls release.
    """
    monitor = Monitor()

    dead = await asyncio.wait_for(gate.acquire(timeout=30), timeout=40)
    await asyncio.wait_for(gate.abandon(dead), timeout=20)

    successor = await acquire_or_none(
        gate,
        timeout=gate.recovery_bound_s,
        deadline=gate.recovery_bound_s + 15,
    )
    assert successor is not None, "precondition failed: nobody reclaimed the lock"
    monitor.enter("successor", successor)

    # The "dead" process wakes up and runs its unlock node.
    outcome = await release_quietly(gate, dead)

    intruder = await acquire_or_none(gate, timeout=2.0, deadline=15)
    if intruder is not None:
        monitor.enter("intruder", intruder)
        await release_quietly(gate, intruder)

    monitor.exit("successor")
    await release_quietly(gate, successor)

    assert intruder is None, (
        f"a release from a holder that had already been declared dead evicted "
        f"the successor mid-critical-section (that release returned "
        f"{outcome or 'no error'})"
    )
    monitor.assert_exclusive()


# -- 3. acquire here, release over there -------------------------------------


async def test_acquire_and_release_in_different_tasks_across_many_windows(gate):
    """
    The consumer's literal shape: acquire in one node, release in another.

    The acquiring coroutine *finishes* — it hands the grant to a context dict
    and returns — and the lock is held for four crash-detection windows before
    an unrelated coroutine releases it. Meanwhile a waiter is queued the whole
    time.

    Two ways to fail. If anything that defends the lock is tied to the acquiring
    task's lifetime, the lock evaporates the moment that task returns and the
    waiter walks in while the critical section is still running. If instead the
    hold is capped by the recovery window, the waiter is let in at the window
    boundary — which is exactly the "1000-second section, 2-minute window"
    coexistence the design claims.

    The sibling suite's long-hold test acquires and releases inside a single
    coroutine and covers 2.5 windows. Nothing anywhere splits the two.
    """
    monitor = Monitor()
    windows = 4
    hold_s = DEAD_HOLDER_MS / 1000 * windows
    context = {}

    async def lock_node():
        """LockDelayExecutor: acquires, stashes the grant, and returns."""
        grant = await asyncio.wait_for(gate.acquire(timeout=60), timeout=90)
        grant.label = "holder"
        monitor.enter("holder", grant)
        context["grant"] = grant

    node = asyncio.create_task(lock_node())
    await asyncio.wait_for(node, timeout=95)
    assert node.done(), "precondition: the acquiring task must have finished"

    async def queued_waiter():
        await hold(
            gate,
            monitor,
            "waiter",
            hold_s=0.05,
            acquire_timeout=hold_s + 60,
            deadline_s=hold_s + 90,
        )

    waiter = asyncio.create_task(queued_waiter())

    # Arbitrary graph work happens here, for many recovery windows.
    await asyncio.sleep(hold_s)
    breaches = list(monitor.overlaps)

    async def unlock_node():
        """UnlockExecutor: a different task entirely, reading back the grant."""
        monitor.exit("holder")
        await asyncio.wait_for(gate.release(context["grant"]), timeout=30)

    await asyncio.wait_for(asyncio.create_task(unlock_node()), timeout=40)
    await asyncio.wait_for(waiter, timeout=hold_s + 95)

    assert not breaches, (
        f"the lock was handed to a second caller while a {hold_s:.0f}s critical "
        f"section ({windows} recovery windows) was still running, after the "
        f"acquiring task had returned; first breach: {breaches[0]}"
    )
    monitor.assert_exclusive()
    assert len(monitor.grants) == 2, monitor.summary()


# -- 4. work that is abandoned rather than finished --------------------------


async def test_cancelled_acquires_leave_no_residue(gate):
    """
    A cancelled acquire must withdraw its ticket, exactly as a timeout does.

    A graph engine cancels tasks constantly — the execution errors, the user
    aborts, a deadline elsewhere fires. Those callers are cancelled while parked
    in ``acquire``; they do not time out. That is a different code path, because
    cleanup after ``CancelledError`` has to run in a ``finally`` that still
    manages to talk to Redis, and an ``await`` inside a cancelled coroutine is
    notoriously easy to get wrong.

    A ticket left behind by a cancelled waiter is worse than a leak: the queue
    is FIFO, so a phantom ticket is at the head of it, and every real caller
    behind it waits for a phantom that will never run.

    The sibling suite exercises timeouts (`test_abandoned_waiters...`) but never
    cancellation, and it never checks the queue after a caller gives up.
    """
    holder = await asyncio.wait_for(gate.acquire(timeout=30), timeout=40)

    parked = [asyncio.create_task(gate.acquire(timeout=None)) for _ in range(6)]
    await asyncio.sleep(1.0)  # everyone is queued behind the holder
    for task in parked:
        task.cancel()
    await asyncio.wait_for(asyncio.gather(*parked, return_exceptions=True), timeout=30)
    await asyncio.sleep(0.5)

    await asyncio.wait_for(gate.release(holder), timeout=20)

    started = time.monotonic()
    successor = await acquire_or_none(gate, timeout=30, deadline=45)
    latency = time.monotonic() - started
    assert successor is not None, "the gate was wedged by cancelled waiters"
    await asyncio.wait_for(gate.release(successor), timeout=20)

    assert latency < gate.recovery_bound_s, (
        f"an uncontended acquire took {latency:.1f}s after 6 waiters were "
        f"cancelled — the lock is being handed to tickets whose callers are "
        f"gone, and every real caller now waits out a recovery window"
    )
    remaining = await asyncio.wait_for(gate.queue_size(), timeout=20)
    assert remaining == 0, (
        f"{remaining} tickets left by 6 cancelled acquires; a graph engine "
        f"cancels constantly, so this queue grows for the life of the process"
    )


async def test_unreleased_lock_is_survivable(gate):
    """
    The graph errored after the lock node. Nothing will ever release.

    This is not a hypothetical: the consumer has no finaliser, no context
    manager and no other path that releases. When a graph branches away or dies
    after acquiring, the lock stays held until the *process* goes.

    Three things must hold. Waiters given a timeout must actually get their
    timeout back rather than hang (`continue_if_lock_timeout` depends on it).
    Their giving up must not corrupt the queue. And once the holding process
    really does die, the lock must still come back — the abandoned ticket must
    not survive to block the successor.

    Every test in the sibling suite releases what it acquires.
    """
    monitor = Monitor()

    stranded = await asyncio.wait_for(gate.acquire(timeout=30), timeout=40)
    monitor.enter("stranded", stranded)

    # Six graph executions hit the lock node, time out, and carry on regardless.
    results = await asyncio.wait_for(
        asyncio.gather(
            *[acquire_or_none(gate, timeout=0.5, deadline=25) for _ in range(6)]
        ),
        timeout=60,
    )
    assert all(r is None for r in results), (
        "a waiter was granted a lock that was never released — "
        f"{sum(r is not None for r in results)} of 6 got in"
    )

    # The holding process finally dies, still without releasing.
    monitor.exit("stranded")
    await asyncio.wait_for(gate.abandon(stranded), timeout=20)

    successor = await hold(
        gate,
        monitor,
        "successor",
        hold_s=0.05,
        acquire_timeout=gate.recovery_bound_s,
        deadline_s=gate.recovery_bound_s + 15,
    )
    assert successor is not None, (
        f"after a never-released lock and 6 timed-out waiters, nobody could "
        f"reclaim the lock within {gate.recovery_bound_s:.0f}s of the holder "
        f"dying"
    )

    monitor.assert_exclusive()
    remaining = await asyncio.wait_for(gate.queue_size(), timeout=20)
    assert remaining == 0, (
        f"{remaining} tickets survived a never-released lock plus 6 timeouts"
    )


async def test_timeout_retry_storm_makes_progress_without_overlap(gate):
    """
    Every caller times out and immediately tries again, for seconds on end.

    With ``continue_if_lock_timeout`` a timeout is routine, and the graph comes
    straight back for another attempt. Under load that means a permanent churn
    of tickets being taken and withdrawn while grants are being handed out — the
    withdrawal path and the handoff path running concurrently, constantly.

    Failing looks like any of: two callers inside at once because a ticket was
    withdrawn just as it was being granted; livelock, where the churn is so
    heavy nobody ever gets in; or a queue that grows by one per abandoned
    attempt.

    The sibling suite times waiters out once, from a standing start, and never
    retries.
    """
    monitor = Monitor()
    budget_s = 6.0
    successes = 0
    timeouts = 0

    async def storming_caller(i):
        nonlocal successes, timeouts
        label = f"storm{i}"
        deadline = time.monotonic() + budget_s
        while time.monotonic() < deadline:
            grant = await acquire_or_none(gate, timeout=0.35, deadline=30)
            if grant is None:
                timeouts += 1
                continue
            successes += 1
            monitor.enter(label, grant)
            try:
                await asyncio.sleep(0.25)
            finally:
                monitor.exit(label)
                await asyncio.wait_for(gate.release(grant), timeout=30)

    await asyncio.wait_for(
        asyncio.gather(*[storming_caller(i) for i in range(8)]),
        timeout=budget_s + 120,
    )
    await asyncio.sleep(1.0)

    monitor.assert_exclusive()
    assert successes >= 2, (
        f"livelock: 8 callers retried for {budget_s:.0f}s and only {successes} "
        f"ever got in ({timeouts} timeouts)"
    )
    remaining = await asyncio.wait_for(gate.queue_size(), timeout=20)
    assert remaining == 0, (
        f"{remaining} tickets left after a retry storm of {timeouts} abandoned "
        f"attempts — the queue grows with every timeout the consumer treats as "
        f"routine"
    )


# -- 5. one process, many locks; many processes, one lock --------------------


async def test_many_lock_names_share_one_client_without_interfering(impl):
    """
    One process-global client, one connection pool, many lock names.

    The consumer templates its lock names (``lock_{workspace_id}``) and caches a
    gate per name forever, so a busy process holds dozens at once over a single
    client. Two independent failures live here: keys derived so that distinct
    names collide (holding one lock excludes callers of another), and a shared
    waiting mechanism that serialises unrelated locks behind each other.

    So this asserts both exclusion *within* each name and independence
    *between* names — eight locks each held three times must finish in roughly
    the time of one, not eight.

    Every test in the sibling suite uses exactly one lock name.
    """
    cls, label = impl
    names = 8
    rounds = 3
    hold_s = 0.6

    client = await make_client()
    gates = [
        cls(client, unique_name(f"{label}.n{i}"), WAKE_MS, DEAD_HOLDER_MS)
        for i in range(names)
    ]
    monitors = [Monitor() for _ in gates]

    try:

        async def exercise(g, monitor, i):
            await asyncio.gather(
                *[hold(g, monitor, f"n{i}.{k}", hold_s, 90, 120) for k in range(rounds)]
            )

        started = time.monotonic()
        await asyncio.wait_for(
            asyncio.gather(
                *[exercise(g, m, i) for i, (g, m) in enumerate(zip(gates, monitors))]
            ),
            timeout=240,
        )
        wall = time.monotonic() - started

        for i, monitor in enumerate(monitors):
            assert not monitor.overlaps, (
                f"lock name #{i} let two callers in at once while 7 other lock "
                f"names were live on the same client; first: "
                f"{monitor.overlaps[0]}"
            )
            assert len(monitor.grants) == rounds, monitor.summary()

        serialised = names * rounds * hold_s
        assert wall < serialised * 0.5, (
            f"{names} unrelated lock names took {wall:.1f}s, close to the "
            f"{serialised:.1f}s they would take if they all queued behind each "
            f"other — distinct lock names are not independent"
        )
    finally:
        for g in gates:
            await g.cleanup()
        await client.aclose()


async def test_separate_instances_contend_for_one_lock_name(impl):
    """
    Six gate objects on six clients, one lock name: the real deployment.

    Production runs several Django workers, so the callers contending for a lock
    are in different processes with different clients and different gate
    objects. Every existing test — unit, integration and adversarial — drives a
    *single* gate instance, so any coordination that quietly relies on in-process
    state (a local "I hold it" flag, a shared in-memory queue, a per-object
    counter) would satisfy the whole suite and still fail the moment the library
    is deployed on more than one worker.

    Mutual exclusion and FIFO are both claimed against Redis, not against a
    process, so both must hold here.
    """
    cls, label = impl
    name = unique_name(f"{label}.shared")
    procs = 6

    clients = [await make_client() for _ in range(procs)]
    gates = [cls(c, name, WAKE_MS, DEAD_HOLDER_MS) for c in clients]
    monitor = Monitor()

    try:
        tasks = []
        for i, g in enumerate(gates):
            tasks.append(
                asyncio.create_task(
                    hold(g, monitor, f"proc{i}", WAKE_MS / 1000 * 3, 90, 120)
                )
            )
            await asyncio.sleep(0.05)  # unambiguous arrival order at the server

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=180)

        monitor.assert_exclusive()
        monitor.assert_fifo()
        assert len(monitor.grants) == procs, monitor.summary()

        remaining = await asyncio.wait_for(gates[0].queue_size(), timeout=20)
        assert remaining == 0, (
            f"{remaining} tickets left after {procs} separate instances each released"
        )
    finally:
        await gates[0].cleanup()
        for c in clients:
            await c.aclose()


async def test_lock_abandoned_by_a_still_running_process_is_reclaimable(gate):
    """
    The graph errored between the lock node and the unlock node — and the
    worker process carried right on serving other requests.

    This is the consumer's *ordinary* error path, and it is a different case
    from `test_unreleased_lock_is_survivable` above, which kills the holding
    process before checking that the lock comes back. Killing the process is
    the easy case: whatever the holder was doing to keep the lock stops when
    the holder stops.

    Here nothing dies. If an implementation keeps a lock alive on the holder's
    behalf in the background, it will keep doing so forever for a caller that
    has long since gone away, and that lock name is wedged for the entire life
    of the worker — no crash, no partition, no timeout, no error anywhere.

    A lock that outlives every caller who could release it is not recoverable
    by anyone, which is the one failure an operator cannot work around.
    """
    monitor = Monitor()

    stranded = await asyncio.wait_for(gate.acquire(timeout=30), timeout=40)
    monitor.enter("stranded", stranded)

    # The graph dies here. Note what we do NOT do: we never release, and we
    # never call abandon() — the process is healthy and still running, exactly
    # as a Django worker would be after one workflow raised.
    del stranded

    # Each implementation declares how long it may take to reclaim a lock
    # abandoned by a process that is still running. For one that renews a lease
    # in the background that is necessarily longer than plain crash recovery,
    # because it cannot tell an abandoned caller from an honest slow one.
    grace = gate.abandon_bound_s
    successor = await hold(
        gate,
        monitor,
        "successor",
        hold_s=0.05,
        acquire_timeout=grace,
        deadline_s=grace + 15,
    )

    assert successor is not None, (
        f"nobody could take the lock within {grace:.0f}s of it being abandoned "
        f"by a process that is still alive and healthy. If the lock is being "
        f"renewed in the background on behalf of a caller that has gone away, "
        f"this lock name is now wedged until the worker restarts"
    )
