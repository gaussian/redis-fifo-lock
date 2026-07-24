"""
Integration tests for the synchronous gate against a real Redis.

``tests/test_sync.py`` drives ``StreamGate`` entirely through ``MagicMock``, so
nothing in the suite has ever pointed the sync gate at a real server. Every
blocking call it makes is therefore unverified: mocks return instantly and never
enforce a read deadline.

Threads here are daemons joined with a timeout, so a gate that blocks forever
fails the test instead of hanging the suite.
"""

import os
import threading
import time
import uuid

import pytest
import redis

from redis_fifo_lock.sync import StreamGate


def redis_url():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
    if not url.startswith("redis://") and not url.startswith("rediss://"):
        url = f"redis://{url}/15"
    return url


class Runner(threading.Thread):
    """Runs ``fn`` on a daemon thread, capturing its result or exception."""

    def __init__(self, fn):
        super().__init__(daemon=True)
        self.fn = fn
        self.result = None
        self.error = None
        self.elapsed = None

    def run(self):
        started = time.monotonic()
        try:
            self.result = self.fn()
        except BaseException as e:  # noqa: BLE001 - the test inspects it
            self.error = e
        finally:
            self.elapsed = time.monotonic() - started


@pytest.fixture
def gate_factory():
    """Builds sync gates with unique keys over caller-configured clients."""
    created = []

    def factory(**client_kwargs):
        client = redis.Redis.from_url(redis_url(), **client_kwargs)
        try:
            client.ping()
        except Exception as e:  # pragma: no cover - environment dependent
            pytest.skip(f"Redis not available: {e}")
        test_id = str(uuid.uuid4())[:8]
        gate = StreamGate(
            client,
            stream=f"test-sync-stream:{test_id}",
            group=f"test-sync-group:{test_id}",
            sig_prefix=f"test-sync-sig:{test_id}:",
            last_key=f"test-sync-last:{test_id}",
        )
        created.append(gate)
        return gate

    yield factory

    for gate in created:
        try:
            keys = gate.r.keys(f"{gate.sig_prefix}*")
            gate.r.delete(gate.stream, gate.last_key, *keys)
        except Exception:
            pass
        try:
            gate.r.close()
        except Exception:
            pass


class TestSyncRelease:
    """``release()`` must not block on an empty queue."""

    def test_release_on_empty_queue_returns_promptly(self, gate_factory):
        """XREADGROUP ``block=0`` means *wait forever*, not *do not wait*."""
        gate = gate_factory()

        runner = Runner(lambda: gate.release("nobody", "0-0"))
        runner.start()
        runner.join(timeout=8)

        assert not runner.is_alive(), (
            "release() never returned on an empty queue: XREADGROUP was sent "
            "BLOCK 0, which blocks indefinitely"
        )
        assert runner.error is None, (
            f"release() raised {type(runner.error).__name__}: {runner.error}"
        )
        assert runner.elapsed < 3, (
            f"release() took {runner.elapsed:.1f}s on an empty queue"
        )


class TestSyncSocketTimeout:
    """The caller's socket timeout must not masquerade as a gate timeout."""

    def test_acquire_timeout_is_the_gates_not_the_sockets(self, gate_factory):
        """A waiter must reach its own deadline, not die at the read deadline."""
        gate = gate_factory(socket_timeout=1.0)
        gate.acquire()  # hold the lock so the next caller genuinely waits

        runner = Runner(lambda: gate.acquire(timeout=3))
        runner.start()
        runner.join(timeout=15)

        assert not runner.is_alive(), "acquire() never returned"
        assert isinstance(runner.error, TimeoutError), (
            f"expected the gate's own TimeoutError after 3s, got "
            f"{type(runner.error).__name__}: {runner.error}"
        )
        assert not isinstance(runner.error, redis.exceptions.TimeoutError), (
            f"the client's 1.0s socket timeout surfaced as the acquire failure "
            f"after {runner.elapsed:.1f}s: {runner.error}"
        )
        assert runner.elapsed >= 2.5, (
            f"acquire(timeout=3) gave up after {runner.elapsed:.1f}s, which is the "
            f"client's 1.0s read deadline rather than the caller's 3s timeout"
        )

    def test_acquire_with_no_timeout_outlives_the_socket_timeout(self, gate_factory):
        """``timeout=None`` means block indefinitely, whatever the client does."""
        gate = gate_factory(socket_timeout=1.0)
        gate.acquire()  # hold the lock so the next caller genuinely waits

        runner = Runner(lambda: gate.acquire(timeout=None))
        runner.start()
        runner.join(timeout=4)

        assert runner.is_alive() or runner.error is None, (
            f"acquire(timeout=None) failed after {runner.elapsed:.1f}s with "
            f"{type(runner.error).__name__}: {runner.error} - it should still be "
            f"waiting, the client's 1.0s read deadline is not the caller's problem"
        )


class TestSyncDispatch:
    """The first caller on an idle gate should get the lock."""

    def test_first_acquire_on_empty_gate_is_dispatched(self, gate_factory):
        """Nothing else is running, so there is nobody else to hand off the baton."""
        gate = gate_factory()

        runner = Runner(lambda: gate.acquire(timeout=5))
        runner.start()
        runner.join(timeout=15)

        assert not runner.is_alive(), "acquire() never returned"
        assert runner.error is None, (
            f"first acquire() on an idle gate failed with "
            f"{type(runner.error).__name__}: {runner.error}"
        )
        owner, msg_id = runner.result
        assert owner and msg_id
        gate.release(owner, msg_id.decode() if isinstance(msg_id, bytes) else msg_id)

    def test_baton_passes_to_the_waiter_on_release(self, gate_factory):
        """The whole point: release() hands the lock to whoever is next."""
        gate = gate_factory()

        owner1, msg1 = gate.acquire(timeout=5)

        runner = Runner(lambda: gate.acquire(timeout=15))
        runner.start()
        time.sleep(1)
        assert runner.is_alive(), "second caller acquired while the lock was held"

        gate.release(owner1, msg1.decode() if isinstance(msg1, bytes) else msg1)
        runner.join(timeout=15)

        assert not runner.is_alive(), "waiter was never dispatched after release()"
        assert runner.error is None, (
            f"waiter failed with {type(runner.error).__name__}: {runner.error}"
        )
        owner2, msg2 = runner.result
        assert owner2 != owner1
        gate.release(owner2, msg2.decode() if isinstance(msg2, bytes) else msg2)

    def test_fifo_order_across_threads(self, gate_factory):
        """Three queued threads are served in the order they enqueued."""
        gate = gate_factory()

        owner1, msg1 = gate.acquire(timeout=5)

        order = []
        lock = threading.Lock()

        def worker(index):
            owner, msg_id = gate.acquire(timeout=30)
            with lock:
                order.append(index)
            time.sleep(0.1)
            gate.release(
                owner, msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            )

        runners = []
        for i in range(3):
            runner = Runner(lambda i=i: worker(i))
            runner.start()
            runners.append(runner)
            time.sleep(0.3)  # stagger so enqueue order is unambiguous

        gate.release(owner1, msg1.decode() if isinstance(msg1, bytes) else msg1)

        for runner in runners:
            runner.join(timeout=30)
            assert not runner.is_alive(), "a queued thread never acquired"
            assert runner.error is None, (
                f"queued thread failed with {type(runner.error).__name__}: "
                f"{runner.error}"
            )

        assert order == [0, 1, 2], f"threads were served out of order: {order}"
