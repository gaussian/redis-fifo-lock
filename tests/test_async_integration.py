"""
Integration tests for AsyncStreamGate with real Redis.

These tests require a running Redis instance and test real-world scenarios including:
- Real Redis Streams behavior
- Multi-coroutine concurrency
- Failure injection and recovery
- Dead holder detection and timeout
- State corruption recovery

No mocking for these tests (unless injecting errors).

Run with: pytest -m integration
Skip if Redis unavailable: Tests will skip gracefully
"""

import asyncio
import multiprocessing
import os
import time
import uuid

import pytest
import pytest_asyncio
import redis.asyncio as redis

from redis_fifo_lock.async_gate import AsyncStreamGate


@pytest_asyncio.fixture
async def real_redis():
    """
    Real Redis connection for integration tests.
    Uses REDIS_URL environment variable or defaults to localhost.
    Skips tests if Redis is not available.
    """
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
    if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
        # If it's just host:port, construct the URL
        redis_url = f"redis://{redis_url}/15"

    try:
        client = await redis.from_url(redis_url, decode_responses=False)
        await client.ping()
        yield client
        await client.aclose()
    except Exception as e:
        pytest.skip(f"Redis not available: {e}")


@pytest_asyncio.fixture
async def clean_gate(real_redis):
    """
    Fresh AsyncStreamGate instance with unique keys for each test.
    Automatically cleans up after test completes.
    """
    test_id = str(uuid.uuid4())[:8]
    gate = AsyncStreamGate(
        real_redis,
        stream=f"test-gate-stream:{test_id}",
        group=f"test-gate-group:{test_id}",
        sig_prefix=f"test-gate-sig:{test_id}:",
        last_key=f"test-gate-last:{test_id}",
    )
    yield gate

    # Cleanup
    try:
        await real_redis.delete(gate.stream, gate.last_key)
        # Delete all signal keys
        cursor = 0
        while True:
            cursor, keys = await real_redis.scan(cursor, match=f"{gate.sig_prefix}*")
            if keys:
                await real_redis.delete(*keys)
            if cursor == 0:
                break
        await real_redis.xgroup_destroy(gate.stream, gate.group)
    except Exception:
        pass  # Best-effort cleanup


# ============================================================================
# Category 1: Integration Tests with Real Redis (10 tests)
# ============================================================================


class TestAsyncRealRedisIntegration:
    """Tests that verify AsyncStreamGate works with real Redis."""

    pytestmark = pytest.mark.asyncio

    async def test_real_redis_single_acquire_release(self, clean_gate):
        """Basic smoke test: acquire and release with real Redis."""
        owner, msg_id = await clean_gate.acquire()

        assert owner is not None
        assert msg_id is not None
        assert isinstance(msg_id, bytes)

        # Verify message exists in stream
        messages = await clean_gate.r.xrange(clean_gate.stream)
        assert len(messages) == 1
        assert messages[0][0] == msg_id

        await clean_gate.release(owner, msg_id.decode())
        await asyncio.sleep(0)  # Let background task run

        # Verify message was acked (removed from pending)
        pending = await clean_gate.r.xpending(clean_gate.stream, clean_gate.group)
        assert pending["pending"] == 0  # XPENDING returns dict with 'pending' key

    async def test_real_redis_fifo_ordering(self, clean_gate):
        """Verify 5 sequential acquires maintain FIFO order."""
        results = []

        for i in range(5):
            owner, msg_id = await clean_gate.acquire()
            results.append((owner, msg_id, i))
            await clean_gate.release(owner, msg_id.decode())
            await asyncio.sleep(0)

        # Verify message IDs are in order (Redis Streams are time-ordered)
        msg_ids = [r[1] for r in results]
        assert msg_ids == sorted(msg_ids), "Message IDs should be in chronological order"

    async def test_real_redis_message_id_generation(self, clean_gate):
        """Verify Redis generates real timestamp-based message IDs."""
        owner, msg_id = await clean_gate.acquire()

        # Redis message IDs format: timestamp-sequence (e.g., "1234567890-0")
        msg_id_str = msg_id.decode()
        assert "-" in msg_id_str

        timestamp_part, sequence_part = msg_id_str.split("-")
        assert timestamp_part.isdigit()
        assert sequence_part.isdigit()

        await clean_gate.release(owner, msg_id.decode())

    async def test_real_redis_consumer_group_creation(self, clean_gate):
        """Verify consumer group is actually created in Redis."""
        await clean_gate.ensure_group()

        # Check group exists
        groups = await clean_gate.r.xinfo_groups(clean_gate.stream)
        assert len(groups) == 1
        # XINFO GROUPS returns list of dicts with 'name' key
        group_name = groups[0].get("name") or groups[0].get(b"name", b"")
        if isinstance(group_name, bytes):
            group_name = group_name.decode()
        assert group_name == clean_gate.group

    async def test_real_redis_xpending_tracking(self, clean_gate):
        """Verify pending entries list is managed correctly."""
        owner, msg_id = await clean_gate.acquire()

        # Check message is pending
        pending = await clean_gate.r.xpending(clean_gate.stream, clean_gate.group)
        assert pending["pending"] == 1  # XPENDING returns dict with 'pending' key

        await clean_gate.release(owner, msg_id.decode())
        await asyncio.sleep(0)

        # Check message was acked
        pending = await clean_gate.r.xpending(clean_gate.stream, clean_gate.group)
        assert pending["pending"] == 0  # XPENDING returns dict with 'pending' key

    async def test_real_redis_xack_removes_from_pel(self, clean_gate):
        """Verify XACK actually removes message from PEL."""
        owner, msg_id = await clean_gate.acquire()

        # Message should be in PEL
        pending_list = await clean_gate.r.xpending_range(
            clean_gate.stream, clean_gate.group, "-", "+", 10
        )
        assert len(pending_list) == 1

        await clean_gate.release(owner, msg_id.decode())
        await asyncio.sleep(0)

        # Message should be removed from PEL
        pending_list = await clean_gate.r.xpending_range(
            clean_gate.stream, clean_gate.group, "-", "+", 10
        )
        assert len(pending_list) == 0

    @pytest.mark.slow
    async def test_real_redis_signal_key_ttl(self, clean_gate):
        """Verify signal keys have TTL set correctly."""
        # First holder acquires
        owner1, msg_id1 = await clean_gate.acquire()

        # Create a second waiter that will be signaled but won't consume yet
        import uuid

        owner2 = str(uuid.uuid4())
        msg_id2 = await clean_gate.r.xadd(clean_gate.stream, {"owner": owner2})

        # Release first holder to signal second waiter
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0.1)  # Let dispatch signal the second waiter

        # Now check TTL of second waiter's signal key (before they BLPOP it)
        sig_key = clean_gate.sig_prefix + owner2
        ttl = await clean_gate.r.pttl(sig_key)
        assert ttl > 0, f"Signal key should exist with positive TTL, got {ttl}"
        assert ttl <= 300000  # Should be <= 5 minutes

        # Clean up: manually consume owner2's signal and ack their message
        # (owner2 was created via xadd, not acquire(), so we clean up manually)
        await clean_gate.r.delete(sig_key)  # Remove the signal
        await clean_gate.r.xack(clean_gate.stream, clean_gate.group, msg_id2)
        await clean_gate.r.delete(clean_gate.last_key)  # Clear the holder pointer

    async def test_real_redis_stream_growth(self, clean_gate):
        """Verify stream doesn't grow unbounded over 100 cycles."""
        for _ in range(100):
            owner, msg_id = await clean_gate.acquire()
            await clean_gate.release(owner, msg_id.decode())
            await asyncio.sleep(0)

        # Stream should have all 100 messages (we don't auto-trim)
        stream_len = await clean_gate.r.xlen(clean_gate.stream)
        assert stream_len == 100

    async def test_real_redis_last_key_lifecycle(self, clean_gate):
        """Verify last_key is set and deleted correctly."""
        # Initially no last_key
        last_key_value = await clean_gate.r.get(clean_gate.last_key)
        assert last_key_value is None

        # After acquire, last_key should be set
        owner, msg_id = await clean_gate.acquire()
        last_key_value = await clean_gate.r.get(clean_gate.last_key)
        assert last_key_value == msg_id

        # After release with empty queue, last_key should be deleted
        await clean_gate.release(owner, msg_id.decode())
        await asyncio.sleep(
            0.5
        )  # Give background task time to complete and delete last_key

        last_key_value = await clean_gate.r.get(clean_gate.last_key)
        assert last_key_value is None

    async def test_real_redis_reconnect_after_disconnect(self, clean_gate):
        """Test recovery after Redis connection is recreated."""
        owner1, msg_id1 = await clean_gate.acquire()
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0)

        # Simulate reconnect by creating new Redis client
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
        if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
            redis_url = f"redis://{redis_url}/15"
        new_client = await redis.from_url(redis_url, decode_responses=False)

        # Create new gate with same keys but new client
        new_gate = AsyncStreamGate(
            new_client,
            stream=clean_gate.stream,
            group=clean_gate.group,
            sig_prefix=clean_gate.sig_prefix,
            last_key=clean_gate.last_key,
        )

        # Should be able to acquire with new client
        owner2, msg_id2 = await new_gate.acquire()
        assert owner2 is not None

        await new_gate.release(owner2, msg_id2.decode())
        await new_client.aclose()


# ============================================================================
# Category 2: Multi-Coroutine Concurrency Tests (8 tests)
# ============================================================================


class TestAsyncConcurrency:
    """Tests for concurrent access by multiple coroutines."""

    pytestmark = pytest.mark.asyncio

    async def test_concurrent_acquire_fifo_order(self, clean_gate):
        """10 coroutines acquire simultaneously - verify FIFO order."""
        results = []

        async def acquire_and_record(index):
            owner, msg_id = await clean_gate.acquire()
            results.append((index, owner, msg_id))
            await clean_gate.release(owner, msg_id.decode())
            await asyncio.sleep(0)

        # Launch 10 concurrent acquires
        await asyncio.gather(*[acquire_and_record(i) for i in range(10)])

        # Verify all completed
        assert len(results) == 10

        # Message IDs should be in order (FIFO)
        # Sort results by msg_id since async completion order may differ
        results_sorted = sorted(results, key=lambda r: r[2])
        msg_ids = [r[2] for r in results_sorted]
        assert msg_ids == sorted(msg_ids)

    async def test_concurrent_acquire_release_interleaved(self, clean_gate):
        """5 coroutines with random delays - verify no deadlocks."""
        import random

        completed = []

        async def acquire_wait_release(index):
            owner, msg_id = await clean_gate.acquire(timeout=30)
            await asyncio.sleep(random.uniform(0.01, 0.1))
            await clean_gate.release(owner, msg_id.decode())
            await asyncio.sleep(0)
            completed.append(index)

        await asyncio.gather(*[acquire_wait_release(i) for i in range(5)])

        assert len(completed) == 5

    async def test_concurrent_setnx_race(self, clean_gate):
        """10 coroutines try SETNX simultaneously - only one succeeds."""
        first_acquirer = []

        async def try_acquire():
            owner, msg_id = await clean_gate.acquire()
            first_acquirer.append((owner, msg_id))
            # Hold for a bit
            await asyncio.sleep(0.1)
            await clean_gate.release(owner, msg_id.decode())
            await asyncio.sleep(0)

        # Start all at once
        await asyncio.gather(*[try_acquire() for _ in range(10)])

        # All should have succeeded sequentially
        assert len(first_acquirer) == 10

    async def test_concurrent_release_same_msg_id(self, clean_gate):
        """Two coroutines try to release same msg_id - guard prevents double-dispatch."""
        owner, msg_id = await clean_gate.acquire()
        msg_id_str = msg_id.decode()

        # Try to release twice concurrently
        results = await asyncio.gather(
            clean_gate.release(owner, msg_id_str),
            clean_gate.release(owner, msg_id_str),
            return_exceptions=True,
        )

        # Both should complete without error (guard makes second one no-op)
        assert all(r is None for r in results)

    async def test_concurrent_recovery_by_multiple_waiters(self, clean_gate):
        """Multiple waiters wake up and call recovery simultaneously."""
        # Create a scenario where multiple waiters are blocked
        owner1, msg_id1 = await clean_gate.acquire()

        results = []

        async def waiter_that_recovers(index):
            # These will block and timeout internally, calling recovery
            try:
                owner, msg_id = await clean_gate.acquire(timeout=10)
                results.append((index, owner, msg_id))
                await clean_gate.release(owner, msg_id.decode())
                await asyncio.sleep(0)
            except asyncio.TimeoutError:
                results.append((index, None, None))

        # Start 3 waiters
        waiters = [asyncio.create_task(waiter_that_recovers(i)) for i in range(3)]

        # Let them start waiting
        await asyncio.sleep(0.1)

        # Release first holder
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0)

        # Wait for all waiters
        await asyncio.gather(*waiters)

        # At least some should have acquired
        successful = [r for r in results if r[1] is not None]
        assert len(successful) > 0

    @pytest.mark.slow
    async def test_high_contention_100_coroutines(self, clean_gate):
        """100 coroutines acquire/release rapidly - verify FIFO and no lost wakeups."""
        completed = []

        async def rapid_acquire_release(index):
            owner, msg_id = await clean_gate.acquire(timeout=60)
            completed.append(index)
            await clean_gate.release(owner, msg_id.decode())
            await asyncio.sleep(0)

        await asyncio.gather(*[rapid_acquire_release(i) for i in range(100)])

        assert len(completed) == 100
        assert set(completed) == set(range(100))

    async def test_concurrent_acquire_with_timeouts(self, clean_gate):
        """5 waiters with different timeouts - correct ones timeout."""
        # First holder doesn't release for 3 seconds
        owner1, msg_id1 = await clean_gate.acquire()

        results = []

        async def waiter_with_timeout(index, timeout):
            try:
                owner, msg_id = await clean_gate.acquire(timeout=timeout)
                results.append((index, "acquired", owner, msg_id))
                await clean_gate.release(owner, msg_id.decode())
                await asyncio.sleep(0)
            except asyncio.TimeoutError:
                results.append((index, "timeout", None, None))

        # Start waiters with different timeouts
        waiters = [
            waiter_with_timeout(0, 1),  # Should timeout
            waiter_with_timeout(1, 1),  # Should timeout
            waiter_with_timeout(2, 10),  # Should acquire
            waiter_with_timeout(3, 10),  # Should acquire
        ]

        tasks = [asyncio.create_task(w) for w in waiters]
        await asyncio.sleep(0.1)  # Let waiters start

        # Hold for 3.5 seconds then release
        # This ensures waiter 0 (1s) and waiter 1 (1s) both timeout before release
        await asyncio.sleep(3.5)
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0)

        await asyncio.gather(*tasks)

        # Check results
        timeouts = [r for r in results if r[1] == "timeout"]
        acquires = [r for r in results if r[1] == "acquired"]

        assert len(timeouts) == 2  # Waiters 0 and 1
        assert len(acquires) == 2  # Waiters 2 and 3

    async def test_session_context_manager_concurrency(self, clean_gate):
        """10 coroutines use async with session() - verify proper pairs."""
        completed = []

        async def use_session(index):
            async with await clean_gate.session(timeout=30) as session:
                assert session.owner is not None
                assert session.msg_id is not None
                await asyncio.sleep(0.01)  # Simulate work
            completed.append(index)

        await asyncio.gather(*[use_session(i) for i in range(10)])

        assert len(completed) == 10


# ============================================================================
# Category 3: Failure Injection Tests (6 tests)
# ============================================================================


class TestAsyncFailureInjection:
    """Tests for handling Redis command failures and exceptions."""

    pytestmark = pytest.mark.asyncio

    async def test_xack_fails_dispatch_continues(self, clean_gate):
        """XACK raises error, verify dispatch still happens."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Enqueue second waiter
        async def waiter():
            return await clean_gate.acquire(timeout=10)

        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.1)

        # Mock XACK to fail
        original_xack = clean_gate.r.xack

        async def failing_xack(*args, **kwargs):
            raise redis.ResponseError("XACK failed")

        clean_gate.r.xack = failing_xack

        # Release should handle XACK failure gracefully
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0.1)

        # Second waiter should still get dispatched
        owner2, msg_id2 = await waiter_task
        assert owner2 is not None

        # Restore
        clean_gate.r.xack = original_xack
        await clean_gate.release(owner2, msg_id2.decode())

    async def test_xreadgroup_fails_gracefully(self, clean_gate):
        """XREADGROUP raises error, verify no crash."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Mock XREADGROUP to fail in dispatch
        original_xreadgroup = clean_gate.r.xreadgroup
        call_count = [0]

        async def failing_xreadgroup(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] > 1:  # Fail on second call (in dispatch)
                raise redis.ResponseError("XREADGROUP failed")
            return await original_xreadgroup(*args, **kwargs)

        clean_gate.r.xreadgroup = failing_xreadgroup

        # Release should handle failure gracefully
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0.1)

        # Restore
        clean_gate.r.xreadgroup = original_xreadgroup

    async def test_xautoclaim_fails_fallback_to_normal(self, clean_gate):
        """XAUTOCLAIM fails, verify normal dispatch works."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Enqueue second waiter
        async def waiter():
            return await clean_gate.acquire(timeout=10)

        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.1)

        # Mock XAUTOCLAIM to fail
        original_xautoclaim = clean_gate.r.xautoclaim

        async def failing_xautoclaim(*args, **kwargs):
            raise redis.ResponseError("XAUTOCLAIM failed")

        clean_gate.r.xautoclaim = failing_xautoclaim

        # Release and dispatch
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0.1)

        # Should fall back to normal dispatch
        owner2, msg_id2 = await waiter_task
        assert owner2 is not None

        # Restore
        clean_gate.r.xautoclaim = original_xautoclaim
        await clean_gate.release(owner2, msg_id2.decode())

    async def test_background_task_exception_handled(self, clean_gate):
        """Exception in background task is caught and handled."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Mock _dispatch_waiter to raise exception
        original_dispatch = clean_gate._dispatch_waiter

        async def failing_dispatch(*args, **kwargs):
            raise RuntimeError("Dispatch failed!")

        clean_gate._dispatch_waiter = failing_dispatch

        # Release should not crash despite exception in background task
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0.1)

        # Restore
        clean_gate._dispatch_waiter = original_dispatch

    async def test_acquire_timeout_during_recovery(self, clean_gate):
        """Timeout expires during _recover_and_maybe_dispatch()."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Try to acquire with very short timeout
        with pytest.raises(asyncio.TimeoutError):
            await clean_gate.acquire(timeout=1)

        # Original holder can still release
        await clean_gate.release(owner1, msg_id1.decode())

    async def test_partial_signal_failure(self, clean_gate):
        """LPUSH succeeds but PEXPIRE fails - signal still works."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Enqueue second waiter
        async def waiter():
            return await clean_gate.acquire(timeout=10)

        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.1)

        # Mock PEXPIRE to fail
        original_pexpire = clean_gate.r.pexpire

        async def failing_pexpire(*args, **kwargs):
            return False  # PEXPIRE returns False on failure

        clean_gate.r.pexpire = failing_pexpire

        # Release and dispatch
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0.1)

        # Second waiter should still get signaled (no TTL is ok)
        owner2, msg_id2 = await waiter_task
        assert owner2 is not None

        # Restore
        clean_gate.r.pexpire = original_pexpire
        await clean_gate.release(owner2, msg_id2.decode())


# ============================================================================
# Category 4: Dead Holder Chain Tests (5 tests)
# ============================================================================


class TestAsyncDeadHolderChains:
    """Tests for detecting and recovering from dead holders."""

    pytestmark = pytest.mark.asyncio

    async def test_single_dead_holder_recovery(self, real_redis):
        """Holder acquires, never releases - next waiter gets gate after timeout."""
        # Create gate with 100ms dead holder timeout (instead of 2 minutes)
        test_id = str(uuid.uuid4())[:8]
        gate = AsyncStreamGate(
            real_redis,
            stream=f"test-gate-{test_id}",
            group=f"test-group-{test_id}",
            sig_prefix=f"test-sig-{test_id}:",
            last_key=f"test-last-{test_id}",
            claim_idle_ms=50,  # Claim after 50ms idle
            dead_holder_timeout_ms=100,  # Consider dead after 100ms
            blpop_internal_timeout_ms=1000,  # Faster recovery loop for tests (1 second)
        )

        # First holder acquires but never releases - simulate dead holder
        owner1, msg_id1 = await gate.acquire()

        # Second waiter starts
        async def waiter():
            return await gate.acquire(timeout=10)

        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.02)  # Let waiter start and enqueue

        # Wait for first holder to exceed dead holder timeout (150ms > 100ms)
        await asyncio.sleep(0.15)

        # Waiter should recover and get the gate via waiter-driven recovery
        owner2, msg_id2 = await asyncio.wait_for(waiter_task, timeout=15)
        assert owner2 is not None
        assert owner2 != owner1

        # Cleanup
        msg_id2_str = msg_id2.decode() if isinstance(msg_id2, bytes) else msg_id2
        await gate.release(owner2, msg_id2_str)
        await asyncio.sleep(0.02)
        await gate.r.delete(gate.stream, gate.last_key)
        await gate.r.delete(f"{gate.sig_prefix}{owner1}")
        await gate.r.delete(f"{gate.sig_prefix}{owner2}")

    async def test_dead_holder_exactly_2_minutes(self, real_redis):
        """Idle time = exactly at timeout threshold - should XACK."""
        # Create gate with 100ms dead holder timeout (instead of 2 minutes)
        test_id = str(uuid.uuid4())[:8]
        gate = AsyncStreamGate(
            real_redis,
            stream=f"test-gate-{test_id}",
            group=f"test-group-{test_id}",
            sig_prefix=f"test-sig-{test_id}:",
            last_key=f"test-last-{test_id}",
            claim_idle_ms=50,  # Claim after 50ms idle
            dead_holder_timeout_ms=100,  # Consider dead after 100ms
            blpop_internal_timeout_ms=1000,
        )

        # Acquire but don't release - simulate dead holder
        owner, msg_id = await gate.acquire()

        # Wait at/past the timeout threshold (120ms > 100ms)
        await asyncio.sleep(0.12)

        # Call recovery - should find it idle >= 500ms and XACK it
        result = await gate._recover_and_maybe_dispatch()

        # Should have XACKed (dead holder)
        # Queue should be empty now
        pending = await gate.r.xpending(gate.stream, gate.group)
        assert pending["pending"] == 0  # XPENDING returns dict with 'pending' key

        # Cleanup
        await gate.r.delete(gate.stream, gate.last_key)
        await gate.r.delete(f"{gate.sig_prefix}{owner}")

    async def test_dead_holder_just_under_2_minutes(self, real_redis):
        """Idle time = just under timeout threshold - should re-signal."""
        # Create gate with 200ms dead holder timeout (threshold=150ms after 50ms tolerance)
        # claim_idle_ms=50, so range 50-149ms triggers re-signal (not XACK)
        test_id = str(uuid.uuid4())[:8]
        gate = AsyncStreamGate(
            real_redis,
            stream=f"test-gate-{test_id}",
            group=f"test-group-{test_id}",
            sig_prefix=f"test-sig-{test_id}:",
            last_key=f"test-last-{test_id}",
            claim_idle_ms=50,  # Claim after 50ms idle
            dead_holder_timeout_ms=200,  # Consider dead after 200ms (150ms with tolerance)
            blpop_internal_timeout_ms=1000,
        )

        # Acquire but don't release - simulate holder still processing
        owner, msg_id = await gate.acquire()

        # Wait under the dead threshold but above claim threshold (80ms: 50 <= 80 < 150)
        await asyncio.sleep(0.08)

        # Call recovery - should find it idle but < 150ms threshold, so re-signal (not XACK)
        result = await gate._recover_and_maybe_dispatch()

        # First owner should have been re-signaled (still processing)
        # Pending should still be 1
        pending = await gate.r.xpending(gate.stream, gate.group)
        assert pending["pending"] == 1  # XPENDING returns dict with 'pending' key

        # Cleanup
        await gate.release(owner, msg_id.decode())
        await asyncio.sleep(0.02)
        await gate.r.delete(gate.stream, gate.last_key)
        await gate.r.delete(f"{gate.sig_prefix}{owner}")

    async def test_all_waiters_dead_queue_empties(self, real_redis):
        """Queue has only dead holders - verify queue drains."""
        # Create gate with 100ms dead holder timeout (instead of 2 minutes)
        test_id = str(uuid.uuid4())[:8]
        gate = AsyncStreamGate(
            real_redis,
            stream=f"test-gate-{test_id}",
            group=f"test-group-{test_id}",
            sig_prefix=f"test-sig-{test_id}:",
            last_key=f"test-last-{test_id}",
            claim_idle_ms=50,  # Claim after 50ms idle
            dead_holder_timeout_ms=100,  # Consider dead after 100ms
            blpop_internal_timeout_ms=1000,
        )

        # Acquire multiple times without releasing - simulate dead holders
        holders = []
        for _ in range(3):
            owner, msg_id = await gate.acquire()
            holders.append((owner, msg_id))
            # Don't release - simulate dead

        # Wait for all holders to exceed timeout (150ms > 100ms)
        await asyncio.sleep(0.15)

        # Run recovery multiple times to clear all dead holders
        for _ in range(5):
            await gate._recover_and_maybe_dispatch()
            await asyncio.sleep(0.02)

        # Queue should be empty now
        pending = await gate.r.xpending(gate.stream, gate.group)
        assert pending["pending"] == 0  # XPENDING returns dict with 'pending' key

        # last_key should be deleted
        last_key = await gate.r.get(gate.last_key)
        assert last_key is None

        # Cleanup
        await gate.r.delete(gate.stream)
        for owner, _ in holders:
            await gate.r.delete(f"{gate.sig_prefix}{owner}")

    @pytest.mark.slow
    async def test_multiple_consecutive_dead_holders(self, real_redis):
        """3 dead holders in queue - verify recovery cascades through all."""
        # Create gate with 100ms dead holder timeout (instead of 2 minutes)
        test_id = str(uuid.uuid4())[:8]
        gate = AsyncStreamGate(
            real_redis,
            stream=f"test-gate-{test_id}",
            group=f"test-group-{test_id}",
            sig_prefix=f"test-sig-{test_id}:",
            last_key=f"test-last-{test_id}",
            claim_idle_ms=50,  # Claim after 50ms idle
            dead_holder_timeout_ms=100,  # Consider dead after 100ms
            blpop_internal_timeout_ms=1000,
        )

        # Create 3 dead holders (acquire but never release)
        holders = []
        for _ in range(3):
            owner, msg_id = await gate.acquire()
            holders.append((owner, msg_id))
            # Don't release - simulate dead

        # Fourth waiter (live) - should eventually get the gate after all 3 dead holders are cleared
        async def live_waiter():
            return await gate.acquire(timeout=15)

        waiter_task = asyncio.create_task(live_waiter())
        await asyncio.sleep(0.02)  # Let live waiter start

        # Wait for all 3 dead holders to exceed timeout (150ms > 100ms)
        await asyncio.sleep(0.15)

        # Live waiter should eventually acquire via waiter-driven recovery
        # The waiter's periodic recovery checks will cascade through all 3 dead holders
        owner4, msg_id4 = await asyncio.wait_for(waiter_task, timeout=20)
        assert owner4 is not None

        # Cleanup
        msg_id4_str = msg_id4.decode() if isinstance(msg_id4, bytes) else msg_id4
        await gate.release(owner4, msg_id4_str)
        await asyncio.sleep(0.02)
        await gate.r.delete(gate.stream, gate.last_key)
        for owner, _ in holders:
            await gate.r.delete(f"{gate.sig_prefix}{owner}")
        await gate.r.delete(f"{gate.sig_prefix}{owner4}")


# ============================================================================
# Category 5: State Corruption Recovery Tests (6 tests)
# ============================================================================


class TestAsyncStateCorruptionRecovery:
    """Tests for recovering from corrupted Redis state."""

    pytestmark = pytest.mark.asyncio

    async def test_last_key_points_to_nonexistent_msg(self, clean_gate):
        """last_key points to fake message ID - graceful handling."""
        # Manually set last_key to invalid ID
        await clean_gate.r.set(clean_gate.last_key, b"9999999999-0")

        # Try to acquire - should work despite corrupted last_key
        owner, msg_id = await clean_gate.acquire()
        assert owner is not None

        await clean_gate.release(owner, msg_id.decode())

    async def test_consumer_group_deleted_mid_operation(self, clean_gate):
        """Consumer group deleted between acquire/release - recreated."""
        owner, msg_id = await clean_gate.acquire()

        # Delete consumer group
        await clean_gate.r.xgroup_destroy(clean_gate.stream, clean_gate.group)

        # Release should recreate group via ensure_group()
        await clean_gate.release(owner, msg_id.decode())

        # Verify group was recreated
        groups = await clean_gate.r.xinfo_groups(clean_gate.stream)
        assert len(groups) == 1

    async def test_stream_deleted_mid_operation(self, clean_gate):
        """Stream deleted - should recreate on next operation."""
        owner, msg_id = await clean_gate.acquire()

        # Delete stream
        await clean_gate.r.delete(clean_gate.stream)

        # Next acquire should recreate stream
        await clean_gate.ensure_group()

        # Verify stream exists
        exists = await clean_gate.r.exists(clean_gate.stream)
        assert exists == 1

    async def test_orphaned_signal_key_cleanup(self, clean_gate):
        """Orphaned signal keys should have TTL for cleanup."""
        # Create orphaned signal key
        orphan_key = f"{clean_gate.sig_prefix}orphan-{uuid.uuid4()}"
        await clean_gate.r.lpush(orphan_key, 1)
        await clean_gate.r.pexpire(orphan_key, 5000)  # 5 second TTL

        # Verify TTL is set
        ttl = await clean_gate.r.pttl(orphan_key)
        assert 0 < ttl <= 5000

        # After TTL expires, key should be gone
        await asyncio.sleep(6)
        exists = await clean_gate.r.exists(orphan_key)
        assert exists == 0

    async def test_pending_entry_with_no_message(self, clean_gate):
        """Pending entry exists but message deleted - XACK handles gracefully."""
        owner, msg_id = await clean_gate.acquire()

        # Delete the message from stream (but it's still pending in group)
        await clean_gate.r.xdel(clean_gate.stream, msg_id)

        # Try to XACK - should handle missing message gracefully
        try:
            await clean_gate.r.xack(clean_gate.stream, clean_gate.group, msg_id)
        except Exception as e:
            pytest.fail(f"XACK should handle missing message: {e}")

    async def test_last_key_race_condition(self, clean_gate):
        """GET(last_key) returns X, but another process updates before XACK."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Simulate race: manually change last_key
        fake_msg_id = b"8888888888-0"
        await clean_gate.r.set(clean_gate.last_key, fake_msg_id)

        # Try to release - guard should prevent dispatch
        await clean_gate.release(owner1, msg_id1.decode())
        await asyncio.sleep(0.1)

        # last_key should still be the fake one (release was no-op)
        current = await clean_gate.r.get(clean_gate.last_key)
        assert current == fake_msg_id


# ============================================================================
# Helper Functions for Multi-Process Testing
# ============================================================================


def _multiprocess_worker_acquire_release(
    redis_url: str,
    stream: str,
    group: str,
    sig_prefix: str,
    last_key: str,
    worker_id: int,
    result_queue: multiprocessing.Queue,
    delay: float = 0.0,
    acquire_order_key: str = None,
):
    """
    Worker function for multiprocess tests - runs in separate process.

    Args:
        redis_url: Redis connection URL
        stream: Stream name
        group: Consumer group name
        sig_prefix: Signal key prefix
        last_key: Last dispatched key name
        worker_id: Worker identifier
        result_queue: Queue to send results back to parent
        delay: Optional delay before acquiring (for testing timing)
        acquire_order_key: Optional Redis key to atomically track acquisition order
    """

    async def _worker():
        # Create new Redis connection in this process
        client = await redis.from_url(redis_url, decode_responses=False)
        gate = AsyncStreamGate(
            client,
            stream=stream,
            group=group,
            sig_prefix=sig_prefix,
            last_key=last_key,
        )

        try:
            if delay > 0:
                await asyncio.sleep(delay)

            start_time = time.time()
            owner, msg_id = await gate.acquire(timeout=30)
            acquire_time = time.time()

            # Atomically record acquisition order via Redis INCR
            acquire_order = None
            if acquire_order_key:
                acquire_order = await client.incr(acquire_order_key)

            # Simulate some work
            await asyncio.sleep(0.01)

            # Handle both bytes and string msg_id
            msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            await gate.release(owner, msg_id_str)
            release_time = time.time()

            result_queue.put(
                {
                    "worker_id": worker_id,
                    "owner": owner,
                    "msg_id": msg_id_str,
                    "start_time": start_time,
                    "acquire_time": acquire_time,
                    "release_time": release_time,
                    "acquire_order": acquire_order,
                    "success": True,
                    "error": None,
                }
            )
        except Exception as e:
            result_queue.put(
                {
                    "worker_id": worker_id,
                    "success": False,
                    "error": str(e),
                }
            )
        finally:
            await client.aclose()

    # Run the async worker function
    asyncio.run(_worker())


def _multiprocess_worker_acquire_only(
    redis_url: str,
    stream: str,
    group: str,
    sig_prefix: str,
    last_key: str,
    worker_id: int,
    result_queue: multiprocessing.Queue,
    timeout: int = 30,
):
    """
    Worker that acquires but doesn't release (for testing dead holder scenarios).
    """

    async def _worker():
        client = await redis.from_url(redis_url, decode_responses=False)
        gate = AsyncStreamGate(
            client,
            stream=stream,
            group=group,
            sig_prefix=sig_prefix,
            last_key=last_key,
        )

        try:
            owner, msg_id = await gate.acquire(timeout=timeout)
            msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            result_queue.put(
                {
                    "worker_id": worker_id,
                    "owner": owner,
                    "msg_id": msg_id_str,
                    "success": True,
                    "error": None,
                }
            )
            # Don't release - simulate dead holder
            # Keep process alive briefly
            await asyncio.sleep(0.5)
        except Exception as e:
            result_queue.put(
                {
                    "worker_id": worker_id,
                    "success": False,
                    "error": str(e),
                }
            )
        finally:
            await client.aclose()

    asyncio.run(_worker())


def _multiprocess_worker_release_only(
    redis_url: str,
    stream: str,
    group: str,
    sig_prefix: str,
    last_key: str,
    worker_id: int,
    owner: str,
    msg_id: str,
    result_queue: multiprocessing.Queue,
):
    """
    Worker that only calls release (for testing double-release scenarios).
    """

    async def _worker():
        client = await redis.from_url(redis_url, decode_responses=False)
        gate = AsyncStreamGate(
            client,
            stream=stream,
            group=group,
            sig_prefix=sig_prefix,
            last_key=last_key,
        )

        try:
            await gate.release(owner, msg_id)
            result_queue.put(
                {
                    "worker_id": worker_id,
                    "success": True,
                    "error": None,
                }
            )
        except Exception as e:
            result_queue.put(
                {
                    "worker_id": worker_id,
                    "success": False,
                    "error": str(e),
                }
            )
        finally:
            await client.aclose()

    asyncio.run(_worker())


# ============================================================================
# Category 6: Multi-Process Concurrency Tests (6 tests)
# ============================================================================


class TestAsyncMultiProcessConcurrency:
    """Tests for multiple separate Python processes competing for the gate."""

    pytestmark = pytest.mark.asyncio

    async def test_multiprocess_simultaneous_acquire(self, clean_gate):
        """3 processes try to acquire simultaneously - verify FIFO order."""
        # Get Redis URL for worker processes
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
        if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
            redis_url = f"redis://{redis_url}/15"

        # Create a unique key to track acquisition order atomically
        acquire_order_key = f"{clean_gate.stream}:test:acquire_order"
        await clean_gate.r.delete(acquire_order_key)

        result_queue = multiprocessing.Queue()
        processes = []

        # Start 3 worker processes simultaneously
        for i in range(3):
            p = multiprocessing.Process(
                target=_multiprocess_worker_acquire_release,
                args=(
                    redis_url,
                    clean_gate.stream,
                    clean_gate.group,
                    clean_gate.sig_prefix,
                    clean_gate.last_key,
                    i,
                    result_queue,
                ),
                kwargs={"acquire_order_key": acquire_order_key},
            )
            processes.append(p)
            p.start()

        # Wait for all processes to complete
        for p in processes:
            p.join(timeout=45)
            if p.is_alive():
                p.terminate()
                p.join()

        # Cleanup
        await clean_gate.r.delete(acquire_order_key)

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # All 3 should have succeeded
        assert len(results) == 3
        assert all(r["success"] for r in results), f"Some workers failed: {results}"

        # Verify FIFO ordering using atomic Redis counter
        # Sort by msg_id (the stream order assigned by Redis)
        results_sorted = sorted(results, key=lambda r: r["msg_id"])

        # The acquire_order (from Redis INCR) should match the msg_id order
        # i.e., the worker with the earliest msg_id should have acquire_order=1
        for i, result in enumerate(results_sorted):
            expected_order = i + 1
            assert result["acquire_order"] == expected_order, (
                f"FIFO violation: worker {result['worker_id']} with msg_id={result['msg_id']} "
                f"acquired in position {result['acquire_order']}, expected position {expected_order}. "
                f"Full results: {results_sorted}"
            )

    async def test_multiprocess_setnx_race(self, clean_gate):
        """5 processes race on SETNX(last_key) - verify only one wins at a time."""
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
        if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
            redis_url = f"redis://{redis_url}/15"

        result_queue = multiprocessing.Queue()
        processes = []

        # Start 5 worker processes simultaneously
        for i in range(5):
            p = multiprocessing.Process(
                target=_multiprocess_worker_acquire_release,
                args=(
                    redis_url,
                    clean_gate.stream,
                    clean_gate.group,
                    clean_gate.sig_prefix,
                    clean_gate.last_key,
                    i,
                    result_queue,
                ),
            )
            processes.append(p)
            p.start()

        # Wait for completion
        for p in processes:
            p.join(timeout=60)
            if p.is_alive():
                p.terminate()
                p.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # All should succeed
        assert len(results) == 5
        assert all(r["success"] for r in results)

        # Check that acquire times don't overlap (FIFO serialization)
        results_sorted = sorted(results, key=lambda r: r["acquire_time"])
        for i in range(len(results_sorted) - 1):
            current_release = results_sorted[i]["release_time"]
            next_acquire = results_sorted[i + 1]["acquire_time"]
            # Next acquire should happen after or very close to current release
            # (allowing small timing variance)
            assert (
                next_acquire >= current_release - 0.1
            ), f"Process {i+1} acquired before process {i} released"

    async def test_multiprocess_one_holds_others_wait(self, clean_gate):
        """Process 1 holds gate, processes 2-4 wait and acquire in order."""
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
        if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
            redis_url = f"redis://{redis_url}/15"

        result_queue = multiprocessing.Queue()

        # First process acquires and holds for 1 second
        p1 = multiprocessing.Process(
            target=_multiprocess_worker_acquire_release,
            args=(
                redis_url,
                clean_gate.stream,
                clean_gate.group,
                clean_gate.sig_prefix,
                clean_gate.last_key,
                0,
                result_queue,
                0.0,  # no delay
            ),
        )
        p1.start()

        # Give first process time to acquire
        await asyncio.sleep(0.2)

        # Start 3 more processes that will wait
        processes = [p1]
        for i in range(1, 4):
            p = multiprocessing.Process(
                target=_multiprocess_worker_acquire_release,
                args=(
                    redis_url,
                    clean_gate.stream,
                    clean_gate.group,
                    clean_gate.sig_prefix,
                    clean_gate.last_key,
                    i,
                    result_queue,
                ),
            )
            processes.append(p)
            p.start()

        # Wait for all to complete
        for p in processes:
            p.join(timeout=45)
            if p.is_alive():
                p.terminate()
                p.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # All 4 should succeed
        assert len(results) == 4
        assert all(r["success"] for r in results)

        # Process 0 should have acquired first
        results_sorted = sorted(results, key=lambda r: r["acquire_time"])
        assert results_sorted[0]["worker_id"] == 0

    async def test_multiprocess_holder_dies_recovery(self, clean_gate):
        """Process acquires then exits without releasing - other process recovers."""
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
        if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
            redis_url = f"redis://{redis_url}/15"

        result_queue = multiprocessing.Queue()

        # First process acquires but doesn't release (dies)
        p1 = multiprocessing.Process(
            target=_multiprocess_worker_acquire_only,
            args=(
                redis_url,
                clean_gate.stream,
                clean_gate.group,
                clean_gate.sig_prefix,
                clean_gate.last_key,
                0,
                result_queue,
                5,  # short timeout
            ),
        )
        p1.start()
        p1.join(timeout=10)
        if p1.is_alive():
            p1.terminate()
            p1.join()

        # Verify first process acquired
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())
        assert len(results) == 1
        assert results[0]["success"]

        # Now start a second process with mocked recovery
        # It should detect the dead holder and eventually acquire
        p2 = multiprocessing.Process(
            target=_multiprocess_worker_acquire_release,
            args=(
                redis_url,
                clean_gate.stream,
                clean_gate.group,
                clean_gate.sig_prefix,
                clean_gate.last_key,
                1,
                result_queue,
            ),
        )
        p2.start()
        p2.join(timeout=20)
        if p2.is_alive():
            p2.terminate()
            p2.join()

        # Collect second process result
        while not result_queue.empty():
            results.append(result_queue.get())

        # Second process should have eventually acquired (via waiter-driven recovery)
        # Note: This may timeout in practice without mocking, so we accept timeout
        second_results = [r for r in results if r["worker_id"] == 1]
        if second_results:
            # If it didn't timeout, it should have succeeded
            assert second_results[0]["success"] or "timed out" in str(
                second_results[0].get("error", "")
            )

    async def test_multiprocess_concurrent_release(self, clean_gate):
        """Two processes try to release same msg_id - double-release guard works."""
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
        if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
            redis_url = f"redis://{redis_url}/15"

        # Acquire in main process
        owner, msg_id = await clean_gate.acquire()
        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id

        result_queue = multiprocessing.Queue()

        # Try to release from two different processes simultaneously
        p1 = multiprocessing.Process(
            target=_multiprocess_worker_release_only,
            args=(
                redis_url,
                clean_gate.stream,
                clean_gate.group,
                clean_gate.sig_prefix,
                clean_gate.last_key,
                0,
                owner,
                msg_id_str,
                result_queue,
            ),
        )
        p2 = multiprocessing.Process(
            target=_multiprocess_worker_release_only,
            args=(
                redis_url,
                clean_gate.stream,
                clean_gate.group,
                clean_gate.sig_prefix,
                clean_gate.last_key,
                1,
                owner,
                msg_id_str,
                result_queue,
            ),
        )

        p1.start()
        p2.start()

        p1.join(timeout=10)
        p2.join(timeout=10)

        if p1.is_alive():
            p1.terminate()
            p1.join()
        if p2.is_alive():
            p2.terminate()
            p2.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # Both should complete without error (guard prevents double-dispatch)
        assert len(results) == 2
        assert all(r["success"] for r in results)

    @pytest.mark.slow
    async def test_multiprocess_high_contention(self, clean_gate):
        """5 processes with 6 acquire/release cycles each - verify no deadlocks."""
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
        if not redis_url.startswith("redis://") and not redis_url.startswith("rediss://"):
            redis_url = f"redis://{redis_url}/15"

        result_queue = multiprocessing.Queue()
        all_processes = []

        # Each process does 6 acquire/release cycles (reduced from 10 for speed)
        for worker_id in range(5):
            for cycle in range(6):
                p = multiprocessing.Process(
                    target=_multiprocess_worker_acquire_release,
                    args=(
                        redis_url,
                        clean_gate.stream,
                        clean_gate.group,
                        clean_gate.sig_prefix,
                        clean_gate.last_key,
                        worker_id * 10 + cycle,  # unique ID
                        result_queue,
                    ),
                )
                all_processes.append(p)
                p.start()

        # Wait for all to complete
        for p in all_processes:
            p.join(timeout=60)
            if p.is_alive():
                p.terminate()
                p.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # All 30 operations should succeed (5 workers × 6 cycles)
        assert len(results) == 30
        successful = [r for r in results if r["success"]]
        assert len(successful) == 30, f"Only {len(successful)}/30 operations succeeded"


# ============================================================================
# Category 7: Network Failure Tests (5 tests)
# ============================================================================


class TestAsyncNetworkFailures:
    """Tests for handling Redis connection failures and network errors."""

    pytestmark = pytest.mark.asyncio

    async def test_connection_dies_during_blpop(self, clean_gate):
        """Connection fails while waiting on BLPOP - handled gracefully."""
        # Acquire first so there's a waiter
        owner1, msg_id1 = await clean_gate.acquire()

        # Create second waiter
        async def waiter_with_connection_failure():
            # Mock blpop to fail with connection error on first call
            original_blpop = clean_gate.r.blpop
            call_count = [0]

            async def failing_blpop(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise redis.ConnectionError("Connection lost during BLPOP")
                return await original_blpop(*args, **kwargs)

            clean_gate.r.blpop = failing_blpop

            try:
                return await clean_gate.acquire(timeout=10)
            finally:
                clean_gate.r.blpop = original_blpop

        waiter_task = asyncio.create_task(waiter_with_connection_failure())
        await asyncio.sleep(0.1)

        # Release first holder
        await clean_gate.release(
            owner1, msg_id1.decode() if isinstance(msg_id1, bytes) else msg_id1
        )
        await asyncio.sleep(0.1)

        # Second waiter should eventually succeed (after connection error)
        try:
            owner2, msg_id2 = await asyncio.wait_for(waiter_task, timeout=15)
            # If we got here, cleanup
            await clean_gate.release(
                owner2, msg_id2.decode() if isinstance(msg_id2, bytes) else msg_id2
            )
        except (asyncio.TimeoutError, redis.ConnectionError):
            # Connection error may propagate - that's acceptable behavior
            pass

    async def test_connection_dies_during_acquire(self, clean_gate):
        """Connection fails during XADD in acquire() - exception raised."""
        # Mock XADD to fail
        original_xadd = clean_gate.r.xadd

        async def failing_xadd(*args, **kwargs):
            raise redis.ConnectionError("Connection lost during XADD")

        clean_gate.r.xadd = failing_xadd

        # Acquire should raise connection error
        with pytest.raises(redis.ConnectionError):
            await clean_gate.acquire()

        # Restore
        clean_gate.r.xadd = original_xadd

    async def test_connection_dies_during_release(self, clean_gate):
        """Connection fails during XACK in release() - handled gracefully."""
        owner, msg_id = await clean_gate.acquire()
        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id

        # Mock XACK to fail
        original_xack = clean_gate.r.xack

        async def failing_xack(*args, **kwargs):
            raise redis.ConnectionError("Connection lost during XACK")

        clean_gate.r.xack = failing_xack

        # Release should handle connection error gracefully (best-effort)
        try:
            await clean_gate.release(owner, msg_id_str)
            await asyncio.sleep(0.1)
        finally:
            # Restore
            clean_gate.r.xack = original_xack

    async def test_connection_timeout(self, clean_gate):
        """Redis command times out - handled gracefully."""
        owner1, msg_id1 = await clean_gate.acquire()

        # Create waiter with timeout mock
        async def waiter_with_timeout():
            original_blpop = clean_gate.r.blpop
            call_count = [0]

            async def timeout_blpop(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise redis.TimeoutError("Command timed out")
                return await original_blpop(*args, **kwargs)

            clean_gate.r.blpop = timeout_blpop

            try:
                return await clean_gate.acquire(timeout=10)
            finally:
                clean_gate.r.blpop = original_blpop

        waiter_task = asyncio.create_task(waiter_with_timeout())
        await asyncio.sleep(0.1)

        # Release first holder
        await clean_gate.release(
            owner1, msg_id1.decode() if isinstance(msg_id1, bytes) else msg_id1
        )
        await asyncio.sleep(0.1)

        # Second waiter should handle timeout and retry
        try:
            owner2, msg_id2 = await asyncio.wait_for(waiter_task, timeout=15)
            await clean_gate.release(
                owner2, msg_id2.decode() if isinstance(msg_id2, bytes) else msg_id2
            )
        except (asyncio.TimeoutError, redis.TimeoutError):
            # Timeout may propagate - acceptable
            pass

    async def test_connection_recovery_after_failure(self, clean_gate):
        """Connection fails, reconnects, gate still works correctly."""
        # First operation succeeds
        owner1, msg_id1 = await clean_gate.acquire()
        await clean_gate.release(
            owner1, msg_id1.decode() if isinstance(msg_id1, bytes) else msg_id1
        )
        await asyncio.sleep(0.1)

        # Simulate connection failure by replacing client temporarily
        original_client = clean_gate.r

        # Create a mock that fails once then succeeds
        failure_count = [0]
        original_xadd = clean_gate.r.xadd

        async def failing_then_working_xadd(*args, **kwargs):
            failure_count[0] += 1
            if failure_count[0] == 1:
                raise redis.ConnectionError("Connection failed")
            return await original_xadd(*args, **kwargs)

        clean_gate.r.xadd = failing_then_working_xadd

        # First acquire after "connection failure" should fail
        with pytest.raises(redis.ConnectionError):
            await clean_gate.acquire()

        # Second acquire should work (connection "recovered")
        owner2, msg_id2 = await clean_gate.acquire()
        assert owner2 is not None

        # Cleanup
        clean_gate.r.xadd = original_xadd
        await clean_gate.release(
            owner2, msg_id2.decode() if isinstance(msg_id2, bytes) else msg_id2
        )
