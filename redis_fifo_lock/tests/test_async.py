"""
Tests for asynchronous Redis Stream gate.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio as redis

from neutron.redis_stream.async_gate import AsyncStreamGate


@pytest.fixture
def mock_async_redis():
    """Create a mock async Redis client."""
    mock = MagicMock(spec=redis.Redis)
    # Make all methods async
    mock.xgroup_create = AsyncMock()
    mock.xadd = AsyncMock()
    mock.blpop = AsyncMock()
    mock.xdel = AsyncMock()
    mock.delete = AsyncMock()
    mock.get = AsyncMock()
    mock.xack = AsyncMock()
    mock.xautoclaim = AsyncMock()
    mock.xreadgroup = AsyncMock()
    mock.xpending_range = AsyncMock()
    mock.lpush = AsyncMock()
    mock.pexpire = AsyncMock()
    mock.set = AsyncMock()
    return mock


@pytest.fixture
def async_stream_gate(mock_async_redis):
    """Create an AsyncStreamGate instance with mock Redis."""
    return AsyncStreamGate(mock_async_redis)


class TestAsyncStreamGateInit:
    """Tests for AsyncStreamGate initialization."""

    def test_init_with_defaults(self, mock_async_redis):
        """Test initialization with default parameters."""
        gate = AsyncStreamGate(mock_async_redis)
        assert gate.r is mock_async_redis
        assert gate.stream == "gate:stream"
        assert gate.group == "gate:group"
        assert gate.sig_prefix == "gate:sig:"
        assert gate.sig_ttl_ms == 5 * 60 * 1000
        assert gate.claim_idle_ms == 60_000
        assert gate.last_key == "gate:last-dispatched"
        assert gate.adv_consumer.startswith("advancer:")

    def test_init_with_custom_params(self, mock_async_redis):
        """Test initialization with custom parameters."""
        gate = AsyncStreamGate(
            mock_async_redis,
            stream="custom:stream",
            group="custom:group",
            adv_consumer="custom:consumer",
            sig_prefix="custom:sig:",
            sig_ttl_ms=120000,
            claim_idle_ms=30000,
            last_key="custom:last",
        )
        assert gate.stream == "custom:stream"
        assert gate.group == "custom:group"
        assert gate.adv_consumer == "custom:consumer"
        assert gate.sig_prefix == "custom:sig:"
        assert gate.sig_ttl_ms == 120000
        assert gate.claim_idle_ms == 30000
        assert gate.last_key == "custom:last"


class TestAsyncStreamGateEnsureGroup:
    """Tests for ensure_group method."""

    @pytest.mark.asyncio
    async def test_ensure_group_creates_group(self, async_stream_gate, mock_async_redis):
        """Test that ensure_group creates the consumer group."""
        await async_stream_gate.ensure_group()
        mock_async_redis.xgroup_create.assert_called_once_with(
            "gate:stream", "gate:group", id="0", mkstream=True
        )

    @pytest.mark.asyncio
    async def test_ensure_group_handles_busygroup(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that ensure_group handles BUSYGROUP error gracefully."""
        mock_async_redis.xgroup_create.side_effect = redis.ResponseError(
            "BUSYGROUP Consumer Group name already exists"
        )
        await async_stream_gate.ensure_group()  # Should not raise

    @pytest.mark.asyncio
    async def test_ensure_group_raises_other_errors(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that ensure_group raises non-BUSYGROUP errors."""
        mock_async_redis.xgroup_create.side_effect = redis.ResponseError(
            "Some other error"
        )
        with pytest.raises(redis.ResponseError, match="Some other error"):
            await async_stream_gate.ensure_group()


class TestAsyncStreamGateAcquire:
    """Tests for acquire method."""

    @pytest.mark.asyncio
    async def test_acquire_success(self, async_stream_gate, mock_async_redis):
        """Test successful acquire."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")

        owner, msg_id = await async_stream_gate.acquire()

        # Verify ensure_group was called
        assert mock_async_redis.xgroup_create.called

        # Verify xadd was called with owner
        assert mock_async_redis.xadd.called
        call_args = mock_async_redis.xadd.call_args
        assert call_args[0][0] == "gate:stream"
        assert "owner" in call_args[0][1]

        # Verify blpop was called
        assert mock_async_redis.blpop.called

        assert isinstance(owner, str)
        assert msg_id == b"1234567890-0"

    @pytest.mark.asyncio
    async def test_acquire_with_timeout(self, async_stream_gate, mock_async_redis):
        """Test acquire with timeout parameter."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")

        await async_stream_gate.acquire(timeout=30)

        # Verify blpop was called with internal timeout (5s max, or remaining time)
        call_args = mock_async_redis.blpop.call_args
        # Internal timeout is min(5, remaining_time), so should be 5 on first call
        assert call_args[1]["timeout"] == 5

    @pytest.mark.asyncio
    async def test_acquire_timeout_reached(self, async_stream_gate, mock_async_redis):
        """Test acquire when timeout is reached."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.blpop.return_value = None  # Timeout

        with pytest.raises(asyncio.TimeoutError, match="acquire timed out"):
            await async_stream_gate.acquire(timeout=1)

        # Verify cleanup was attempted
        assert mock_async_redis.xdel.called
        assert mock_async_redis.delete.called


class TestAsyncStreamGateRelease:
    """Tests for release method."""

    @pytest.mark.asyncio
    async def test_release_with_pending_entry(self, async_stream_gate, mock_async_redis):
        """Test release dispatches the next entry."""
        mock_async_redis.get.return_value = b"1234567890-0"
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = [
            (
                "gate:stream",
                [(b"1234567891-0", {b"owner": b"next-owner"})],
            )
        ]

        await async_stream_gate.release("owner", "1234567890-0")
        # Give event loop a chance to run the background dispatch task
        await asyncio.sleep(0)

        # Verify previous message was acked
        mock_async_redis.xack.assert_called_once_with(
            "gate:stream", "gate:group", "1234567890-0"
        )

        # Verify next entry was dispatched
        assert mock_async_redis.lpush.called
        assert mock_async_redis.pexpire.called
        assert mock_async_redis.set.called

    @pytest.mark.asyncio
    async def test_release_empty_queue(self, async_stream_gate, mock_async_redis):
        """Test release when queue is empty."""
        mock_async_redis.get.return_value = b"msg_id"  # last_key matches msg_id
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = []

        await async_stream_gate.release("owner", "msg_id")
        # Give event loop a chance to run the background dispatch task
        await asyncio.sleep(0)

        # Verify last key was deleted
        mock_async_redis.delete.assert_called_with("gate:last-dispatched")

    @pytest.mark.asyncio
    async def test_release_with_crash_recovery(self, async_stream_gate, mock_async_redis):
        """Test release performs crash recovery."""
        mock_async_redis.get.return_value = b"msg_id"  # last_key matches msg_id
        mock_async_redis.xpending_range.return_value = [
            {"time_since_delivered": 90000}  # 90 seconds, < 2 minutes
        ]
        mock_async_redis.xautoclaim.return_value = (
            "0-0",
            [(b"stuck-id", {b"owner": b"stuck-owner"})],
        )

        await async_stream_gate.release("owner", "msg_id")
        # Give event loop a chance to run the background dispatch task
        await asyncio.sleep(0)

        # Verify stuck entry was re-signaled
        lpush_call = mock_async_redis.lpush.call_args
        assert lpush_call[0][0] == "gate:sig:stuck-owner"

        # Verify last key was updated
        set_call = mock_async_redis.set.call_args
        assert set_call[0] == ("gate:last-dispatched", b"stuck-id")


class TestAsyncStreamGateCancel:
    """Tests for cancel method."""

    @pytest.mark.asyncio
    async def test_cancel(self, async_stream_gate, mock_async_redis):
        """Test cancel removes ticket and signal."""
        await async_stream_gate.cancel("test-owner", "1234567890-0")

        mock_async_redis.xdel.assert_called_once_with("gate:stream", "1234567890-0")
        mock_async_redis.delete.assert_called_once_with("gate:sig:test-owner")


class TestAsyncStreamGateSession:
    """Tests for session context manager."""

    @pytest.mark.asyncio
    async def test_session_success(self, async_stream_gate, mock_async_redis):
        """Test session context manager acquire and release."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.set.return_value = True  # First waiter gets lock
        mock_async_redis.xreadgroup.return_value = [
            ("gate:stream", [(b"1234567890-0", {b"owner": b"test-owner"})])
        ]
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_async_redis.get.return_value = b"1234567890-0"  # For release
        mock_async_redis.xautoclaim.return_value = ("0-0", [])

        async with await async_stream_gate.session() as session:
            assert session.owner is not None
            assert session.msg_id is not None

        # Verify release was called
        assert mock_async_redis.xreadgroup.called

    @pytest.mark.asyncio
    async def test_session_with_timeout(self, async_stream_gate, mock_async_redis):
        """Test session context manager with timeout."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.set.return_value = True  # First waiter gets lock
        mock_async_redis.xreadgroup.return_value = [
            ("gate:stream", [(b"1234567890-0", {b"owner": b"test-owner"})])
        ]
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_async_redis.get.return_value = b"1234567890-0"  # For release
        mock_async_redis.xautoclaim.return_value = ("0-0", [])

        async with await async_stream_gate.session(timeout=30):
            pass

        # Verify acquire was called with internal timeout (5s)
        call_args = mock_async_redis.blpop.call_args
        assert call_args[1]["timeout"] == 5

    @pytest.mark.asyncio
    async def test_session_with_exception(self, async_stream_gate, mock_async_redis):
        """Test session context manager releases even on exception."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.set.return_value = True  # First waiter gets lock
        mock_async_redis.xreadgroup.return_value = [
            ("gate:stream", [(b"1234567890-0", {b"owner": b"test-owner"})])
        ]
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_async_redis.get.return_value = b"1234567890-0"  # For release
        mock_async_redis.xautoclaim.return_value = ("0-0", [])

        with pytest.raises(ValueError):
            async with await async_stream_gate.session():
                raise ValueError("Test error")

        # Verify release was called despite exception
        assert mock_async_redis.xreadgroup.called


class TestAsyncStreamGateIssueFixesNormalOperation:
    """Tests for normal operation with multiple waiters (Issue A verification)."""

    @pytest.mark.asyncio
    async def test_multiple_waiters_sequential(self, async_stream_gate, mock_async_redis):
        """Test that multiple waiters are processed in FIFO order."""
        # First waiter
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.set.return_value = True  # First waiter gets lock
        mock_async_redis.xreadgroup.return_value = [
            ("gate:stream", [(b"1234567890-0", {b"owner": b"owner-1"})])
        ]
        mock_async_redis.blpop.return_value = (b"gate:sig:owner-1", b"1")

        owner1, msg_id1 = await async_stream_gate.acquire()
        assert owner1 is not None
        assert msg_id1 == b"1234567890-0"

        # Release first waiter and dispatch second
        mock_async_redis.get.return_value = b"1234567890-0"  # last_key matches
        mock_async_redis.xpending_range.return_value = []  # No pending entries
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = [
            ("gate:stream", [(b"1234567891-0", {b"owner": b"owner-2"})])
        ]

        # Decode msg_id1 to string for release (msg_id parameter expects string)
        await async_stream_gate.release(owner1, msg_id1.decode() if isinstance(msg_id1, bytes) else msg_id1)
        # Give event loop a chance to run the background dispatch task
        await asyncio.sleep(0)

        # Verify second waiter was signaled
        assert mock_async_redis.lpush.called
        lpush_calls = [call[0][0] for call in mock_async_redis.lpush.call_args_list]
        # Check if owner-2 was signaled (should be second lpush call after first waiter's self-signal)
        assert any("owner-2" in str(call) for call in lpush_calls), f"Expected 'owner-2' in lpush calls: {lpush_calls}"


class TestAsyncStreamGateIssueFixesWaiterDrivenRecovery:
    """Tests for waiter-driven crash recovery (Issue A fix)."""

    @pytest.mark.asyncio
    async def test_waiter_driven_recovery_on_timeout(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that waiters call recovery logic when BLPOP times out."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.set.return_value = False  # Not first waiter
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = []

        # Simulate BLPOP timeout twice, then signal
        mock_async_redis.blpop.side_effect = [
            None,  # First timeout (5s)
            None,  # Second timeout (5s)
            (b"gate:sig:test-owner", b"1"),  # Finally signaled
        ]

        owner, msg_id = await async_stream_gate.acquire()

        # Verify BLPOP was called multiple times (internal timeout loop)
        assert mock_async_redis.blpop.call_count == 3

        # Verify recovery was attempted on timeouts
        assert mock_async_redis.xautoclaim.call_count >= 2

    @pytest.mark.asyncio
    async def test_waiter_detects_dead_leader(self, async_stream_gate, mock_async_redis):
        """Test that waiting process can detect and advance past dead leader."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.set.return_value = False  # Not first waiter

        # First timeout: XAUTOCLAIM finds stuck entry (< 2 min idle)
        mock_async_redis.xpending_range.return_value = [
            {"time_since_delivered": 90000}  # 90 seconds, < 2 minutes
        ]
        mock_async_redis.xautoclaim.return_value = (
            "0-0",
            [(b"stuck-id", {b"owner": b"stuck-owner"})],
        )

        # Second timeout: no stuck entry, read new message
        mock_async_redis.blpop.side_effect = [
            None,  # Timeout, triggers recovery
            (b"gate:sig:test-owner", b"1"),  # Gets signaled
        ]

        owner, msg_id = await async_stream_gate.acquire()

        # Verify recovery logic was called
        assert mock_async_redis.xautoclaim.called
        assert mock_async_redis.xpending_range.called


class TestAsyncStreamGateIssueFixesDeadHolderTimeout:
    """Tests for dead holder timeout (Issue B fix)."""

    @pytest.mark.asyncio
    async def test_dead_holder_timeout_skips_entry(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that entries idle >= 2 minutes are XACKed and skipped."""
        mock_async_redis.get.return_value = b"previous-msg-id"

        # XAUTOCLAIM finds an entry idle for 2+ minutes
        mock_async_redis.xautoclaim.return_value = (
            "0-0",
            [(b"dead-holder-msg-id", {b"owner": b"dead-owner"})],
        )

        # XPENDING_RANGE shows it's been idle for 2+ minutes
        mock_async_redis.xpending_range.return_value = [
            {"time_since_delivered": 150000}  # 150 seconds = 2.5 minutes
        ]

        # After XACKing dead holder, XREADGROUP returns next waiter
        mock_async_redis.xreadgroup.return_value = [
            ("gate:stream", [(b"next-msg-id", {b"owner": b"next-owner"})])
        ]

        # Call _recover_and_maybe_dispatch
        result = await async_stream_gate._recover_and_maybe_dispatch()

        # Verify dead holder was XACKed
        xack_calls = mock_async_redis.xack.call_args_list
        assert any(
            "dead-holder-msg-id" in str(call)
            for call in xack_calls
        )

        # Verify next waiter was dispatched
        assert result is True
        assert mock_async_redis.lpush.called

    @pytest.mark.asyncio
    async def test_idle_holder_still_processing(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that entries idle < 2 minutes are re-signaled (still processing)."""
        mock_async_redis.get.return_value = b"previous-msg-id"

        # XAUTOCLAIM finds an idle entry
        mock_async_redis.xautoclaim.return_value = (
            "0-0",
            [(b"slow-holder-msg-id", {b"owner": b"slow-owner"})],
        )

        # XPENDING_RANGE shows it's only been idle for 90 seconds (< 2 minutes)
        mock_async_redis.xpending_range.return_value = [
            {"time_since_delivered": 90000}  # 90 seconds
        ]

        result = await async_stream_gate._recover_and_maybe_dispatch()

        # Verify slow holder was re-signaled (not skipped)
        assert result is True
        lpush_calls = [call[0][0] for call in mock_async_redis.lpush.call_args_list]
        assert any("slow-owner" in str(call) for call in lpush_calls)

        # Verify it was NOT XACKed (should keep processing)
        xack_calls = mock_async_redis.xack.call_args_list
        # Should not contain the slow holder's msg_id
        assert not any(
            "slow-holder-msg-id" in str(call) for call in xack_calls
        ), "Slow holder should not be XACKed"


class TestAsyncStreamGateIssueFixesDoubleReleaseProtection:
    """Tests for double-release protection (Issue C fix)."""

    @pytest.mark.asyncio
    async def test_double_release_prevented(self, async_stream_gate, mock_async_redis):
        """Test that double release does not break last_key."""
        msg_id = "1234567890-0"
        owner = "test-owner"

        # First release: last_key matches
        mock_async_redis.get.return_value = msg_id.encode()
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = [
            ("gate:stream", [(b"next-msg-id", {b"owner": b"next-owner"})])
        ]

        await async_stream_gate.release(owner, msg_id)

        # Verify first release worked
        first_release_xack_count = mock_async_redis.xack.call_count
        first_release_lpush_count = mock_async_redis.lpush.call_count

        # Second release: last_key no longer matches (already advanced)
        mock_async_redis.get.return_value = b"next-msg-id"  # Different!

        await async_stream_gate.release(owner, msg_id)

        # Verify second release did XACK (best-effort)
        assert mock_async_redis.xack.call_count > first_release_xack_count

        # Verify second release did NOT dispatch (no lpush)
        assert (
            mock_async_redis.lpush.call_count == first_release_lpush_count
        ), "Second release should not dispatch"

    @pytest.mark.asyncio
    async def test_wrong_session_release_prevented(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that release from wrong session doesn't dispatch."""
        # Current holder is "msg-1"
        mock_async_redis.get.return_value = b"msg-1"

        # Someone tries to release "msg-0" (wrong session)
        await async_stream_gate.release("old-owner", "msg-0")

        # Verify XACK was attempted (best-effort cleanup)
        assert mock_async_redis.xack.called

        # Verify NO dispatch occurred (no xautoclaim, no xreadgroup)
        assert not mock_async_redis.xautoclaim.called
        assert not mock_async_redis.xreadgroup.called

    @pytest.mark.asyncio
    async def test_release_after_last_key_cleared(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that release after last_key cleared doesn't break anything."""
        # last_key is None (queue empty)
        mock_async_redis.get.return_value = None

        await async_stream_gate.release("owner", "msg-id")

        # Verify XACK was attempted (best-effort)
        assert mock_async_redis.xack.called

        # Verify NO dispatch occurred
        assert not mock_async_redis.xautoclaim.called
        assert not mock_async_redis.xreadgroup.called
