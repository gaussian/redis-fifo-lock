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
    async def test_ensure_group_creates_group(
        self, async_stream_gate, mock_async_redis
    ):
        """Test that ensure_group creates the consumer group."""
        await async_stream_gate.ensure_group()
        mock_async_redis.xgroup_create.assert_called_once_with(
            "gate:stream", "gate:group", id="$", mkstream=True
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

        # Verify blpop was called with timeout
        call_args = mock_async_redis.blpop.call_args
        assert call_args[1]["timeout"] == 30

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
    async def test_release_with_pending_entry(
        self, async_stream_gate, mock_async_redis
    ):
        """Test release dispatches the next entry."""
        mock_async_redis.get.return_value = b"1234567890-0"
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = [
            (
                "gate:stream",
                [(b"1234567891-0", {b"owner": b"next-owner"})],
            )
        ]

        await async_stream_gate.release("owner", "msg_id")

        # Verify previous message was acked (note: last.decode() converts bytes to str)
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
        mock_async_redis.get.return_value = None
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = []

        await async_stream_gate.release("owner", "msg_id")

        # Verify last key was deleted
        mock_async_redis.delete.assert_called_with("gate:last-dispatched")

    @pytest.mark.asyncio
    async def test_release_with_crash_recovery(
        self, async_stream_gate, mock_async_redis
    ):
        """Test release performs crash recovery."""
        mock_async_redis.get.return_value = None
        mock_async_redis.xautoclaim.return_value = (
            "0-0",
            [(b"stuck-id", {b"owner": b"stuck-owner"})],
        )

        await async_stream_gate.release("owner", "msg_id")

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
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_async_redis.get.return_value = None
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = []

        async with await async_stream_gate.session() as session:
            assert session.owner is not None
            assert session.msg_id is not None

        # Verify release was called
        assert mock_async_redis.xreadgroup.called

    @pytest.mark.asyncio
    async def test_session_with_timeout(self, async_stream_gate, mock_async_redis):
        """Test session context manager with timeout."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_async_redis.get.return_value = None
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = []

        async with await async_stream_gate.session(timeout=30):
            pass

        # Verify acquire was called with timeout
        call_args = mock_async_redis.blpop.call_args
        assert call_args[1]["timeout"] == 30

    @pytest.mark.asyncio
    async def test_session_with_exception(self, async_stream_gate, mock_async_redis):
        """Test session context manager releases even on exception."""
        mock_async_redis.xadd.return_value = b"1234567890-0"
        mock_async_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_async_redis.get.return_value = None
        mock_async_redis.xautoclaim.return_value = ("0-0", [])
        mock_async_redis.xreadgroup.return_value = []

        with pytest.raises(ValueError):
            async with await async_stream_gate.session():
                raise ValueError("Test error")

        # Verify release was called despite exception
        assert mock_async_redis.xreadgroup.called
