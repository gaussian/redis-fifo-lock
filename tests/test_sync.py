"""
Tests for synchronous Redis Stream gate.
"""

import threading
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
import redis

from redis_fifo_lock.sync import StreamGate


@pytest.fixture
def mock_redis():
    """Create a mock Redis client."""
    return MagicMock(spec=redis.Redis)


@pytest.fixture
def stream_gate(mock_redis):
    """Create a StreamGate instance with mock Redis."""
    return StreamGate(mock_redis)


class TestStreamGateInit:
    """Tests for StreamGate initialization."""

    def test_init_with_defaults(self, mock_redis):
        """Test initialization with default parameters."""
        gate = StreamGate(mock_redis)
        assert gate.r is mock_redis
        assert gate.stream == "gate:stream"
        assert gate.group == "gate:group"
        assert gate.sig_prefix == "gate:sig:"
        assert gate.sig_ttl_ms == 5 * 60 * 1000
        assert gate.claim_idle_ms == 60_000
        assert gate.last_key == "gate:last-dispatched"
        assert gate.adv_consumer.startswith("advancer:")

    def test_init_with_custom_params(self, mock_redis):
        """Test initialization with custom parameters."""
        gate = StreamGate(
            mock_redis,
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


class TestStreamGateEnsureGroup:
    """Tests for ensure_group method."""

    def test_ensure_group_creates_group(self, stream_gate, mock_redis):
        """Test that ensure_group creates the consumer group."""
        stream_gate.ensure_group()
        mock_redis.xgroup_create.assert_called_once_with(
            "gate:stream", "gate:group", id="$", mkstream=True
        )

    def test_ensure_group_handles_busygroup(self, stream_gate, mock_redis):
        """Test that ensure_group handles BUSYGROUP error gracefully."""
        mock_redis.xgroup_create.side_effect = redis.ResponseError(
            "BUSYGROUP Consumer Group name already exists"
        )
        stream_gate.ensure_group()  # Should not raise

    def test_ensure_group_raises_other_errors(self, stream_gate, mock_redis):
        """Test that ensure_group raises non-BUSYGROUP errors."""
        mock_redis.xgroup_create.side_effect = redis.ResponseError("Some other error")
        with pytest.raises(redis.ResponseError, match="Some other error"):
            stream_gate.ensure_group()


class TestStreamGateAcquire:
    """Tests for acquire method."""

    def test_acquire_success(self, stream_gate, mock_redis):
        """Test successful acquire."""
        mock_redis.xadd.return_value = b"1234567890-0"
        mock_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")

        owner, msg_id = stream_gate.acquire()

        # Verify ensure_group was called
        assert mock_redis.xgroup_create.called

        # Verify xadd was called with owner
        assert mock_redis.xadd.called
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "gate:stream"
        assert "owner" in call_args[0][1]

        # Verify blpop was called
        assert mock_redis.blpop.called

        assert isinstance(owner, str)
        assert msg_id == b"1234567890-0"

    def test_acquire_with_timeout(self, stream_gate, mock_redis):
        """Test acquire with timeout parameter."""
        mock_redis.xadd.return_value = b"1234567890-0"
        mock_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")

        stream_gate.acquire(timeout=30)

        # Verify blpop was called with timeout
        call_args = mock_redis.blpop.call_args
        assert call_args[1]["timeout"] == 30

    def test_acquire_timeout_reached(self, stream_gate, mock_redis):
        """Test acquire when timeout is reached."""
        mock_redis.xadd.return_value = b"1234567890-0"
        mock_redis.blpop.return_value = None  # Timeout

        with pytest.raises(TimeoutError, match="acquire timed out"):
            stream_gate.acquire(timeout=1)

        # Verify cleanup was attempted
        assert mock_redis.xdel.called
        assert mock_redis.delete.called


class TestStreamGateRelease:
    """Tests for release method."""

    def test_release_with_pending_entry(self, stream_gate, mock_redis):
        """Test release dispatches the next entry."""
        mock_redis.get.return_value = b"1234567890-0"
        mock_redis.xautoclaim.return_value = ("0-0", [])
        mock_redis.xreadgroup.return_value = [
            (
                "gate:stream",
                [(b"1234567891-0", {b"owner": b"next-owner"})],
            )
        ]

        stream_gate.release("owner", "msg_id")

        # Verify previous message was acked (note: last.decode() converts bytes to str)
        mock_redis.xack.assert_called_once_with(
            "gate:stream", "gate:group", "1234567890-0"
        )

        # Verify next entry was dispatched
        assert mock_redis.lpush.called
        assert mock_redis.pexpire.called
        assert mock_redis.set.called

    def test_release_empty_queue(self, stream_gate, mock_redis):
        """Test release when queue is empty."""
        mock_redis.get.return_value = None
        mock_redis.xautoclaim.return_value = ("0-0", [])
        mock_redis.xreadgroup.return_value = []

        stream_gate.release("owner", "msg_id")

        # Verify last key was deleted
        mock_redis.delete.assert_called_with("gate:last-dispatched")

    def test_release_with_crash_recovery(self, stream_gate, mock_redis):
        """Test release performs crash recovery."""
        mock_redis.get.return_value = None
        mock_redis.xautoclaim.return_value = (
            "0-0",
            [(b"stuck-id", {b"owner": b"stuck-owner"})],
        )

        stream_gate.release("owner", "msg_id")

        # Verify stuck entry was re-signaled
        lpush_call = mock_redis.lpush.call_args
        assert lpush_call[0][0] == "gate:sig:stuck-owner"

        # Verify last key was updated
        set_call = mock_redis.set.call_args
        assert set_call[0] == ("gate:last-dispatched", b"stuck-id")


class TestStreamGateCancel:
    """Tests for cancel method."""

    def test_cancel(self, stream_gate, mock_redis):
        """Test cancel removes ticket and signal."""
        stream_gate.cancel("test-owner", "1234567890-0")

        mock_redis.xdel.assert_called_once_with("gate:stream", "1234567890-0")
        mock_redis.delete.assert_called_once_with("gate:sig:test-owner")


class TestStreamGateContextManager:
    """Tests for context manager interface."""

    def test_context_manager_success(self, stream_gate, mock_redis):
        """Test context manager acquire and release."""
        mock_redis.xadd.return_value = b"1234567890-0"
        mock_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_redis.get.return_value = None
        mock_redis.xautoclaim.return_value = ("0-0", [])
        mock_redis.xreadgroup.return_value = []

        with stream_gate as gate:
            assert gate.owner is not None
            assert gate.msg_id is not None

        # Verify release was called
        assert mock_redis.xreadgroup.called

    def test_context_manager_with_exception(self, stream_gate, mock_redis):
        """Test context manager releases even on exception."""
        mock_redis.xadd.return_value = b"1234567890-0"
        mock_redis.blpop.return_value = (b"gate:sig:test-uuid", b"1")
        mock_redis.get.return_value = None
        mock_redis.xautoclaim.return_value = ("0-0", [])
        mock_redis.xreadgroup.return_value = []

        with pytest.raises(ValueError):
            with stream_gate:
                raise ValueError("Test error")

        # Verify release was called despite exception
        assert mock_redis.xreadgroup.called
