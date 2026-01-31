"""
Tests for common utilities.
"""

import os

import pytest

from redis_fifo_lock.common import (
    DEFAULT_CLAIM_IDLE_MS,
    DEFAULT_GROUP,
    DEFAULT_LAST_KEY,
    DEFAULT_SIG_PREFIX,
    DEFAULT_SIG_TTL_MS,
    DEFAULT_STREAM,
    get_advancer_consumer,
)


class TestConstants:
    """Tests for constant values."""

    def test_default_values(self):
        """Test that default constants have expected values."""
        assert DEFAULT_STREAM == "gate:stream"
        assert DEFAULT_GROUP == "gate:group"
        assert DEFAULT_SIG_PREFIX == "gate:sig:"
        assert DEFAULT_SIG_TTL_MS == 5 * 60 * 1000
        assert DEFAULT_CLAIM_IDLE_MS == 60_000
        assert DEFAULT_LAST_KEY == "gate:last-dispatched"


class TestGetAdvancerConsumer:
    """Tests for get_advancer_consumer function."""

    def test_with_default_pid(self):
        """Test get_advancer_consumer with default PID."""
        consumer = get_advancer_consumer()
        assert consumer.startswith("advancer:")
        assert str(os.getpid()) in consumer

    def test_with_custom_pid(self):
        """Test get_advancer_consumer with custom PID."""
        consumer = get_advancer_consumer(pid=12345)
        assert consumer == "advancer:12345"
