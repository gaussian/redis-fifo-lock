"""
Fast unit tests: no Redis, no sleeping, no concurrency.

Correctness of the lock itself is the adversarial suite's job — it needs a real
server and real contention, and no amount of mocking substitutes for either.
What lives here is the pure logic around the edges, the parts that are cheap to
check and were each got wrong at least once.
"""

from unittest.mock import MagicMock

import pytest
import redis.asyncio as aredis

from redis_fifo_lock import FifoLock
from redis_fifo_lock.common import blpop_block_seconds, effective_socket_timeout


class TestBlockDuration:
    """A blocking wait must finish before the client's own read deadline."""

    def test_stays_below_the_socket_timeout(self):
        # 5s poll against redis-py 8's 5s default is a coin flip the client wins,
        # which is the bug that started all of this.
        assert blpop_block_seconds(5_000, 5.0, None) < 5.0

    def test_tracks_the_client_rather_than_a_fixed_margin(self):
        assert blpop_block_seconds(30_000, 1.0, None) < 1.0

    def test_unbounded_client_gets_the_configured_interval(self):
        assert blpop_block_seconds(30_000, None, None) == pytest.approx(30.0)

    def test_sub_second_intervals_are_not_rounded_up(self):
        # Integer division floored every sub-second config at 1s.
        assert blpop_block_seconds(250, None, None) == pytest.approx(0.25)

    def test_never_blocks_past_the_callers_deadline(self):
        # int(remaining) rounded 0.4s down to 0, then up to a full second,
        # overshooting a deadline that had already expired.
        assert blpop_block_seconds(30_000, None, 0.4) <= 0.4

    def test_never_returns_zero(self):
        # BLPOP reads 0 as "block forever", so a vanishing remainder must not
        # round down into an unbounded wait.
        assert blpop_block_seconds(30_000, None, 0.0000001) > 0


class TestEffectiveSocketTimeout:
    """An absent socket_timeout is not the same as an explicit None."""

    def test_reads_the_class_default_when_unset(self):
        client = aredis.Redis.from_url("redis://localhost:6379/15")
        # A plain from_url() client carries no socket_timeout key at all, so the
        # value has to come off the connection class — where redis-py 8 sets 5s.
        assert effective_socket_timeout(client) == 5

    def test_respects_an_explicit_none(self):
        client = aredis.Redis.from_url("redis://x", socket_timeout=None)
        assert effective_socket_timeout(client) is None

    def test_respects_an_explicit_value(self):
        client = aredis.Redis.from_url("redis://x", socket_timeout=2.5)
        assert effective_socket_timeout(client) == 2.5

    def test_unknown_client_shape_is_not_fatal(self):
        # Cluster clients, custom pools and test doubles must degrade to
        # "unknown" rather than raising.
        assert effective_socket_timeout(MagicMock(spec=aredis.Redis)) is None


class TestConstruction:
    def test_name_is_required(self):
        # A shared default name means two unrelated services silently share one
        # lock — a near-undebuggable outage.
        with pytest.raises(ValueError, match="name is required"):
            FifoLock(MagicMock(spec=aredis.Redis), "")

    def test_max_hold_must_exceed_the_lease(self):
        with pytest.raises(ValueError, match="max_hold_ms"):
            FifoLock(MagicMock(spec=aredis.Redis), "x", lease_ms=60, max_hold_ms=60)

    def test_keys_share_one_cluster_slot(self):
        lock = FifoLock(MagicMock(spec=aredis.Redis), "invoices")
        assert all(k.startswith("{invoices}:") for k in lock._keys)
