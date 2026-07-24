"""
Regression tests for the client-side socket timeout colliding with the gate's
own blocking waits.

redis-py 8 changed the default ``socket_timeout`` from ``None`` to 5 seconds.
The async waiter loop blocks on BLPOP for ``blpop_internal_timeout_ms`` (5000ms
by default), so on a stock client the server-side block and the client-side read
deadline expire at the same instant: redis-py wins the race, tears down the
connection and raises ``redis.exceptions.TimeoutError`` instead of BLPOP
returning nil.

The gate must work against *any* client, whatever its socket timeout, because a
library does not get to dictate how the caller builds their Redis client.
"""

import asyncio
import os
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
import redis.asyncio as aredis
import redis.exceptions

from redis_fifo_lock.async_gate import AsyncStreamGate

ASYNC_CLIENT_METHODS = (
    "xgroup_create",
    "xadd",
    "blpop",
    "xdel",
    "delete",
    "get",
    "xack",
    "xautoclaim",
    "xreadgroup",
    "xpending_range",
    "lpush",
    "pexpire",
    "set",
)


def make_mock_client(socket_timeout):
    """
    Async client mock whose connection pool reports ``socket_timeout``.

    Configured so that ``acquire()`` loses the SETNX race and therefore goes
    straight into the BLPOP waiter loop, which is what these tests are about.
    """
    mock = MagicMock(spec=aredis.Redis)
    for name in ASYNC_CLIENT_METHODS:
        setattr(mock, name, AsyncMock())

    mock.xadd.return_value = b"1-0"
    mock.set.return_value = False  # someone else holds the lock -> we wait
    mock.xpending_range.return_value = []
    mock.xautoclaim.return_value = ("0-0", [])
    mock.xreadgroup.return_value = []

    pool = MagicMock()
    pool.connection_kwargs = {"socket_timeout": socket_timeout}
    mock.connection_pool = pool
    return mock


def blpop_timeouts(mock):
    """Every ``timeout`` value the gate asked BLPOP to block for."""
    return [call.kwargs["timeout"] for call in mock.blpop.call_args_list]


class TestBlpopBlockDuration:
    """The BLPOP block must fit inside the client's read deadline."""

    async def test_blpop_block_stays_below_client_socket_timeout(self):
        """A 5s BLPOP against a 5s socket timeout is a coin flip redis-py wins."""
        client = make_mock_client(5.0)
        gate = AsyncStreamGate(client)  # default blpop_internal_timeout_ms=5000
        client.blpop.side_effect = [None, (b"sig", b"1")]

        await gate.acquire()

        timeouts = blpop_timeouts(client)
        assert timeouts, "BLPOP was never called"
        assert all(t < 5.0 for t in timeouts), (
            f"BLPOP was told to block for {timeouts}, which meets or exceeds the "
            f"client's 5.0s socket timeout; redis-py raises TimeoutError and drops "
            f"the connection instead of letting BLPOP return nil"
        )

    async def test_blpop_block_stays_below_short_socket_timeout(self):
        """The clamp has to track the client, not just dodge the 5s default."""
        client = make_mock_client(1.0)
        gate = AsyncStreamGate(client, blpop_internal_timeout_ms=5000)
        client.blpop.side_effect = [None, (b"sig", b"1")]

        await gate.acquire()

        timeouts = blpop_timeouts(client)
        assert all(t < 1.0 for t in timeouts), (
            f"BLPOP was told to block for {timeouts} against a 1.0s socket timeout"
        )

    async def test_sub_second_internal_timeout_is_not_rounded_up(self):
        """``max(1, ms // 1000)`` silently floors every sub-second config at 1s."""
        client = make_mock_client(None)
        gate = AsyncStreamGate(client, blpop_internal_timeout_ms=250)
        client.blpop.side_effect = [None, (b"sig", b"1")]

        await gate.acquire()

        first = blpop_timeouts(client)[0]
        assert first == pytest.approx(0.25), (
            f"blpop_internal_timeout_ms=250 should block for 0.25s, blocked for {first}s"
        )

    async def test_acquire_does_not_block_past_user_deadline(self):
        """``max(1, int(remaining))`` overshoots any deadline under one second."""
        client = make_mock_client(None)
        gate = AsyncStreamGate(client)

        async def slow_none(*args, **kwargs):
            await asyncio.sleep(0.05)
            return None

        client.blpop.side_effect = slow_none

        with pytest.raises(asyncio.TimeoutError):
            await gate.acquire(timeout=0.5)

        first = blpop_timeouts(client)[0]
        assert first <= 0.55, (
            f"BLPOP was told to block {first}s but the caller's deadline was only "
            f"0.5s away; acquire() overshoots its own timeout"
        )


class TestClientTimeoutIsSurvivable:
    """A client-side read deadline is a wake-up, not a failure."""

    async def test_client_timeout_error_is_treated_as_a_wakeup(self):
        """The waiter loop must absorb it and keep waiting, as it does for nil."""
        client = make_mock_client(5.0)
        gate = AsyncStreamGate(client)
        client.blpop.side_effect = [
            redis.exceptions.TimeoutError("Timeout reading from localhost:6379"),
            (b"sig", b"1"),
        ]

        owner, msg_id = await gate.acquire()

        assert msg_id == b"1-0"
        assert client.blpop.call_count == 2, (
            "acquire() gave up on the first client-side timeout instead of "
            "treating it as a spurious wake-up and blocking again"
        )

    async def test_client_timeout_still_runs_recovery(self):
        """Waking up is the whole point: recovery must run on each wake-up."""
        client = make_mock_client(5.0)
        gate = AsyncStreamGate(client)
        client.blpop.side_effect = [
            redis.exceptions.TimeoutError("Timeout reading from localhost:6379"),
            (b"sig", b"1"),
        ]

        await gate.acquire()

        assert client.xpending_range.called, (
            "no crash-recovery pass ran after the waiter woke up"
        )

    async def test_persistent_client_timeouts_eventually_raise(self):
        """Absorbing timeouts must not turn a dead Redis into an infinite loop."""
        client = make_mock_client(5.0)
        gate = AsyncStreamGate(client, blpop_internal_timeout_ms=10)
        client.blpop.side_effect = redis.exceptions.TimeoutError("redis is gone")

        with pytest.raises(redis.exceptions.TimeoutError):
            await asyncio.wait_for(gate.acquire(), timeout=5)

        assert client.blpop.call_count > 1, (
            "acquire() bailed on the first timeout; it should retry a bounded "
            "number of times before declaring the connection dead"
        )


def redis_url():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
    if not url.startswith("redis://") and not url.startswith("rediss://"):
        url = f"redis://{url}/15"
    return url


def as_str(msg_id):
    return msg_id.decode() if isinstance(msg_id, bytes) else msg_id


@pytest_asyncio.fixture
async def gate_factory():
    """Builds gates with unique keys over caller-configured clients, and cleans up."""
    created = []

    async def factory(**client_kwargs):
        client = await aredis.from_url(
            redis_url(), decode_responses=False, **client_kwargs
        )
        try:
            await client.ping()
        except Exception as e:  # pragma: no cover - environment dependent
            pytest.skip(f"Redis not available: {e}")
        test_id = str(uuid.uuid4())[:8]
        gate = AsyncStreamGate(
            client,
            stream=f"test-sock-stream:{test_id}",
            group=f"test-sock-group:{test_id}",
            sig_prefix=f"test-sock-sig:{test_id}:",
            last_key=f"test-sock-last:{test_id}",
        )
        created.append(gate)
        return gate

    yield factory

    for gate in created:
        try:
            keys = await gate.r.keys(f"{gate.sig_prefix}*")
            await gate.r.delete(gate.stream, gate.last_key, *keys)
        except Exception:
            pass
        try:
            await gate.r.aclose()
        except Exception:
            pass


class TestRealRedisSocketTimeout:
    """End to end, against a real server, with a caller-built client."""

    async def test_waiter_survives_short_client_socket_timeout(self, gate_factory):
        """Waiting longer than the socket timeout is normal, not an error."""
        gate = await gate_factory(socket_timeout=1.0)

        owner1, msg1 = await gate.acquire()
        waiter = asyncio.create_task(gate.acquire(timeout=20))

        # Sit on the lock for several times the client's 1s read deadline.
        await asyncio.sleep(3)
        await gate.release(owner1, as_str(msg1))

        owner2, msg2 = await asyncio.wait_for(waiter, timeout=10)
        await gate.release(owner2, as_str(msg2))
        assert owner2 != owner1

    async def test_waiter_survives_default_client(self, gate_factory):
        """The CI scenario: stock ``from_url()`` client, redis-py 8 defaults."""
        gate = await gate_factory()

        owner1, msg1 = await gate.acquire()
        waiter = asyncio.create_task(gate.acquire(timeout=30))

        # Cross the 5s default socket timeout while holding the lock.
        await asyncio.sleep(7)
        await gate.release(owner1, as_str(msg1))

        owner2, msg2 = await asyncio.wait_for(waiter, timeout=15)
        await gate.release(owner2, as_str(msg2))
        assert owner2 != owner1
