"""
Asynchronous Redis Stream-based FIFO lock.
"""

import asyncio
import uuid
from typing import Optional, Tuple

import redis.asyncio as redis

from neutron.redis_stream.common import (
    DEFAULT_CLAIM_IDLE_MS,
    DEFAULT_GROUP,
    DEFAULT_LAST_KEY,
    DEFAULT_SIG_PREFIX,
    DEFAULT_SIG_TTL_MS,
    DEFAULT_STREAM,
    get_advancer_consumer,
)


class AsyncStreamGate:
    """
    FIFO baton using Redis Streams (asynchronous version).

    - Enqueue: XADD STREAM * owner=<uuid>
    - Dispatch: one-at-a-time via consumer group
    - Holder completes ⇒ release(): XACK previous + dispatch next
    - Crash safety: XAUTOCLAIM re-delivers stuck holder after idle timeout
    """

    def __init__(
        self,
        r: redis.Redis,
        stream: str = DEFAULT_STREAM,
        group: str = DEFAULT_GROUP,
        adv_consumer: Optional[str] = None,
        sig_prefix: str = DEFAULT_SIG_PREFIX,
        sig_ttl_ms: int = DEFAULT_SIG_TTL_MS,
        claim_idle_ms: int = DEFAULT_CLAIM_IDLE_MS,
        last_key: str = DEFAULT_LAST_KEY,
    ):
        """
        Initialize AsyncStreamGate.

        Args:
            r: Async Redis client instance
            stream: Stream name for the gate
            group: Consumer group name
            adv_consumer: Dispatcher/advancer consumer identity (auto-generated if None)
            sig_prefix: Prefix for per-waiter signal keys
            sig_ttl_ms: TTL for signal keys in milliseconds
            claim_idle_ms: Idle time before considering a holder dead
            last_key: Key to store the last dispatched message ID
        """
        self.r = r
        self.stream = stream
        self.group = group
        self.adv_consumer = adv_consumer or get_advancer_consumer()
        self.sig_prefix = sig_prefix
        self.sig_ttl_ms = sig_ttl_ms
        self.claim_idle_ms = claim_idle_ms
        self.last_key = last_key

    async def ensure_group(self) -> None:
        """Create stream + group if missing."""
        try:
            await self.r.xgroup_create(self.stream, self.group, id="$", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def acquire(self, timeout: Optional[int] = None) -> Tuple[str, str]:
        """
        Join the FIFO and block until dispatched.

        Args:
            timeout: Seconds to wait for dispatch; None = infinite

        Returns:
            Tuple of (owner_uuid, stream_message_id)

        Raises:
            asyncio.TimeoutError: If timeout is reached before being dispatched
        """
        await self.ensure_group()
        owner = str(uuid.uuid4())

        # 1) Enqueue your ticket
        msg_id = await self.r.xadd(self.stream, {"owner": owner})

        # 2) Block until the dispatcher signals you
        sig_key = self.sig_prefix + owner
        res = await self.r.blpop(sig_key, timeout=timeout)

        if res is None:
            # Timed out ⇒ best-effort cancel our ticket
            try:
                await self.r.xdel(self.stream, msg_id)
            finally:
                # drain possible late signal
                await self.r.delete(sig_key)
            raise asyncio.TimeoutError("acquire timed out waiting for dispatch")

        return owner, msg_id

    async def release(self, owner: str, msg_id: str) -> None:
        """
        Holder calls this when done. Acks the currently active entry (if any) and
        dispatches the next in FIFO. Best-effort crash recovery first.

        Args:
            owner: Owner UUID (currently unused but kept for API compatibility)
            msg_id: Stream message ID (currently unused but kept for API compatibility)
        """
        await self.ensure_group()

        # 1) Ack previously dispatched message (idempotent)
        last = await self.r.get(self.last_key)
        if last:
            try:
                await self.r.xack(self.stream, self.group, last.decode())
            except Exception:
                pass  # already acked or gone

        # 2) Crash recovery: reclaim the oldest idle pending entry (if any) and re-signal
        try:
            next_start, claimed = await self.r.xautoclaim(
                self.stream,
                self.group,
                self.adv_consumer,
                min_idle_time=self.claim_idle_ms,
                start_id="0-0",
                count=1,
            )
            if claimed:
                stuck_id, fields = claimed[0]
                owner2 = fields.get(b"owner", b"").decode()
                if owner2:
                    sig = self.sig_prefix + owner2
                    # wake the rightful owner
                    await self.r.lpush(sig, 1)
                    await self.r.pexpire(sig, self.sig_ttl_ms)
                    await self.r.set(self.last_key, stuck_id)
                    return
        except Exception:
            # recovery is best-effort; proceed to normal dispatch
            pass

        # 3) Normal dispatch: deliver next new message in order
        res = await self.r.xreadgroup(
            self.group,
            self.adv_consumer,
            streams={self.stream: ">"},
            count=1,
            block=0,
        )

        if not res:
            # queue empty → clear pointer
            await self.r.delete(self.last_key)
            return

        _, entries = res[0]
        next_id, fields = entries[0]
        owner3 = fields.get(b"owner", b"").decode()
        if owner3:
            sig = self.sig_prefix + owner3
            await self.r.lpush(sig, 1)
            await self.r.pexpire(sig, self.sig_ttl_ms)
            await self.r.set(self.last_key, next_id)

    async def cancel(self, owner: str, msg_id: str) -> None:
        """
        Call if you want to give up before being dispatched.

        Args:
            owner: Owner UUID
            msg_id: Stream message ID to cancel
        """
        await self.r.xdel(self.stream, msg_id)
        await self.r.delete(self.sig_prefix + owner)

    async def session(self, timeout: Optional[int] = None):
        """
        Async context manager for a gate session.

        Args:
            timeout: Optional timeout for acquire

        Returns:
            Context manager that acquires on entry and releases on exit
        """

        class _Session:
            def __init__(self, gate, timeout):
                self.gate = gate
                self.timeout = timeout
                self.owner = None
                self.msg_id = None

            async def __aenter__(self):
                self.owner, self.msg_id = await self.gate.acquire(timeout=self.timeout)
                return self

            async def __aexit__(self, exc_type, exc, tb):
                # Always release; idempotent enough for typical use.
                await self.gate.release(self.owner, self.msg_id)
                return False

        return _Session(self, timeout)
