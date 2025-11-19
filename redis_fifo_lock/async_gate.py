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
            await self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _dispatch_waiter(self, entry: Tuple[str, dict]) -> Optional[str]:
        """
        Extract owner from stream entry and signal them.

        Args:
            entry: Tuple of (message_id, fields) from XREADGROUP

        Returns:
            The owner that was dispatched, or None if no valid owner
        """
        msg_id, fields = entry

        # Handle both decoded (string keys) and non-decoded (bytes keys) responses
        owner = fields.get("owner") or fields.get(b"owner", b"")
        if isinstance(owner, bytes):
            owner = owner.decode()

        if not owner:
            return None

        # Signal the waiter
        sig = self.sig_prefix + owner
        await self.r.lpush(sig, 1)
        await self.r.pexpire(sig, self.sig_ttl_ms)

        # Mark this message as the currently dispatched one
        await self.r.set(self.last_key, msg_id)

        return owner

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

        # 2) Try to become the lock holder (if no one is holding it)
        acquired = await self.r.set(self.last_key, msg_id, nx=True)

        if acquired:
            # We got the lock! Claim our message in the consumer group via XREADGROUP
            res = await self.r.xreadgroup(
                self.group,
                self.adv_consumer,
                streams={self.stream: ">"},
                count=1,
                block=1,  # 1ms timeout (essentially non-blocking, block=0 means wait forever!)
            )
            # Validate that we got our message
            if not res or not res[0] or not res[0][1]:
                raise RuntimeError(
                    f"XREADGROUP returned no messages after SETNX succeeded. "
                    f"Expected to claim msg_id {msg_id}"
                )
            # Signal ourselves - we'll wait on BLPOP below to consume the signal
            sig_key = self.sig_prefix + owner
            await self.r.lpush(sig_key, 1)
            await self.r.pexpire(sig_key, self.sig_ttl_ms)

        # 3) Block until the dispatcher signals you (could be ourselves or previous holder)
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

    async def _dispatch_next_async(self) -> None:
        """
        Background task to dispatch the next waiter after release completes.
        This ensures release() returns before the next waiter is signaled.
        """
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
                dispatched = await self._dispatch_waiter(claimed[0])
                if dispatched:
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
            block=1,  # 1ms timeout (essentially non-blocking, block=0 means wait forever!)
        )

        if not res:
            # queue empty → clear pointer
            await self.r.delete(self.last_key)
            return

        _, entries = res[0]
        await self._dispatch_waiter(entries[0])

    async def release(self, owner: str, msg_id: str) -> None:
        """
        Holder calls this when done. Acks the currently active entry (if any) and
        dispatches the next in FIFO. Best-effort crash recovery first.

        Args:
            owner: Owner UUID (currently unused but kept for API compatibility)
            msg_id: Stream message ID to acknowledge
        """
        await self.ensure_group()

        # 1) Ack OUR OWN message (not from last_key!)
        try:
            await self.r.xack(self.stream, self.group, msg_id)
        except Exception:
            pass  # already acked or gone

        # Schedule dispatch as background task to ensure release() completes first
        # Use create_task with name for better debugging, task will complete on its own
        task = asyncio.create_task(self._dispatch_next_async())
        # Don't await - let it run in background, but add done callback to handle errors
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)

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
