"""
Asynchronous Redis Stream-based FIFO lock.
"""

import asyncio
import uuid
from typing import Optional, Tuple

import redis.asyncio as redis

from redis_fifo_lock.common import (
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
        dead_holder_timeout_ms: int = 120_000,
        blpop_internal_timeout_ms: int = 5_000,
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
            dead_holder_timeout_ms: Idle time (ms) before considering holder truly dead (default 2 minutes)
            blpop_internal_timeout_ms: Internal BLPOP timeout in milliseconds for waiter recovery loop (default 5000)
        """
        self.r = r
        self.stream = stream
        self.group = group
        self.adv_consumer = adv_consumer or get_advancer_consumer()
        self.sig_prefix = sig_prefix
        self.sig_ttl_ms = sig_ttl_ms
        self.claim_idle_ms = claim_idle_ms
        self.last_key = last_key
        self.dead_holder_timeout_ms = dead_holder_timeout_ms
        self.blpop_internal_timeout_ms = blpop_internal_timeout_ms

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

    async def _recover_and_maybe_dispatch(self) -> bool:
        """
        Attempt crash recovery and normal dispatch. Called by waiters and release().

        This implements:
        1. Crash recovery via XAUTOCLAIM (reclaim idle entries)
        2. Dead holder timeout: XACK entries idle >= 2 minutes
        3. Normal dispatch via XREADGROUP (read next new message)

        Returns:
            True if a waiter was dispatched, False if queue is empty
        """
        # Phase 1: Crash recovery - check for dead or idle holders
        # CRITICAL: Check XPENDING *before* XAUTOCLAIM, because XAUTOCLAIM resets the idle timer
        try:
            # First, check pending entries WITHOUT claiming them
            pending_info = await self.r.xpending_range(
                self.stream,
                self.group,
                min="-",
                max="+",
                count=1,
            )

            if pending_info and len(pending_info) > 0:
                entry = pending_info[0]
                # Keys: "message_id", "consumer", "time_since_delivered", "times_delivered"
                idle_time_ms = entry.get("time_since_delivered", 0)
                msg_id = entry.get("message_id")

                # Allow 50ms tolerance for timing/network latency precision
                if idle_time_ms >= (self.dead_holder_timeout_ms - 50):
                    # Dead holder - XACK to remove from queue
                    await self.r.xack(self.stream, self.group, msg_id)
                    # Don't dispatch this one - fall through to dispatch next message

                elif idle_time_ms >= self.claim_idle_ms:
                    # Idle but not dead - claim and re-signal the holder
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
                            return True
                # else: idle < claim_idle_ms, still processing normally, do nothing

        except Exception:
            # Recovery is best-effort; proceed to normal dispatch
            pass

        # Phase 2: Normal dispatch - read next new message
        try:
            res = await self.r.xreadgroup(
                self.group,
                self.adv_consumer,
                streams={self.stream: ">"},
                count=1,
                block=1,  # 1ms timeout (essentially non-blocking)
            )

            if not res:
                # Queue empty → clear pointer
                await self.r.delete(self.last_key)
                return False

            _, entries = res[0]
            if entries:
                await self._dispatch_waiter(entries[0])
                return True
        except Exception:
            pass

        return False

    async def acquire(self, timeout: Optional[int] = None) -> Tuple[str, str]:
        """
        Join the FIFO and block until dispatched.

        Implements waiter-driven crash recovery: BLPOP runs in a loop with 5-second
        internal timeout. On each wake, if not signaled, the waiter calls recovery
        logic to detect and advance past dead holders.

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
            # Validate that we got a message
            if not res or not res[0] or not res[0][1]:
                raise RuntimeError(
                    f"XREADGROUP returned no messages after SETNX succeeded. "
                    f"Expected to claim msg_id {msg_id}"
                )

            # Check if the message we read is our own
            read_msg_id, read_fields = res[0][1][0]

            if read_msg_id == msg_id:
                # We read our own message - we're first in FIFO! Signal ourselves.
                sig_key = self.sig_prefix + owner
                await self.r.lpush(sig_key, 1)
                await self.r.pexpire(sig_key, self.sig_ttl_ms)
            else:
                # We read someone else's message - they're first in FIFO.
                # Dispatch them (signal + set last_key) and wait for our turn.
                await self._dispatch_waiter((read_msg_id, read_fields))

        # 3) Block until the dispatcher signals you (could be ourselves or previous holder)
        # Use internal timeout loop for waiter-driven crash recovery
        sig_key = self.sig_prefix + owner
        deadline = None if timeout is None else (asyncio.get_event_loop().time() + timeout)

        while True:
            # Internal wait for periodic recovery checks
            internal_timeout = max(1, self.blpop_internal_timeout_ms // 1000)
            if deadline is not None:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    # User timeout reached
                    await self._cancel_ticket(owner, msg_id, sig_key)
                    raise asyncio.TimeoutError("acquire timed out waiting for dispatch")
                # Use smaller of internal timeout or remaining time
                internal_timeout = min(internal_timeout, max(1, int(remaining)))

            res = await self.r.blpop(sig_key, timeout=internal_timeout)

            if res is not None:
                # Got our signal → it's our turn
                return owner, msg_id

            # We woke up because internal timeout hit, not because we were signaled
            # Run crash/recovery logic to detect dead leaders and advance the queue
            try:
                await self._recover_and_maybe_dispatch()
            except Exception:
                # Recovery is best-effort, continue waiting
                pass
            # Loop and BLPOP again

    async def _cancel_ticket(self, owner: str, msg_id: str, sig_key: str) -> None:
        """
        Best-effort cleanup when acquire times out.

        Args:
            owner: Owner UUID
            msg_id: Stream message ID
            sig_key: Signal key for this owner
        """
        try:
            await self.r.xdel(self.stream, msg_id)
        finally:
            # Drain possible late signal
            await self.r.delete(sig_key)

    async def _dispatch_next_async(self) -> None:
        """
        Background task to dispatch the next waiter after release completes.
        This ensures release() returns before the next waiter is signaled.

        Delegates to _recover_and_maybe_dispatch() for all recovery and dispatch logic.
        """
        await self._recover_and_maybe_dispatch()

    async def release(self, owner: str, msg_id: str) -> None:
        """
        Holder calls this when done. Acks the currently active entry (if any) and
        dispatches the next in FIFO. Best-effort crash recovery first.

        Guards against double-release: only the current holder (last_key == msg_id)
        can drive dispatch. This prevents race conditions and maintains invariants.

        Args:
            owner: Owner UUID (currently unused but kept for API compatibility)
            msg_id: Stream message ID to acknowledge
        """
        await self.ensure_group()

        # Guard: Only the current holder should drive dispatch
        # This prevents double-release from breaking last_key invariants
        current = await self.r.get(self.last_key)
        # Handle both decode_responses=True (str) and decode_responses=False (bytes)
        current_str = current.decode() if isinstance(current, bytes) else current
        if current is None or current_str != msg_id:
            # Either already advanced or wrong session; just ack best-effort and bail
            try:
                await self.r.xack(self.stream, self.group, msg_id)
            except Exception:
                pass
            return

        # 1) Ack OUR OWN message (not from last_key!)
        try:
            await self.r.xack(self.stream, self.group, msg_id)
        except Exception:
            pass  # already acked or gone

        # Dispatch next waiter synchronously to ensure it completes before release() returns.
        # This prevents the dispatch from being cancelled if the caller's event loop closes.
        await self._dispatch_next_async()

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
                # Decode msg_id if bytes (Redis returns bytes by default)
                msg_id_str = self.msg_id.decode() if isinstance(self.msg_id, bytes) else self.msg_id
                await self.gate.release(self.owner, msg_id_str)
                return False

        return _Session(self, timeout)
