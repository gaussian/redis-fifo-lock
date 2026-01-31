"""
Synchronous Redis Stream-based FIFO lock.
"""

import uuid
from typing import Optional, Tuple

import redis

from redis_fifo_lock.common import (
    DEFAULT_CLAIM_IDLE_MS,
    DEFAULT_GROUP,
    DEFAULT_LAST_KEY,
    DEFAULT_SIG_PREFIX,
    DEFAULT_SIG_TTL_MS,
    DEFAULT_STREAM,
    get_advancer_consumer,
)


class StreamGate:
    """
    FIFO baton using Redis Streams (synchronous version).

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
        Initialize StreamGate.

        Args:
            r: Redis client instance
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

    def ensure_group(self) -> None:
        """Create stream + group if missing."""
        try:
            self.r.xgroup_create(self.stream, self.group, id="$", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def acquire(self, timeout: Optional[int] = None) -> Tuple[str, str]:
        """
        Join the FIFO and block until dispatched.

        Args:
            timeout: Seconds to wait for dispatch; None = infinite

        Returns:
            Tuple of (owner_uuid, stream_message_id)

        Raises:
            TimeoutError: If timeout is reached before being dispatched
        """
        self.ensure_group()
        owner = str(uuid.uuid4())

        # 1) Enqueue your ticket
        msg_id = self.r.xadd(self.stream, {"owner": owner})

        # 2) Block until the dispatcher signals you
        sig_key = self.sig_prefix + owner
        res = self.r.blpop(sig_key, timeout=timeout)

        if res is None:
            # Timed out ⇒ best-effort cancel our ticket
            try:
                self.r.xdel(self.stream, msg_id)
            finally:
                # drain possible late signal
                self.r.delete(sig_key)
            raise TimeoutError("acquire timed out waiting for dispatch")

        return owner, msg_id

    def release(self, owner: str, msg_id: str) -> None:
        """
        Holder calls this when done. Acks the currently active entry (if any) and
        dispatches the next in FIFO. Best-effort crash recovery first.

        Args:
            owner: Owner UUID (currently unused but kept for API compatibility)
            msg_id: Stream message ID (currently unused but kept for API compatibility)
        """
        self.ensure_group()

        # 1) Ack previously dispatched message (idempotent)
        last = self.r.get(self.last_key)
        if last:
            try:
                self.r.xack(self.stream, self.group, last.decode())
            except Exception:
                pass  # already acked or gone

        # 2) Crash recovery: reclaim the oldest idle pending entry (if any) and re-signal
        try:
            claimed = self.r.xautoclaim(
                self.stream,
                self.group,
                self.adv_consumer,
                min_idle_time=self.claim_idle_ms,
                start_id="0-0",
                count=1,
            )
            # redis-py returns (next_start_id, [(id, fields)]...)
            if claimed and isinstance(claimed, tuple) and claimed[1]:
                stuck_id, stuck_fields = claimed[1][0]
                owner2 = stuck_fields.get(b"owner", b"").decode()
                if owner2:
                    sig = self.sig_prefix + owner2
                    self.r.lpush(sig, 1)
                    self.r.pexpire(sig, self.sig_ttl_ms)
                    self.r.set(self.last_key, stuck_id)
                    return
        except Exception:
            # recovery is best-effort; proceed to normal dispatch
            pass

        # 3) Normal dispatch: deliver next new message in order
        res = self.r.xreadgroup(
            self.group, self.adv_consumer, {self.stream: ">"}, count=1, block=0
        )

        if not res:
            # queue empty → clear pointer
            self.r.delete(self.last_key)
            return

        # Structure: [(stream, [(id, {fields})])]
        _, entries = res[0]
        next_id, fields = entries[0]
        owner3 = fields.get(b"owner", b"").decode()
        if owner3:
            sig = self.sig_prefix + owner3
            self.r.lpush(sig, 1)
            self.r.pexpire(sig, self.sig_ttl_ms)
            self.r.set(self.last_key, next_id)

    def cancel(self, owner: str, msg_id: str) -> None:
        """
        Call if you want to give up before being dispatched.

        Args:
            owner: Owner UUID
            msg_id: Stream message ID to cancel
        """
        self.r.xdel(self.stream, msg_id)
        self.r.delete(self.sig_prefix + owner)

    def __enter__(self):
        """Context manager entry."""
        self.owner, self.msg_id = self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release(self.owner, self.msg_id)
        return False
