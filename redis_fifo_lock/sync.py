"""
Synchronous Redis Stream-based FIFO lock.
"""

import time
import uuid
from typing import Optional, Tuple

import redis

from redis_fifo_lock.common import (
    DEFAULT_BLPOP_INTERNAL_TIMEOUT_MS,
    DEFAULT_CLAIM_IDLE_MS,
    DEFAULT_GROUP,
    DEFAULT_LAST_KEY,
    DEFAULT_SIG_PREFIX,
    DEFAULT_SIG_TTL_MS,
    DEFAULT_STREAM,
    MAX_CONSECUTIVE_BLPOP_TIMEOUTS,
    blpop_block_seconds,
    effective_socket_timeout,
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
        blpop_internal_timeout_ms: int = DEFAULT_BLPOP_INTERNAL_TIMEOUT_MS,
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
            blpop_internal_timeout_ms: How long a single BLPOP blocks before the
                waiter loops (default 5000). Capped at runtime so each BLPOP
                returns before the client's own socket timeout.
        """
        self.r = r
        self.stream = stream
        self.group = group
        self.adv_consumer = adv_consumer or get_advancer_consumer()
        self.sig_prefix = sig_prefix
        self.sig_ttl_ms = sig_ttl_ms
        self.claim_idle_ms = claim_idle_ms
        self.last_key = last_key
        self.blpop_internal_timeout_ms = blpop_internal_timeout_ms
        self._socket_timeout = effective_socket_timeout(r)

    def ensure_group(self) -> None:
        """Create stream + group if missing."""
        try:
            self.r.xgroup_create(self.stream, self.group, id="$", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _signal(self, owner: str) -> None:
        """Wake the waiter identified by ``owner``."""
        sig = self.sig_prefix + owner
        self.r.lpush(sig, 1)
        self.r.pexpire(sig, self.sig_ttl_ms)

    def _dispatch_waiter(self, msg_id, fields) -> Optional[str]:
        """
        Signal the owner of a stream entry and mark it as the active one.

        Args:
            msg_id: Stream message ID
            fields: Entry fields from XREADGROUP/XAUTOCLAIM

        Returns:
            The owner that was dispatched, or None if the entry has no owner
        """
        # Handle both decoded (string keys) and non-decoded (bytes keys) responses
        owner = fields.get("owner") or fields.get(b"owner", b"")
        if isinstance(owner, bytes):
            owner = owner.decode()

        if not owner:
            return None

        self._signal(owner)
        self.r.set(self.last_key, msg_id)
        return owner

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

        # 2) Try to become the lock holder (if no one is holding it).
        # Without this an idle gate has nobody to dispatch the first caller, and
        # acquire() waits for a signal that is never sent.
        if self.r.set(self.last_key, msg_id, nx=True):
            res = self.r.xreadgroup(
                self.group,
                self.adv_consumer,
                {self.stream: ">"},
                count=1,
                block=1,  # 1ms timeout (essentially non-blocking, block=0 means wait forever!)
            )
            if not res or not res[0] or not res[0][1]:
                raise RuntimeError(
                    f"XREADGROUP returned no messages after SETNX succeeded. "
                    f"Expected to claim msg_id {msg_id}"
                )

            read_msg_id, read_fields = res[0][1][0]
            if read_msg_id == msg_id:
                # We read our own message - we're first in FIFO! Signal ourselves.
                self._signal(owner)
            else:
                # We read someone else's message - they're first in FIFO.
                # Dispatch them (signal + set last_key) and wait for our turn.
                self._dispatch_waiter(read_msg_id, read_fields)

        # 3) Block until the dispatcher signals you.
        #
        # BLPOP runs in a loop rather than as one long block: the caller's
        # timeout is theirs, but the client's socket timeout is redis-py's, and
        # a block that outlives the latter raises instead of returning nil.
        sig_key = self.sig_prefix + owner
        deadline = None if timeout is None else (time.monotonic() + timeout)
        consecutive_timeouts = 0

        while True:
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._cancel_ticket(owner, msg_id, sig_key)
                    raise TimeoutError("acquire timed out waiting for dispatch")

            try:
                res = self.r.blpop(
                    sig_key,
                    timeout=blpop_block_seconds(
                        self.blpop_internal_timeout_ms, self._socket_timeout, remaining
                    ),
                )
            except redis.TimeoutError:
                # The client's read deadline fired, not ours. Nobody signalled
                # us, which is the same thing a nil reply means, so block again.
                # Bounded: if it keeps happening the connection is dead, not idle.
                consecutive_timeouts += 1
                if consecutive_timeouts >= MAX_CONSECUTIVE_BLPOP_TIMEOUTS:
                    try:
                        self._cancel_ticket(owner, msg_id, sig_key)
                    except Exception:
                        # The connection is already suspect; do not mask the
                        # timeout with whatever cleanup hits on the way out.
                        pass
                    raise
                continue
            else:
                consecutive_timeouts = 0

            if res is not None:
                return owner, msg_id

    def _cancel_ticket(self, owner: str, msg_id: str, sig_key: str) -> None:
        """
        Best-effort cleanup when acquire gives up.

        Args:
            owner: Owner UUID
            msg_id: Stream message ID
            sig_key: Signal key for this owner
        """
        try:
            self.r.xdel(self.stream, msg_id)
        finally:
            # drain possible late signal
            self.r.delete(sig_key)

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
                if self._dispatch_waiter(stuck_id, stuck_fields):
                    return
        except Exception:
            # recovery is best-effort; proceed to normal dispatch
            pass

        # 3) Normal dispatch: deliver next new message in order
        res = self.r.xreadgroup(
            self.group,
            self.adv_consumer,
            {self.stream: ">"},
            count=1,
            block=1,  # 1ms timeout (essentially non-blocking, block=0 means wait forever!)
        )

        if not res:
            # queue empty → clear pointer
            self.r.delete(self.last_key)
            return

        # Structure: [(stream, [(id, {fields})])]
        _, entries = res[0]
        next_id, fields = entries[0]
        self._dispatch_waiter(next_id, fields)

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
