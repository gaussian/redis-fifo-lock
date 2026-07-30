"""
Client introspection shared by the lock.

Both functions exist for the same reason: the caller supplies the Redis client,
and the lock has to work with whatever they hand it rather than dictating its
configuration.
"""

import inspect
from typing import Optional

# Fraction of the client's socket timeout we will spend inside a single blocking
# call. redis-py enforces its read deadline client-side and raises (dropping the
# connection) the moment it passes, so the server-side block has to finish
# comfortably first - even when the caller is busy under contention.
SOCKET_TIMEOUT_SAFETY_FACTOR = 0.5
MIN_BLOCK_SECONDS = 0.1


def effective_socket_timeout(client) -> Optional[float]:
    """
    Best-effort read of the read deadline redis-py will enforce on this client.

    The lock blocks server-side while waiting, and must always come back before
    that deadline, otherwise redis-py raises TimeoutError and tears the
    connection down instead of the command simply returning nil. redis-py 8
    made this pressing by defaulting ``socket_timeout`` to 5 seconds where
    earlier versions used ``None``.

    An absent ``socket_timeout`` key is not the same as ``socket_timeout=None``:
    a plain ``from_url()`` client carries no key at all and inherits the
    connection class default, so the default has to be read off the class. It is
    declared on a base class rather than the concrete one, hence the MRO walk.

    Args:
        client: A redis-py client (sync or async)

    Returns:
        Seconds before redis-py gives up on a read, or None when there is no
        deadline or it cannot be determined - a custom pool, a cluster or
        sentinel client, a test double. Callers must tolerate a TimeoutError
        anyway, since this can only ever be best-effort.
    """
    try:
        pool = client.connection_pool
        if "socket_timeout" in pool.connection_kwargs:
            return pool.connection_kwargs["socket_timeout"]
        for klass in pool.connection_class.__mro__:
            param = inspect.signature(klass.__init__).parameters.get("socket_timeout")
            if param is not None and param.default is not inspect.Parameter.empty:
                return param.default
    except Exception:
        # Anything unexpected about the client's shape means "unknown".
        pass
    return None


def blpop_block_seconds(
    configured_ms: int,
    socket_timeout: Optional[float],
    remaining: Optional[float],
) -> float:
    """
    How long a single blocking wait may block for.

    Bounded by three things: the configured poll interval, the client's socket
    timeout (exceeding it turns a routine nil reply into a raised TimeoutError
    and a dropped connection), and whatever is left of the caller's own
    deadline.

    Args:
        configured_ms: The lock's ``poll_ms``
        socket_timeout: Client read deadline in seconds, or None if unknown
        remaining: Seconds left on the caller's timeout, or None if infinite

    Returns:
        Seconds to block, always positive and rounded to whole milliseconds so
        it never reaches Redis in exponent notation or as a bare 0, which BLPOP
        reads as "block forever".
    """
    block_s = configured_ms / 1000.0

    if socket_timeout is not None:
        block_s = min(
            block_s,
            max(MIN_BLOCK_SECONDS, socket_timeout * SOCKET_TIMEOUT_SAFETY_FACTOR),
        )

    if remaining is not None:
        block_s = min(block_s, remaining)

    return max(round(block_s, 3), 0.001)
