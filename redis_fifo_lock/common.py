"""
Common utilities and constants for Redis Stream gates.
"""

import os

# Default configuration constants
DEFAULT_STREAM = "gate:stream"
DEFAULT_GROUP = "gate:group"
DEFAULT_SIG_PREFIX = "gate:sig:"
DEFAULT_SIG_TTL_MS = 5 * 60 * 1000  # 5 minutes
DEFAULT_CLAIM_IDLE_MS = 60_000  # 60 seconds
DEFAULT_LAST_KEY = "gate:last-dispatched"


def get_advancer_consumer(pid: int = None) -> str:
    """Generate a dispatcher/advancer consumer identity."""
    if pid is None:
        pid = os.getpid()
    return f"advancer:{pid}"
