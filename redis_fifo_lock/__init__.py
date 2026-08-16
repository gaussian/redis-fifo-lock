"""
Redis Stream-based FIFO lock implementation.

A distributed FIFO mutex on Redis: callers are served strictly in the order
Redis saw them arrive, and a holder that dies is detected and replaced.
"""

__version__ = "0.3.1"

from redis_fifo_lock.holds import HoldSet
from redis_fifo_lock.lock import FifoLock, Lease, LeaseLost

__all__ = ["FifoLock", "HoldSet", "Lease", "LeaseLost"]
