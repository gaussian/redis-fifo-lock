"""
Redis Stream-based FIFO lock implementation.

This module provides both synchronous and asynchronous lock-like classes
that ensure strict FIFO ordering using Redis Streams.
"""

__version__ = "0.1.1"

from redis_fifo_lock.async_gate import AsyncStreamGate
from redis_fifo_lock.sync import StreamGate

__all__ = ["StreamGate", "AsyncStreamGate"]
