"""
Redis Stream-based FIFO lock implementation.

This module provides both synchronous and asynchronous lock-like classes
that ensure strict FIFO ordering using Redis Streams.
"""

from neutron.redis_stream.async_gate import AsyncStreamGate
from neutron.redis_stream.sync import StreamGate

__all__ = ["StreamGate", "AsyncStreamGate"]
