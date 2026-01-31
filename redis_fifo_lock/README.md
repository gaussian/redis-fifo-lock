# Redis Stream-based FIFO Lock

This module provides both synchronous and asynchronous lock-like classes that ensure strict FIFO ordering using Redis Streams.

## Features

- **Strict FIFO ordering**: Stream entry order defines who's next
- **Blocking**: Waiters block on BLPOP of their personal signal key; they wake only when dispatched
- **Handoff**: release() acks the previous holder's message and dispatches the next in a single step
- **Crash safety**: If a holder dies, its message stays pending. The next release() (or any call site) uses XAUTOCLAIM to take that stuck pending and re-signal the rightful owner
- **Fencing token**: The stream id (e.g., "1731294030000-0") is a natural, ever-increasing fencing token you can pass downstream if needed

## Usage

### Synchronous (StreamGate)

```python
import redis
from redis_fifo_lock import StreamGate

# Create Redis client
r = redis.Redis(host='localhost', port=6379, db=0)

# Create gate
gate = StreamGate(r)

# Using context manager (recommended)
with gate:
    # do your exclusive/critical work here
    print("I have the lock!")

# Or manual acquire/release
owner, msg_id = gate.acquire()
try:
    # do your exclusive/critical work here
    print("I have the lock!")
finally:
    gate.release(owner, msg_id)

# With timeout
try:
    owner, msg_id = gate.acquire(timeout=30)
    try:
        # do work
        pass
    finally:
        gate.release(owner, msg_id)
except TimeoutError:
    print("Timed out waiting for lock")
```

### Asynchronous (AsyncStreamGate)

```python
import asyncio
import redis.asyncio as redis
from redis_fifo_lock import AsyncStreamGate

async def worker(name: str, gate: AsyncStreamGate):
    async with await gate.session():   # blocks until it's your turn
        print(f"{name} running…")
        await asyncio.sleep(0.5)       # do exclusive work

async def main():
    r = redis.Redis(host="localhost", port=6379, db=0)
    gate = AsyncStreamGate(r)

    # launch a few contenders
    tasks = [asyncio.create_task(worker(f"W{i}", gate)) for i in range(5)]
    await asyncio.gather(*tasks)

asyncio.run(main())
```

### Manual async acquire/release

```python
import redis.asyncio as redis
from redis_fifo_lock import AsyncStreamGate

async def main():
    r = redis.Redis(host="localhost", port=6379, db=0)
    gate = AsyncStreamGate(r)

    owner, msg_id = await gate.acquire()
    try:
        # do your exclusive/critical work here
        print("I have the lock!")
    finally:
        await gate.release(owner, msg_id)

    # With timeout
    try:
        owner, msg_id = await gate.acquire(timeout=30)
        try:
            # do work
            pass
        finally:
            await gate.release(owner, msg_id)
    except asyncio.TimeoutError:
        print("Timed out waiting for lock")
```

## Configuration

Both `StreamGate` and `AsyncStreamGate` accept the following optional parameters:

- `stream` (str): Stream name for the gate (default: "gate:stream")
- `group` (str): Consumer group name (default: "gate:group")
- `adv_consumer` (str): Dispatcher/advancer consumer identity (auto-generated if None)
- `sig_prefix` (str): Prefix for per-waiter signal keys (default: "gate:sig:")
- `sig_ttl_ms` (int): TTL for signal keys in milliseconds (default: 300000 = 5 minutes)
- `claim_idle_ms` (int): Idle time before considering a holder dead (default: 60000 = 60 seconds)
- `last_key` (str): Key to store the last dispatched message ID (default: "gate:last-dispatched")

Example with custom configuration:

```python
gate = StreamGate(
    r,
    stream="my:custom:stream",
    group="my:custom:group",
    sig_ttl_ms=120000,  # 2 minutes
    claim_idle_ms=30000,  # 30 seconds
)
```

## Cancellation

If you want to cancel waiting before being dispatched:

```python
# Synchronous
owner, msg_id = gate.acquire(timeout=1)  # Use short timeout
# If timeout expires, TimeoutError is raised and cleanup is automatic

# Or manually cancel
owner, msg_id = gate.acquire()  # This would block, but in another context...
gate.cancel(owner, msg_id)  # ...you can cancel

# Asynchronous
await gate.cancel(owner, msg_id)
```

## Observability

You can use Redis commands to observe the state of the gate:

```bash
# View pending messages (active holders)
XPENDING gate:stream gate:group

# View stream entries
XRANGE gate:stream - +

# View consumer group info
XINFO GROUPS gate:stream
```

## How it Works

1. **Enqueue**: Each waiter enqueues a ticket using `XADD gate:stream * owner=<uuid>`
2. **Dispatch**: The current holder (on `release()`) advances the queue by:
   - Reading the next stream entry in order using `XREADGROUP`
   - Signaling that specific owner to proceed via `LPUSH` to their personal signal key
   - Keeping that entry pending (so if the owner dies, it can be re-delivered)
3. **Wait**: Waiters block on `BLPOP` of their personal signal key until dispatched
4. **Release**: When the owner eventually calls `release()`, we:
   - ACK the entry that represented their turn using `XACK`
   - Dispatch the next ticket using `XREADGROUP`
5. **Crash Recovery**: If a holder dies, their message stays pending. The next `release()` uses `XAUTOCLAIM` to detect and re-signal stuck holders after `claim_idle_ms` milliseconds

This keeps exactly one pending message representing the active holder.

## Development

```bash
uv sync --all-extras
uv run pytest
```
