# redis-fifo-lock

A distributed FIFO mutex on Redis. Callers are served strictly in the order
Redis saw them arrive, and a holder whose process dies is detected and replaced.

```python
from redis_fifo_lock import FifoLock

lock = FifoLock(redis_client, "invoices")

async with lock.hold() as lease:
    # exactly one caller is here at a time
    await do_work(fence=lease.fence)
```

Or acquire and release by hand, when the two happen in different places:

```python
lease = await lock.acquire(timeout=30)  # asyncio.TimeoutError if it runs out
try:
    ...
finally:
    await lock.release(lease)  # raises LeaseLost if we no longer held it
```

## What it guarantees

- **Mutual exclusion** — at most one caller inside the critical section, as long
  as the holder's process can run a coroutine at least once per `lease_ms` and
  reach Redis.
- **Strict FIFO by arrival at Redis.** Not by the order callers started: that is
  not knowable from the server and never can be.
- **Crash recovery within `lease_ms`, independent of how long the lock is held.**
  A ten-minute critical section and a one-minute crash-detection window coexist,
  because a live holder renews its own lease.
- **Idempotence.** A retried acquire keeps its place in the queue rather than
  going to the back; a retried release does not report a false alarm.
- **Loud failure.** `release()` raises `LeaseLost` if the lock was no longer
  ours, and `lease.lost` is set the moment a renewal is refused — so a holder
  can find out *during* the critical section, not after.

## What it refuses to promise

- **Exclusion under network partition, `SIGSTOP`, VM pause, or any freeze longer
  than `lease_ms`.** The holder keeps running while its lease expires underneath
  it. **No lease-based lock solves this** — not this one, not Redlock, not any
  other. The only real defence is to pass `lease.fence` to whatever you are
  protecting and have *that* reject stale tokens. This library cannot enforce
  that for you, and does not pretend to.
- **Anything across a Redis failover that loses writes**, including FIFO order.
  If you need that, you need consensus — etcd or ZooKeeper.
- **Bounding a holder that is alive but stuck.** It is indistinguishable from
  honest slow work. `max_hold_ms` caps how long the lock can be renewed, so a
  caller that vanishes cannot wedge it forever, but that is a backstop and not a
  guarantee about live holders.
- **Redis Cluster.** All keys share a hash tag so they land in one slot, but the
  scripts write a key computed from arguments rather than declared up front,
  which is formally undefined. Untested, unadvertised.

## Configuration

```python
FifoLock(
    redis_client,
    name,  # required — no shared default
    lease_ms=60_000,
    poll_ms=30_000,
    max_hold_ms=1_800_000,
)
```

`name` is required deliberately. A shared default means two unrelated services
on one Redis silently share a lock.

**`lease_ms`** is how long a grant survives without renewal. It bounds crash
detection, **not** how long you may hold the lock — but it must exceed the
longest stretch during which your holder cannot run a coroutine: a blocking
call, a GC pause, CPU throttling. This is the one setting whose value can cost
you correctness. If the renewal task is ever starved, the library logs a warning
naming the lock; treat that as a signal to raise this or to stop blocking the
loop.

**`poll_ms`** is only latency. A waiter is normally woken immediately; this is
the backstop for a wake-up that was lost. Raising it cuts steady-state load
roughly linearly — at 10,000 waiters, 2.5s costs about 4,000 script calls a
second where 30s costs about 330.

**`max_hold_ms`** caps total renewal. It exists for the caller that disappears
without its process dying — an abandoned job, an exception between acquire and
release. Set it above your longest legitimate critical section. Without it, one
abandoned caller holds the lock until the process restarts.

## Requirements

- **Redis must not evict the lock's keys.** The lease key carries a TTL, which
  makes it a *preferred* victim under any `volatile-*` policy — and evicting it
  silently frees a held lock while leaving the queue intact, so nothing looks
  wrong. Amazon ElastiCache ships `volatile-lru` by default, where Redis itself
  defaults to `noeviction`. The library checks on first use and refuses to run
  if the policy is unsafe; if it cannot read the policy, it warns instead. Give
  the lock a Redis set to `noeviction`, or one of its own.
- **Budget a connection per waiter.** A waiter holds one pooled connection while
  it blocks, and redis-py's default pool is 100 — the 101st concurrent waiter in
  a process fails with `MaxConnectionsError`, not a timeout. Raise
  `max_connections`, or cap concurrency.
- **Do not share an async client across event loops.** A client cached in a
  process global outlives the loop its connections were made on, and the next
  loop gets `RuntimeError: Event loop is closed`. This bites anything using
  `async_to_sync` (Celery, Django management commands): build the client per
  loop.

## How it works

Five keys, all sharing one hash tag: a sorted set holding the queue, a string
whose *existence* is the lock, two counters, and a record of the last release.

Every state transition is a single Lua script, so the decision and the act
cannot be separated by a round trip. Acquiring enqueues and grants in one call;
releasing frees and hands off in one call. That is the whole design, and it is
the fix for the defect that motivated the rewrite: the previous implementation
read the queue in one round trip and dispatched in another, and concurrent
callers slipped into the gap.

A waiter is woken by a doorbell, but the doorbell is only an optimisation — the
poll is authoritative, and the lock is correct with the doorbell removed
entirely. A holder renews its own lease in the background, which is what lets
crash detection be fast without capping how long you may legitimately hold the
lock.

## Testing

```bash
uv run --all-extras pytest                  # fast, no Redis, ~0.1s
uv run --all-extras pytest -m adversarial   # real Redis, randomized, ~2.5min
```

The adversarial suite is the real coverage. It attacks mutual exclusion and FIFO
ordering directly — a shared record of who is inside the critical section, never
a bookkeeping proxy — with long holds, crashes, cancellations, duplicate
releases and retry storms.

Its predecessor shipped a mutual-exclusion bug that survived eight months and 88
green tests, because every concurrency test held the lock for 100ms against a
5000ms wake interval, so no waiter ever woke while a holder was inside. **Every
test here was demonstrated failing against that implementation before it was
trusted.** A test that cannot fail is not evidence.
