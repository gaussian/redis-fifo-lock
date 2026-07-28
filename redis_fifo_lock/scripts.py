"""
The protocol, as three Lua scripts.

Every state transition is one script, so the decision and the act cannot be
separated by a round trip. That is not an optimisation — it is the fix. The
previous design read the queue in one round trip and dispatched in another, and
concurrent callers slipped into the gap: six advancers racing on an empty queue
dispatched six holders, where the fused version dispatches one.

KEYS are the same five everywhere:
    1 {name}:q     ZSET   member = owner, score = pos. The queue.
    2 {name}:h     STRING "owner:fence", PX = lease_ms. Existence IS the lock.
    3 {name}:seq   STRING INCR -> pos. Queue order only, never a token.
    4 {name}:fence STRING INCR -> fence. Minted only when the lock is granted.
    5 {name}:last  STRING the last fence released, so a retried release is not
                          mistaken for a lost lease.

ARGV[1..3] are always prefix, owner, lease_ms. ARGV[4] varies by script.
"""

# Owner is uuid4().hex, so it never contains ':' and the split is unambiguous.
_PRELUDE = """
local KQ, KH, KSEQ, KF, KLAST = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local PFX, OWNER, LEASE = ARGV[1], ARGV[2], tonumber(ARGV[3])

local function sig(t) return PFX .. 's:' .. t end

local function held_by(cur, who)
  return cur and cur ~= false
     and string.sub(cur, 1, string.len(who) + 1) == who .. ':'
end

local function fence_of(cur)
  return tonumber(string.sub(cur, string.find(cur, ':') + 1))
end

-- Grant the lock to the head of the queue, and only if it is genuinely free.
-- The EXISTS guard and the SET live in the same script, so no two callers can
-- both observe "free" and both grant.
local function advance()
  if redis.call('EXISTS', KH) == 1 then return end
  local head = redis.call('ZRANGE', KQ, 0, 0)
  if #head == 0 then return end
  local tok = head[1]
  local fence = redis.call('INCR', KF)
  redis.call('ZREM', KQ, tok)
  redis.call('SET', KH, tok .. ':' .. fence, 'PX', LEASE)
  -- The doorbell is advisory: it saves the winner a poll, and losing it costs
  -- latency only, never correctness. The poll below is what is authoritative.
  redis.call('LPUSH', sig(tok), '1')
  redis.call('PEXPIRE', sig(tok), LEASE)
end
"""

# ARGV[4] = the caller's known pos, or '' if it has never been queued.
#
# Enqueue and poll are the same call, which is what makes it idempotent: a
# retried first attempt finds itself already in the ZSET and keeps its original
# position rather than being re-scored to the back of the queue.
ACQUIRE = (
    _PRELUDE
    + """
local cur = redis.call('GET', KH)
if held_by(cur, OWNER) then
  -- A retry of a call that already granted us the lock.
  local p = redis.call('ZSCORE', KQ, OWNER)
  redis.call('ZREM', KQ, OWNER)
  redis.call('DEL', sig(OWNER))
  return {'HELD', tostring(fence_of(cur)), p and tostring(math.floor(p)) or ARGV[4]}
end

local pos = redis.call('ZSCORE', KQ, OWNER)
if not pos then
  if ARGV[4] ~= '' then pos = tonumber(ARGV[4]) else pos = redis.call('INCR', KSEQ) end
  redis.call('ZADD', KQ, pos, OWNER)
else
  pos = tonumber(pos)
end

advance()

cur = redis.call('GET', KH)
if held_by(cur, OWNER) then
  redis.call('DEL', sig(OWNER))
  return {'HELD', tostring(fence_of(cur)), tostring(math.floor(pos))}
end
return {'WAIT', '', tostring(math.floor(pos))}
"""
)

# ARGV[4] = the fence being released, or '' to abandon a queued ticket.
#
# Releasing compares the whole "owner:fence" value, so a caller whose lease
# expired cannot free its successor's lock. Abandoning refuses to free a lease
# the caller turns out to be holding — otherwise cancel() would be a public,
# fence-less force-release of a live critical section.
RELEASE = (
    _PRELUDE
    + """
local cur = redis.call('GET', KH)
local rc, extra = 'OK', ''

if ARGV[4] ~= '' then
  if held_by(cur, OWNER) and fence_of(cur) == tonumber(ARGV[4]) then
    redis.call('DEL', KH)
    redis.call('SET', KLAST, ARGV[4], 'PX', LEASE * 10)
    rc = 'OK'
  elseif redis.call('GET', KLAST) == ARGV[4] then
    -- We already released this exact lease; this is a redelivered command, not
    -- evidence that somebody else ran alongside us.
    rc = 'NOOP'
  else
    rc = 'STALE'
  end
elseif held_by(cur, OWNER) then
  -- Granted while we were giving up. Report the fence so the caller can
  -- release it properly instead of us silently dropping a held lock.
  rc, extra = 'HELD', tostring(fence_of(cur))
end

redis.call('ZREM', KQ, OWNER)
redis.call('DEL', sig(OWNER))
if rc ~= 'HELD' then advance() end
return {rc, extra}
"""
)

# ARGV[4] = the fence being renewed.
#
# PEXPIRE only ever extends an existing key. A heartbeat can therefore never
# resurrect a lease that already expired and was handed to somebody else --
# which is the single line that stops a slow holder from stealing the lock back.
BEAT = (
    _PRELUDE
    + """
local cur = redis.call('GET', KH)
if held_by(cur, OWNER) and fence_of(cur) == tonumber(ARGV[4]) then
  redis.call('PEXPIRE', KH, LEASE)
  return 1
end
return 0
"""
)
