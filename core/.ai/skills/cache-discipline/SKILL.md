# QuantLab Cache Discipline

## Purpose
Add caching for speed without changing correctness or determinism.

## When to use
- Replay Cache Layer (meta/files/page caches)
- Any in-memory/disk cache in services
- Any optimization that might bypass source reads

## Non-negotiable rules
- Cache must never be the source of truth.
- Cache must be safe on stale/mismatch: invalidate and fall back to source.
- Cache keys MUST include:
  - schema_version
  - partition identity (stream/date/symbol)
  - any manifest id / fingerprint seed if available
- Cache entries must have TTL or explicit invalidation strategy.
- Track cache_hit/cache_miss and evictions.

## Failure modes
- On decode error or corruption in cache: drop entry + source fallback.
- On size pressure: bounded LRU; never unbounded growth.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS
   - hit/miss visible in logs/metrics
   - mismatch invalidates cache and still returns correct data
