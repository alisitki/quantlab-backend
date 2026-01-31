# QuantLab Deterministic Ordering & Cursor

## Purpose
Guarantee stable, reproducible ordering and unambiguous resume semantics across collector/compact/replay.

## When to use
- Any logic involving ordering, sorting, merging, dedup
- Any cursor format, cursor comparisons, resume-from-cursor
- Any change involving ts_event / seq / partition boundaries

## Invariants (non-negotiable)
- Same inputs => same emitted sequence (byte-for-byte where applicable).
- Total order must be stable and explicit (define tie-breakers).
- Cursor is monotonic and is the only resume authority.
- No precision loss for timestamps: use string for ns if needed.

## Ordering rules (must be explicit in code/comments)
- Define primary key (e.g., ts_event_ns) and tie-breakers (e.g., seq, exchange id, file index, row index).
- Never rely on iteration order of maps/objects.
- If parallelism exists, merge must be stable (deterministic merge strategy).

## Cursor rules
- Cursor must encode: stream + date + symbol + position (and tie-breaker state if needed).
- Cursor comparisons must be deterministic and language-safe.
- Any cursor format change requires versioning plan.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS
   - determinism replay twice => same fingerprint
   - resume test: stop at cursor, continue => no dup/no gap
