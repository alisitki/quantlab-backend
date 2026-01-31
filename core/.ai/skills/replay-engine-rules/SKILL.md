# QuantLab Replay Engine Rules

## Purpose
Production-grade replay behavior: deterministic event ordering, cursor semantics, stream correctness, and safe performance (cache is optional, never changes correctness).

## When to use
- Any work under core/replay/*
- replayd endpoints (/stream, /runs, /control related)
- Cursor format changes, ordering logic, pagination, cache layer, SSE reconnection behavior

## Non-negotiable invariants
- Determinism: same inputs => same output sequence.
- Ordering: stable total order per stream/partition (no "almost sorted").
- Cursor: monotonic progression; cursor is the single source of truth for resuming.
- Idempotency: re-reading a cursor range must not duplicate or skip events.
- Safety: corrupt/invalid data must be quarantined or skipped with explicit logging (never silent).

## Cursor rules
- Cursor must be JSON-safe and stable across languages.
- High-precision timestamps MUST NOT lose precision (use string for ns if needed).
- Cursor includes enough info to resume without ambiguity (stream/date/symbol + position).
- Cursor comparisons must be well-defined and consistent.

## Streaming rules (SSE / long-poll)
- Support resume-from-cursor. Never reset to start unless explicitly requested.
- On disconnect: exponential backoff reconnect and continue from lastCursor.
- Heartbeat/keepalive is allowed, but must never affect ordering.
- Backpressure: apply bounded buffering; drop strategy is NOT allowed unless explicitly designed.

## Read path rules (Parquet/DuckDB)
- Validate schema_version before processing.
- Validate row counts and log them per batch/page.
- Never assume file integrity: handle corrupted parquet/snappy by skipping with quarantine signal and log.
- Prefer minimal reads: page by cursor window, not full scans.

## Cache layer rules (optional)
- Cache only accelerates; correctness must not depend on cache.
- Cache keys must include schema_version and any fingerprint seed / manifest id.
- On mismatch: invalidate cache and fall back to source read.
- Track cache hit/miss metrics.

## Output format required
1) PLAN (bulleted)
2) FILE PATCH LIST (paths + what changes)
3) VERIFY STEPS (commands + expected signals)
   - Include at least: deterministic replay check, cursor resume check, and one failure-mode check.

## Never do
- Change event shape without versioning plan
- Introduce non-deterministic iteration (unordered maps, parallel merge without stable sort)
- “Fix” performance by skipping validation
