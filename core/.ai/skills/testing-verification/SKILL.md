# QuantLab Testing & Verification Discipline

## Purpose
Ensure changes are verified with minimal, high-signal checks (determinism, resume, failure modes).

## When to use
- Any change in replay/compact/collector logic
- Any cursor/order/schema change
- Any cache/stream/backpressure work
- Any control/run lifecycle change

## Required verification set (choose minimal that applies)
A) Determinism check (data-path changes)
- Run the same partition twice => identical fingerprint (or identical emitted sequence)

B) Resume correctness (stream changes)
- Start streaming, capture lastCursor, reconnect from it => no duplicates, no gaps

C) Failure-mode check (robustness changes)
- Simulate: corrupt file OR missing meta OR cache mismatch
- Expected: explicit log + safe fallback + no silent data loss

D) Security check (endpoint changes)
- Unauthorized request fails
- Authorized request succeeds
- Rate limit behaves

## Output format
- VERIFY STEPS must include commands and expected signals:
  - curl commands
  - log grep / journalctl filters
  - expected counters (hit/miss) where relevant

## Never
- Declare "done" without at least one applicable verification step
