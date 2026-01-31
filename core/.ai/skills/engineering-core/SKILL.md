# QuantLab Engineering Core

## Purpose
Standardize how code is written in QuantLab.

## Always do
- Start with PLAN before code.
- Preserve determinism (ordering, cursors, idempotency).
- Minimal patch, no random refactors.
- Add validation at IO boundaries.
- Add logs: start, end, counts, durations.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS

## Never
- Break ordering rules
- Add endpoints without auth or rate limit
- Silent failure
