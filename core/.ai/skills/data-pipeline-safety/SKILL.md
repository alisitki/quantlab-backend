# QuantLab Data Pipeline Safety

## Core rules
- Pipelines must be idempotent.
- Validate schema before processing.
- Log row counts.
- Corrupt data goes to quarantine.
- Fingerprint outputs when possible.

## Never
- Assume file integrity
- Skip validation for speed
