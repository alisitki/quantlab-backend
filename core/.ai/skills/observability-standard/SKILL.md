# QuantLab Observability Standard

## Purpose
Make every service and pipeline debuggable: consistent structured logs, minimal metrics, and run correlation.

## When to use
- New/modified services, endpoints, long-running jobs
- Replay/compact/collector changes
- Cache layers, performance work, reconnect logic

## Logging rules (required)
- Use structured logs where possible (JSON or key=value).
- Every meaningful operation logs:
  - run_id (or request_id)
  - component (collector/compact/replayd/strategyd/ui)
  - stream, date, symbol (when applicable)
  - action (start/end)
  - counts (rows/files/batches)
  - duration_ms
- Errors must include: error_code (if any), message, and context fields.

## Metrics rules (minimum)
Track at least:
- request_count, error_count
- latency_ms (p50/p95 if available; otherwise raw duration logs)
- For replay/cache:
  - cache_hit, cache_miss
  - page_size, rows_emitted
  - s3_get_count (or storage ops count if tracked)

## Correlation
- Propagate run_id/request_id across internal calls where possible.
- For SSE streams: include a stable stream_id and lastCursor in logs.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS (how to see logs/metrics, expected fields)
