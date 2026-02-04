# QuantLab Prod Runbook (Release Hardening)

## Start / Stop (systemd)
Use the unit files in `ops/systemd/` and `core/observer-api/` as the source of truth.

Examples:
- Start: `sudo systemctl start quantlab-replayd.service`
- Stop: `sudo systemctl stop quantlab-replayd.service`
- Restart: `sudo systemctl restart quantlab-replayd.service`
- Status: `sudo systemctl status quantlab-replayd.service`

Known unit files in repo:
- `ops/systemd/quantlab-replayd.service`
- `ops/systemd/quantlab-worker.service`
- `ops/systemd/quantlab-console-ui.service`
- `core/observer-api/quantlab-observer.service`
- `core/compressor/quantlab-compact.service`

## Health Checks
Observer API:
- `curl -s -H "Authorization: Bearer $OBSERVER_TOKEN" http://127.0.0.1:9150/observer/health`
- `curl -s -H "Authorization: Bearer $OBSERVER_TOKEN" http://127.0.0.1:9150/observer/runs`

Self-test (startup validation):
- `node core/release/ConfigCheck.js`
- `node core/release/SelfTest.js`

## Kill-Switch (Live Run Stop)
- `curl -s -X POST -H "Authorization: Bearer $OBSERVER_TOKEN" http://127.0.0.1:9150/observer/runs/<live_run_id>/stop`

## Canary Validation
- `node tools/run-canary-live.js --exchange binance --symbols BTCUSDT,ETHUSDT --strategy /path/to/strategy.js --dataset-parquet live --dataset-meta live`

Expected:
- PASS log from canary
- Archive files present in S3
- Audit entries present in local spool

## Most Common Failures (Top 5)
1) **S3 archive unreachable**
   - Symptom: archive writes fail or self-test fails on S3 check.
   - Fix: verify `RUN_ARCHIVE_S3_*` env, endpoint reachability, and bucket permissions.

2) **Observer API unauthorized (401)**
   - Symptom: health/stop endpoints return 401.
   - Fix: set `OBSERVER_TOKEN` and use `Authorization: Bearer <token>`.

3) **Live WS disabled**
   - Symptom: live run fails with `CORE_LIVE_WS_DISABLED`.
   - Fix: set `CORE_LIVE_WS_ENABLED=1`.

4) **Budget/guard stops**
   - Symptom: run stops with `BUDGET_EXCEEDED` or `PROMOTION_GUARD_FAIL`.
   - Fix: review guard/budget configs and audit trail in `/tmp/quantlab-audit/`.

5) **Archive missing after run**
   - Symptom: canary fails archive check.
   - Fix: confirm `RUN_ARCHIVE_ENABLED=1`, check S3 permissions, and verify RunArchiveWriter logs.

## Break Glass (Emergency)
- Stop live runs: `curl -s -X POST -H "Authorization: Bearer $OBSERVER_TOKEN" http://127.0.0.1:9150/observer/runs/<live_run_id>/stop`
- Rotate observer token:
  - Update env file with new `OBSERVER_TOKEN`
  - `sudo systemctl restart quantlab-observer.service`
- Disable live WS ingest if needed: set `CORE_LIVE_WS_ENABLED=0` and restart live runner service.

## Safety Defaults (systemd + limits)
- systemd recommendations:
  - `Restart=on-failure`
  - `RestartSec=5s`
  - `StartLimitIntervalSec=60s`
  - `StartLimitBurst=5`
  - `MemoryMax=2G`
  - `CPUQuota=80%`
- Conservative limits (defaults in env):
  - `RUN_BUDGET_MAX_RUN_SECONDS=3600`
  - `RUN_BUDGET_MAX_EVENTS=1000000`
  - `RUN_BUDGET_MAX_DECISIONS_PER_MIN=600`

