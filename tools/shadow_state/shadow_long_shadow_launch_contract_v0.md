# Long Shadow Launch Contract v0

This contract is operational only.

It does not:
- change ranking
- change selection semantics
- introduce real trading or OMS
- add daemonization or scheduler rollout
- change batch cadence or runtime policy

## Canonical Entrypoint

Tool:
- `tools/run-long-shadow-launch-v0.py`

This wrapper standardizes one bounded long-shadow profile on top of:
- `tools/run-shadow-observation-batch-v0.py`

## Fixed Launch Profile

Profile id:
- `top1_printheadtail_long_shadow_v0`

Selection:
- current watchlist top-1 only

Default strategy:
- `core/strategy/strategies/PrintHeadTailStrategy.js`

Default guards:
- `--per-run-timeout-sec 90`
- `--run-max-duration-sec 60`
- `--heartbeat-ms 5000`

This profile is bounded and finite.

It is not:
- an unattended forever-run
- a service
- a scheduler

## Canonical Command

```bash
timeout 150s python3 tools/run-long-shadow-launch-v0.py \
  --strategy core/strategy/strategies/PrintHeadTailStrategy.js \
  --audit-base-dir /tmp/quantlab-long-shadow-audit-v0 \
  --out-dir /tmp/quantlab-long-shadow-out-v0 \
  --batch-result-json tools/shadow_state/shadow_long_shadow_batch_result_v0.json \
  --launch-result-json tools/shadow_state/shadow_long_shadow_launch_v0.json
```

## Required Artifacts After A Real Run

The launch is evaluated against these artifacts:
- batch result JSON
- canonical refresh result JSON
- per-run summary JSON
- per-run stdout log
- per-run stderr log
- per-run audit spool dir
- operator snapshot JSON
- execution review queue JSON
- execution events JSONL
- trade ledger JSONL

Zero execution events or zero trades are allowed.
Missing execution-events or trade-ledger files are not allowed.

## Health And Validity Rules

`DRY_RUN_ONLY`:
- wrapper invoked with `--dry-run`
- not counted as a valid run

`INVALID`:
- batch exit non-zero
- batch result missing or malformed
- `attempted_count != 1`
- `completed_count != 1`
- `refresh_executed != true`
- `refresh_exit_code != 0`
- `surfaces_synced != true`
- `execution_artifacts_synced != true`
- per-item `run_exit_code != 0`
- per-item `verify_soft_live_pass != true`
- per-item `summary_generated != true`
- per-item `history_updated != true`
- per-item `note != ""`
- summary/log/audit artifacts missing
- summary `heartbeat_seen != true`
- summary `processed_event_count <= 0`
- operator snapshot missing
- execution review queue missing
- execution events JSONL missing
- trade ledger JSONL missing

`VALID_NO_EXECUTION_ACTIVITY`:
- all validity checks pass
- matched execution-event count = 0
- matched trade count = 0

`VALID_WITH_EXECUTION_ACTIVITY`:
- all validity checks pass
- matched execution-event count > 0 or matched trade count > 0

## Matching Rules

The wrapper reads the selected pack and run identity from:
- batch result item `pack_id`
- summary JSON `live_run_id`

Matched execution-event count:
- `selected_pack_id == pack_id`
- `live_run_id == live_run_id`

Matched trade count:
- `selected_pack_id == pack_id`
- `open_live_run_id == live_run_id` or `last_live_run_id == live_run_id`

## Primary Output

Primary launch artifact:
- `tools/shadow_state/shadow_long_shadow_launch_v0.json`

Key fields:
- `launch_status`
- `valid_run`
- `invalid_reason`
- `selected_pack_id`
- `selected_live_run_id`
- `summary_heartbeat_seen`
- `summary_processed_event_count`
- `matched_execution_event_count`
- `matched_trade_count`
- `required_artifacts_ok`
