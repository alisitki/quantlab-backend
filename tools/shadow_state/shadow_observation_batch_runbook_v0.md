# Shadow Observation Batch Runbook v0

This runbook is informational and operational only.

It does not:
- change ranking
- change selection
- suppress or skip watchlist items
- start any infinite loop or daemon
- replace scheduler/systemd/cron
- change shadow runtime policy

It standardizes safe, bounded usage of:
- `tools/run-shadow-observation-batch-v0.py`

## Canonical Tool

Tool:
- `tools/run-shadow-observation-batch-v0.py`

The tool consumes the current watchlist in its existing order and runs a finite, bounded observe-only batch.

It reuses the existing chain:
1. `tools/run-shadow-watchlist-v0.js`
2. `tools/verify-soft-live.js`
3. `tools/shadow_observation_summary_v0.py`
4. `tools/shadow_observation_history_v0.py`

Primary result artifact:
- `tools/shadow_state/shadow_observation_batch_result_v0.json`

## Required Operator Input

Required:
- `--strategy`

Usually use:
- `core/strategy/strategies/PrintHeadTailStrategy.js`

Common bounded controls:
- `--max-items`
- `--per-run-timeout-sec`
- `--run-max-duration-sec`
- `--audit-base-dir`
- `--out-dir`
- `--result-json`

Optional preview mode:
- `--dry-run`

## Invocation Profiles

### 1. Dry-Run Preview

Intent:
- show which watchlist items would be consumed
- show deterministic result artifact structure
- do not spawn shadow runs

Command:

```bash
python3 tools/run-shadow-observation-batch-v0.py \
  --dry-run \
  --max-items 3 \
  --strategy core/strategy/strategies/PrintHeadTailStrategy.js \
  --audit-base-dir /tmp/quantlab-shadow-batch-audit-dryrun \
  --out-dir /tmp/quantlab-shadow-batch-out-dryrun \
  --result-json /tmp/quantlab-shadow_observation_batch_result_dryrun.json
```

Prefer when:
- checking the top of the current watchlist
- validating command parameters before a real bounded run

Expected result:
- `attempted_count` reflects selected top-N
- `completed_count=0`
- `dry_run=true`
- each item has `run_executed=false`

### 2. Top-1 Bounded Real Run

Intent:
- observe only the top watchlist item
- smallest real operational batch

Command:

```bash
timeout 90s python3 tools/run-shadow-observation-batch-v0.py \
  --max-items 1 \
  --strategy core/strategy/strategies/PrintHeadTailStrategy.js \
  --per-run-timeout-sec 45 \
  --run-max-duration-sec 20 \
  --audit-base-dir /tmp/quantlab-shadow-batch-audit-real \
  --out-dir /tmp/quantlab-shadow-batch-out-real \
  --result-json tools/shadow_state/shadow_observation_batch_result_v0.json
```

Prefer when:
- smoke-checking the current top candidate
- validating end-to-end observe-only wiring

Expected result:
- `attempted_count=1`
- `completed_count` is `1` only if wrapper, verify, summary, and history all succeed

### 3. Top-3 Bounded Real Run

Intent:
- finite multi-item observe-only batch
- closest safe step toward repeated shadow usage without any daemon/service behavior

Command:

```bash
timeout 240s python3 tools/run-shadow-observation-batch-v0.py \
  --max-items 3 \
  --strategy core/strategy/strategies/PrintHeadTailStrategy.js \
  --per-run-timeout-sec 45 \
  --run-max-duration-sec 20 \
  --audit-base-dir /tmp/quantlab-shadow-batch-audit-top3 \
  --out-dir /tmp/quantlab-shadow-batch-out-top3 \
  --result-json tools/shadow_state/shadow_observation_batch_result_v0.json
```

Prefer when:
- you want a small manual batch over the current watchlist head
- you still want bounded, finite behavior with no scheduling layer

Expected result:
- `attempted_count<=3`
- each item produces its own stdout/stderr/audit path under the configured output roots

## Result Reading Guide

Open first:
- `tools/shadow_state/shadow_observation_batch_result_v0.json`

Read in this order:
1. `attempted_count`
2. `completed_count`
3. each result's `run_exit_code`
4. each result's `verify_soft_live_pass`
5. each result's `summary_generated`
6. each result's `history_updated`
7. each result's `note`

Interpretation:
- `run_exit_code=0` means wrapper completed
- `verify_soft_live_pass=true` means `verify-soft-live.js` passed
- `summary_generated=true` means post-run summary was written
- `history_updated=true` means history/index accepted the new summary
- non-empty `note` is the shortest failure/localization hint

## Failure Triage Order

If an item did not complete cleanly, inspect in this order:

1. batch result JSON
   - check `run_exit_code`, `verify_soft_live_pass`, `summary_generated`, `history_updated`, `note`
2. per-item stderr log
   - path from `stderr_log_path`
3. per-item stdout log
   - path from `stdout_log_path`
4. verify outcome
   - re-run `verify_command` from the result entry if needed
5. summary output
   - inspect `summary_json_path`
6. history/index state
   - inspect:
     - `tools/shadow_state/shadow_observation_history_v0.jsonl`
     - `tools/shadow_state/shadow_observation_index_v0.json`

Short reading rule:
- wrapper failure -> start with stderr log
- verify failure -> inspect audit dir and `/tmp/quantlab-soft-live.json`
- summary/history failure -> inspect the recorded command and the referenced artifact paths

## Boundedness Guard

This routine is finite by design.

Bounded behavior comes from:
- `--max-items`
- `--per-run-timeout-sec`
- `--run-max-duration-sec`
- optional outer `timeout ...`

It must not be treated as:
- a continuous service
- a scheduler
- a daemon
- an auto-reobserve loop

## Minimal Operator Checklist

1. Start with dry-run preview.
2. Use top-1 bounded run first.
3. Read the batch result JSON before anything else.
4. Only use top-3 after top-1 behavior is understood.
5. If a result item has a failure note, triage in the documented order above.
