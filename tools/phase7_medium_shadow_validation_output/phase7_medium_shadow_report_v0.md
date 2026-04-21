# Phase7 Medium-Frequency Shadow Validation Report

Generated from:
- `tools/phase7_medium_shadow_result_v0.json`
- `tools/phase7_medium_shadow_validation_output/full_run`

Result timestamp:
- `2026-04-04T23:52:34Z`

## Scope

This run evaluated only the `MEDIUM_FREQUENCY` Phase7 lane:
- `xrpusdt`
- `linkusdt`
- `ethusdt`
- `avaxusdt`

Execution posture:
- isolated task-local shadow state
- isolated task-local watchlists
- no ranking change
- no promotion state change
- no global shadow watchlist change
- no strategy logic change

Run policy:
- `run_semantics=ISOLATED_PAPER_DIRECTIONAL_SHADOW`
- `max_parallel=4`
- `run_max_duration_sec=43200`
- `per_run_timeout_sec=43500`
- `subprocess_timeout_sec=44400`
- `heartbeat_ms=5000`
- `max_continuation=2`

## Summary

- `target_count=4`
- `continuation_count=2`
- `lane_result=POSITIVE_LANE`
- verdicts:
- `CONTINUE=4`
- `WEAK=0`
- `NO_SIGNAL=0`
- `FAIL_CHURN=0`
- `INVALID_RUN=0`

All four strategies completed a full ~12 hour window with:
- `verify_soft_live_pass=true`
- `stop_reason=STREAM_END`
- bounded churn
- non-zero decisions
- non-zero fills

## Per-Strategy Results

### `xrpusdt`
- `status=OK`
- `verdict=CONTINUE`
- `processed_event_count=99294`
- `decision_count=6`
- `fill_count=6`
- `open_count=0`
- `exit_count=1`
- `reversal_count=null`
- `trade_transitions_per_1k_events=0.010071`
- `run_duration_sec=43201.349`
- `eliminated=true`
- `elimination_reason=lower_priority_than_top2`

Artifacts:
- `tools/phase7_medium_shadow_validation_output/full_run/runs/01_xrpusdt/summary_runtime.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/01_xrpusdt/shadow_observation_batch_result_v0.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/01_xrpusdt/shadow_state/shadow_execution_events_v1.jsonl`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/01_xrpusdt/shadow_state/shadow_trade_ledger_v1.jsonl`

### `linkusdt`
- `status=OK`
- `verdict=CONTINUE`
- `processed_event_count=103938`
- `decision_count=10`
- `fill_count=10`
- `open_count=0`
- `exit_count=1`
- `reversal_count=null`
- `trade_transitions_per_1k_events=0.009621`
- `run_duration_sec=43201.259`
- `eliminated=false`

Artifacts:
- `tools/phase7_medium_shadow_validation_output/full_run/runs/02_linkusdt/summary_runtime.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/02_linkusdt/shadow_observation_batch_result_v0.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/02_linkusdt/shadow_state/shadow_execution_events_v1.jsonl`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/02_linkusdt/shadow_state/shadow_trade_ledger_v1.jsonl`

### `ethusdt`
- `status=OK`
- `verdict=CONTINUE`
- `processed_event_count=97742`
- `decision_count=10`
- `fill_count=10`
- `open_count=0`
- `exit_count=1`
- `reversal_count=null`
- `trade_transitions_per_1k_events=0.010231`
- `run_duration_sec=43200.911`
- `eliminated=true`
- `elimination_reason=lower_priority_than_top2`

Artifacts:
- `tools/phase7_medium_shadow_validation_output/full_run/runs/03_ethusdt/summary_runtime.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/03_ethusdt/shadow_observation_batch_result_v0.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/03_ethusdt/shadow_state/shadow_execution_events_v1.jsonl`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/03_ethusdt/shadow_state/shadow_trade_ledger_v1.jsonl`

### `avaxusdt`
- `status=OK`
- `verdict=CONTINUE`
- `processed_event_count=99918`
- `decision_count=40`
- `fill_count=40`
- `open_count=0`
- `exit_count=1`
- `reversal_count=null`
- `trade_transitions_per_1k_events=0.010008`
- `run_duration_sec=43201.118`
- `eliminated=false`

Artifacts:
- `tools/phase7_medium_shadow_validation_output/full_run/runs/04_avaxusdt/summary_runtime.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/04_avaxusdt/shadow_observation_batch_result_v0.json`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/04_avaxusdt/shadow_state/shadow_execution_events_v1.jsonl`
- `tools/phase7_medium_shadow_validation_output/full_run/runs/04_avaxusdt/shadow_state/shadow_trade_ledger_v1.jsonl`

## Continuation Decision

Selected continuation set:
- `linkusdt`
- `avaxusdt`

Reasoning:
- `avaxusdt` was the strongest row by activity with `40` decisions and `40` fills at bounded churn.
- `linkusdt` and `ethusdt` both finished at `10` decisions and `10` fills with zero risk rejects; `linkusdt` won the tie on lower `trade_transitions_per_1k_events` (`0.009621` vs `0.010231`).
- `xrpusdt` remained valid and fill-backed, but was weaker on fill and decision count than the selected pair.

Note:
- `continuation_candidates` in the JSON artifact are rank-sorted for readability, not strength-sorted.

## Conclusion

The medium-frequency lane passed.

Operational conclusion:
- the 12-hour window was sufficient to produce real fill-backed shadow evidence
- no strategy failed infra
- no strategy stayed flat
- two candidates are worth continuing: `linkusdt` and `avaxusdt`

No global verdict beyond this lane is implied by this report.
