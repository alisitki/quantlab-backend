# Phase7 Continuation Validation Report

Generated from:
- `/home/deploy/quantlab-backend/tools/phase7_medium_shadow_result_v0.json`
- `/home/deploy/quantlab-backend/tools/phase7_medium_shadow_validation_output/phase7_medium_shadow_report_v0.md`
- `['linkusdt', 'avaxusdt']`

## Summary

- `target_count=2`
- `lane_result=DRY_RUN_ONLY`
- `final_recommendation=NEITHER_ADVANCE`
- verdicts:
- `KEEP_ADVANCING=0`
- `WEAK_CONTINUE=0`
- `DROP=2`
- `INVALID_RUN=0`

## Per-Strategy Results

### `linkusdt`
- `verdict=DROP`
- `reason=dry_run only`
- `processed_event_count=0`
- `decision_count=0`
- `fill_count=0`
- `open_count=0`
- `exit_count=0`
- `trade_transitions_per_1k_events=None`
- `run_duration_sec=None`
- `stop_reason=DRY_RUN`
- `cost_proxy=not available in current artifact surface`

### `avaxusdt`
- `verdict=DROP`
- `reason=dry_run only`
- `processed_event_count=0`
- `decision_count=0`
- `fill_count=0`
- `open_count=0`
- `exit_count=0`
- `trade_transitions_per_1k_events=None`
- `run_duration_sec=None`
- `stop_reason=DRY_RUN`
- `cost_proxy=not available in current artifact surface`

## Pairwise Comparison

- `stronger_candidate=None`
- `weaker_candidate=None`
- `both_survive=False`
- `only_one_survives=False`
- `final_recommendation=NEITHER_ADVANCE`

## Next Phase

- Continue only the surviving pair decision from this artifact.
- Do not mutate ranking, promotion, or the global watchlist from this sprint alone.

