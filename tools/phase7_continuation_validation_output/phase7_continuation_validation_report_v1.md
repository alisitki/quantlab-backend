# Phase7 Continuation Validation Report
Generated from:
- `/home/deploy/quantlab-backend/tools/phase7_medium_shadow_result_v0.json`
- `/home/deploy/quantlab-backend/tools/phase7_medium_shadow_validation_output/phase7_medium_shadow_report_v0.md`
- `['linkusdt', 'avaxusdt']`
## Summary
- `target_count=2`
- `lane_result=POSITIVE_LANE`
- `final_recommendation=BOTH_ADVANCE`
- verdicts:
- `KEEP_ADVANCING=2`
- `WEAK_CONTINUE=0`
- `DROP=0`
- `INVALID_RUN=0`
## Per-Strategy Results
### `linkusdt`
- `verdict=KEEP_ADVANCING`
- `reason=fill_count > 0 with bounded churn`
- `processed_event_count=174256`
- `decision_count=41`
- `fill_count=41`
- `open_count=0`
- `exit_count=1`
- `trade_transitions_per_1k_events=0.005739`
- `run_duration_sec=86401.309`
- `stop_reason=STREAM_END`
- `cost_proxy=NET_AFTER_FEES_AND_FUNDING`
### `avaxusdt`
- `verdict=KEEP_ADVANCING`
- `reason=fill_count > 0 with bounded churn`
- `processed_event_count=171148`
- `decision_count=118`
- `fill_count=118`
- `open_count=0`
- `exit_count=1`
- `trade_transitions_per_1k_events=0.005843`
- `run_duration_sec=86401.875`
- `stop_reason=STREAM_END`
- `cost_proxy=NET_AFTER_FEES_AND_FUNDING`
## Pairwise Comparison
- `stronger_candidate=avaxusdt`
- `weaker_candidate=linkusdt`
- `both_survive=True`
- `only_one_survives=False`
- `final_recommendation=BOTH_ADVANCE`
## Next Phase
- Continue only the surviving pair decision from this artifact.
- Do not mutate ranking, promotion, or the global watchlist from this sprint alone.
