# Shadow Combined Operator Reading Contract v0

This contract is informational only.

It does not:
- change ranking
- change selection
- trigger trades
- auto-run anything
- replace cadence, scheduler, or batch policy
- replace deeper observation analysis
- replace deeper execution or PnL analysis

Use it to read the current combined observation + execution/PnL + outcome-review surfaces faster.

## Authoritative Surfaces

Observation-aware review/watchlist surface:
- `tools/phase6_state/candidate_review.tsv`
- `tools/phase6_state/candidate_review.json`
- `tools/shadow_state/shadow_watchlist_v0.tsv`
- `tools/shadow_state/shadow_watchlist_v0.json`

Execution/PnL latest snapshot surface:
- `tools/shadow_state/shadow_execution_pack_summary_v0.json`

Outcome review surface:
- `tools/shadow_state/shadow_execution_outcome_review_v0.json`

Observation reading contract:
- `tools/shadow_state/shadow_operator_reading_contract_v0.md`

Execution/PnL reading contract:
- `tools/shadow_state/shadow_execution_reading_contract_v0.md`

Outcome review reading contract:
- `tools/shadow_state/shadow_execution_outcome_review_reading_contract_v0.md`

## Reading Order

Read in this order:

1. `decision_tier`, `score`, `context_flags`
2. Observation layer:
   - `observation_status`
   - `next_action_hint`
   - `reobserve_status`
   - `observation_last_outcome_short`
   - `observation_attention_flag`
3. Execution/PnL layer:
   - `pnl_interpretation`
   - `pnl_attention_flag`
   - `last_pnl_state`
   - `latest_unrealized_sign`, `last_total_unrealized_pnl`
   - `latest_realized_sign`, `last_total_realized_pnl`
4. Outcome review layer:
   - `outcome_class`
   - `outcome_attention_flag`
   - `latest_vs_recent_consistency`
   - `outcome_review_short`
5. Recency and short history:
   - `last_observation_age_hours`
   - `observation_recency_bucket`
   - `recent_observation_trail`
6. Only if needed:
   - `last_positions_count`
   - `last_fills_count`
   - `last_snapshot_present`
   - `last_equity`
   - `last_max_position_value`

Why this order:
- first read candidate quality and tier
- then read the latest observation state
- then read the latest execution/PnL state
- then read the compact outcome review
- then use recency and trail as context
- only then inspect raw size/equity details

## Combined Interpretation Patterns

1. `NEW` + `READY_TO_OBSERVE` + `NOT_OBSERVED` + execution fields `UNKNOWN`
   Read as: candidate has not yet been shadow-observed; execution/PnL state is absent because no recent execution snapshot exists.

2. `OBSERVED_PASS` + `ALREADY_OBSERVED_GOOD` + `RECENTLY_OBSERVED` + `FLAT_NO_FILLS`
   Read as: the latest shadow run passed cleanly and stayed flat without fills.

3. `OBSERVED_PASS` + `ALREADY_OBSERVED_GOOD` + `RECENTLY_OBSERVED` + `ACTIVE_GAINING`
   Read as: observation passed and the latest execution snapshot still shows an open position with positive unrealized PnL.

4. `OBSERVED_PASS` + `ALREADY_OBSERVED_GOOD` + `RECENTLY_OBSERVED` + `ACTIVE_LOSING`
   Read as: observation passed but the latest execution snapshot shows an open losing position; inspect raw unrealized PnL before drawing conclusions.

5. `OBSERVED_PASS_NO_EVENTS` + `REOBSERVE_CANDIDATE` + `FLAT_NO_FILLS`
   Read as: lifecycle passed, but no meaningful event-flow/fill activity was captured; re-observation may be more useful than deeper execution interpretation.

6. `OBSERVED_FAIL` + `NEEDS_ATTENTION` + execution fields `UNKNOWN` or `NO_SNAPSHOT`
   Read as: the latest observation failed before producing a trustworthy execution snapshot.

7. `OBSERVED_UNKNOWN` + `REVIEW_OBSERVATION_STATE` + `UNKNOWN`
   Read as: the latest observation state is ambiguous and the execution snapshot is not trustworthy enough to simplify further.

8. `RECENTLY_OBSERVED` + `REALIZED_GAIN`
   Read as: the latest known execution snapshot is flat and closed with positive realized PnL.

9. `RECENTLY_OBSERVED` + `REALIZED_LOSS`
   Read as: the latest known execution snapshot is flat and closed with negative realized PnL; this is an operator attention case even if the observation path itself completed.

10. `observation_attention_flag=true` and `pnl_attention_flag=true`
    Read as: both observation-layer and execution-layer attention signals are active; inspect the raw fields instead of relying only on compact enums.

11. `outcome_class=STABLE_FLAT` + `latest_vs_recent_consistency=CONSISTENT`
    Read as: latest execution outcome and recent window both lean flat; no extra outcome drift is visible.

12. `outcome_class=MIXED_RECENT` + `outcome_attention_flag=true`
    Read as: latest execution state does not line up cleanly with recent outcomes; inspect the execution rollup if the pack matters.

## What Not To Infer

Do not treat this combined read as:
- an automatic go-live signal
- an automatic skip signal
- an execution recommendation
- a replacement for `decision_tier`
- a replacement for `context_flags`
- a replacement for the batch cadence contract

This contract is only for human operator reading.

## Minimal Operator Workflow

1. Check `decision_tier`, `score`, and `context_flags`.
2. Check `observation_status`, `next_action_hint`, and `reobserve_status`.
3. Check `pnl_interpretation` and `pnl_attention_flag`.
4. Check `outcome_class` and `outcome_attention_flag`.
5. If needed, check realized/unrealized signs and totals.
6. If still unclear, inspect `recent_observation_trail`.

Stop there unless deeper forensic detail is required.
