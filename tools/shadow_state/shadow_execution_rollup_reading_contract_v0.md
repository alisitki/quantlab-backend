# Shadow Execution Rollup Snapshot Reading Contract v0

This contract is informational only.

It does not:
- change ranking
- change selection
- trigger trades
- auto-run anything
- replace cadence, scheduler, or batch policy
- replace deeper execution or PnL analysis

Use it to read the compact execution/PnL rollup snapshot faster.

## Authoritative Surface

Primary artifact:
- `tools/shadow_state/shadow_execution_rollup_snapshot_v0.json`

Authoritative builder:
- `tools/shadow_execution_rollup_snapshot_v0.py`

Upstream execution/PnL sources:
- `tools/shadow_state/shadow_execution_pack_summary_v0.json`
- `tools/shadow_execution_pack_summary_v0.py`

Related reading contracts:
- `tools/shadow_state/shadow_execution_reading_contract_v0.md`
- `tools/shadow_state/shadow_combined_operator_reading_contract_v0.md`

## Field Reading Order

Read each rollup item in this order:

1. `combined_pnl_status_short`
2. `pnl_rollup_attention`
3. `pnl_interpretation`
4. `recent_pnl_bias`
5. `recent_rollup_short`
6. `recent_run_count`, `recent_attention_count`
7. `last_pnl_state`
8. `last_observed_at`

Why this order:
- first read the shortest combined state
- then see whether the compact rollup is attention-worthy
- then inspect the latest interpretation and recent bias
- then use the short recent-run summary
- only then inspect the raw latest state and timestamp

## Field Semantics

### `combined_pnl_status_short`

Format:
- `<pnl_interpretation>/<recent_pnl_bias>`

Examples:
- `FLAT_NO_FILLS/FLAT_BIAS`
- `ACTIVE_GAINING/GAIN_BIAS`
- `REALIZED_LOSS/LOSS_BIAS`
- `UNKNOWN/NO_HISTORY`

### `pnl_rollup_attention`

Values:
- `true`
- `false`

Read:
- `true` means either:
  - latest execution/PnL state is already attention-worthy
  - or recent attention count is non-zero

### `pnl_interpretation`

Current enum:
- `NO_SNAPSHOT`
- `ACTIVE_GAINING`
- `ACTIVE_LOSING`
- `ACTIVE_FLAT`
- `ACTIVE_UNKNOWN`
- `REALIZED_GAIN`
- `REALIZED_LOSS`
- `REALIZED_FLAT`
- `FLAT_NO_FILLS`
- `UNKNOWN`

### `recent_pnl_bias`

Current enum:
- `GAIN_BIAS`
- `LOSS_BIAS`
- `FLAT_BIAS`
- `MIXED`
- `NO_HISTORY`

### `recent_rollup_short`

Format:
- `r<recent_run_count>:g<recent_gain_count>/l<recent_loss_count>/f<recent_flat_count>/a<recent_attention_count>`

Example:
- `r3:g0/l0/f1/a2`

## Deterministic Interpretation Patterns

1. `FLAT_NO_FILLS/FLAT_BIAS`
   Read as: latest state is flat with no fills, and the recent window is also dominated by flat outcomes.

2. `ACTIVE_GAINING/GAIN_BIAS`
   Read as: latest state is an open gaining position and the recent window also leans positive.

3. `ACTIVE_LOSING/LOSS_BIAS`
   Read as: latest state is an open losing position and the recent window also leans negative.

4. `REALIZED_GAIN/GAIN_BIAS`
   Read as: latest flat state closed with positive realized PnL and recent runs also lean positive.

5. `REALIZED_LOSS/LOSS_BIAS`
   Read as: latest flat state closed with negative realized PnL and recent runs also lean negative.

6. `*/MIXED`
   Read as: recent window does not show a single dominant directional bias.

7. `*/NO_HISTORY`
   Read as: no recent rollup history is available for this pack.

8. `pnl_rollup_attention=true`
   Read as: do not rely only on the compact combined status; inspect the latest and recent components explicitly.

9. `recent_rollup_short` with `a>0`
   Read as: at least one run in the recent window landed in an attention-class state.

10. `recent_run_count=0`
    Read as: no recent runs were available for rollup summarization.

## What Not To Infer

Do not treat this artifact as:
- an execution recommendation
- a trading signal
- an automatic go-live decision
- an automatic skip signal
- a replacement for the combined operator reading contract
- a replacement for deeper pack summary inspection

This contract is only for human operator reading.

## Minimal Operator Workflow

1. Check `combined_pnl_status_short`.
2. Check `pnl_rollup_attention`.
3. Check `pnl_interpretation` and `recent_pnl_bias`.
4. Check `recent_rollup_short`.
5. If needed, inspect `last_pnl_state` and `last_observed_at`.

Stop there unless deeper execution/PnL forensics are required.
