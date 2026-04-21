# Phase7 Microstructure Shadow Result v0

- generated_ts_utc: `2026-04-06T08:38:12Z`
- status: `STOPPED_RUNTIME_INCOMPATIBLE`
- lane_result: `INVALID_LANE`
- bybit-only validation: requested, not executed
- strategies requested: `10`
- strategies run: `0`
- fills produced: `0`
- survivors: `0`

## Reason
`ReturnReversalV1Strategy.js` requires `family_id=return_reversal_v1` and a selected cell with `mean_product < 0` / `t_stat <= -2`. The shortlist is `microstructure_imbalance_v1` with positive directional pressure fields (`mean_signed_fwd_return_bps`, `pressure_threshold`).

Running ReturnReversal here would validate price return-reversal behavior, not microstructure pressure. I stopped rather than producing misleading Phase7 evidence.

## Next Step
Implement a minimal `MicrostructureImbalanceV1Strategy` runtime/binding that consumes trade side/quantity pressure, then run the same bybit-only shortlist under isolated `PAPER_DIRECTIONAL_V1`. No profitability verdict was produced in this sprint.
