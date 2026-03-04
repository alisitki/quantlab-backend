# Phase-5 latency_leadlag_v1 Execution Feasibility (Spec-Only)

## 1) SCOPE
- This document is feasibility-only.
- This is not a trading decision and not a Phase-6 artifact.
- Family and fixed setup:
- `family=latency_leadlag_v1`
- `stream=bbo`
- `exchanges=binance|okx|bybit`
- `focus_pair=binance->okx`
- `dt_ms=25`, `H_ms=250`, `tolerance_ms=20`
- Evidence reference:
- `pack_id=multi-hypothesis-phase5-latency-leadlag-v1-bbo-top3-20260213_063414`
- `N=18`, `DIRECTIONAL=18/18`, `determinism PASS=18/18`
- `median(mean_bps)=0.872906`, `min(mean_bps)=0.704696`, `conservative_cost_bps(>=80% survive)=0.6`

## 2) COST MODEL (explicit, bounded)
- Define:
- `total_cost_bps = fee_bps + slippage_bps + adverse_selection_bps + misc_bps`

- Mode A: TAKER (cross spread)
- Inputs to plug later per exchange/account tier:
- `fee_bps_taker` in exchange-tier-dependent range (placeholder)
- `slippage_bps_taker` in measured execution band (placeholder)
- `adverse_selection_bps_taker` in measured short-horizon band (placeholder)
- `misc_bps_taker` (rebates/funding/frictions not in signal)
- Condition checks:
- Robust tradeable: `total_cost_bps <= 0.6`
- Weak but possible: `0.6 < total_cost_bps <= 0.8`
- Not tradeable under current edge envelope: `total_cost_bps > 0.8`

- Mode B: MAKER (post-only)
- Inputs to plug later:
- `fee_bps_maker` in exchange-tier-dependent range (placeholder)
- `fill_probability` (0..1)
- `queue_penalty_bps` proxy for missed/late fills and queue position
- `adverse_selection_bps_maker` (if filled during adverse micro-move)
- `misc_bps_maker`
- Effective cost model to evaluate later:
- `effective_total_cost_bps_maker = fee_bps_maker + slippage_bps_maker + adverse_selection_bps_maker + queue_penalty_bps + misc_bps_maker`
- Condition checks:
- Robust tradeable: `effective_total_cost_bps_maker <= 0.6`
- Weak but possible: `0.6 < effective_total_cost_bps_maker <= 0.8`

- No exact fee tier values are asserted here.
- This section is inequality-based and requires later measured/contractual inputs.

## 3) SIGNAL-TO-EXECUTION MAP (action interface)
- Signal time:
- `t0`: leader move detected on source (binance in focus pair)
- `t1`: follower reference timestamp on target (okx) with matching tolerance `±20ms` around `t0 + dt_ms`
- Action horizon:
- `H=250ms` from `t1`
- Direction:
- Use sign of estimated effect from the signal (`mean_bps` sign for configured cell). Current evidence is positive for configured focus cell.
- Action interface (minimal):
- Input: `(pair, dt_ms=25, H_ms=250, tolerance_ms=20, signal_sign, confidence)`
- Output intent: `(target_exchange, side, size_cap, expiry<=H)`
- Assumption statement:
- This spec assumes action can be emitted within latency budget; no guarantee is claimed.

## 4) LATENCY BUDGET (hard requirement)
- End-to-end latency decomposition:
- `total_latency_ms = ingest_delay + compute_delay + order_route_delay + exchange_ack_delay`
- Hard target (placeholder, to validate later):
- `total_latency_ms < 50ms`
- Justification:
- `dt=25ms` and `H=250ms` imply a narrow reaction window; latency must be materially below horizon scale and not dominate the 25ms lead-lag structure.
- Measurement plan (no run now):
- Instrument timestamps at:
- market data ingest receive
- signal compute start/end
- order submit call
- exchange ack receive
- Persist per-order latency components and compute distribution (`p50/p90/p99`) in later phase.

## 5) RISK / GUARDS (non-negotiable)
- Position limit:
- `max_position_notional_per_symbol = PARAM_NOTIONAL_CAP` (to be set before any live-like stage)
- Order rate limit:
- `max_orders_per_minute = PARAM_OPM_CAP`
- Kill switches:
- Latency breach: disable strategy if measured `total_latency_ms` exceeds configured budget threshold.
- Slippage breach: disable if online slippage estimate `> PARAM_SLIPPAGE_BPS_MAX`.
- Drawdown breach: disable if live/shadow PnL drawdown `> PARAM_DRAWDOWN_MAX`.
- Signal anomaly: disable on abnormal signal-rate spike (`> PARAM_SIGNAL_RATE_MAX`).
- Promotion guard:
- No live deployment until a formal Phase-6 exists and is approved.

## 6) DEPLOYMENT STAGING (Phase-5 only)
- Stage 0: offline replay evidence (DONE).
- Stage 1: shadow / quote-only validation (future, Phase-6+).
- Stage 2: tiny-capital controlled live (future, Phase-6+).
- Explicit boundary:
- Current work stops at Phase-5 spec; no execution rollout in this phase.

## 7) DECISION OUTPUT (required unknowns + rule)
- Unknowns to fill before any promotion design:
- Actual maker/taker fee tier per exchange and account.
- Measured slippage distribution at `H=250ms` for intended size bands.
- Achievable end-to-end latency distribution on target infrastructure.

- Mechanical go/no-go rule:
- If estimated `total_cost_bps >= min(mean_bps)=0.704696` => `NO-GO`.
- If estimated `total_cost_bps <= 0.6` => `GO_TO_PHASE6_DESIGN` (design only, not execute).
- Else (`0.6 < total_cost_bps < 0.704696`) => `NEEDS_IMPROVEMENT` (latency/execution optimization or cost reduction required).

## 8) APPENDIX: numbers from current evidence
- Break-even grid (from `break_even_grid.tsv`):

| total_cost_bps | survival_rate | avg_net_bps |
|---:|---:|---:|
| 0.0 | 1.000000 | 0.948255 |
| 0.2 | 1.000000 | 0.748255 |
| 0.4 | 1.000000 | 0.548255 |
| 0.6 | 1.000000 | 0.348255 |
| 0.8 | 0.722222 | 0.148255 |
| 1.0 | 0.333333 | -0.051745 |
| 1.2 | 0.111111 | -0.251745 |
| 1.5 | 0.000000 | -0.551745 |
| 2.0 | 0.000000 | -1.051745 |

- Per-symbol `min/p10/median` of `mean_bps`:

| symbol | min_bps | p10_bps | median_bps |
|---|---:|---:|---:|
| adausdt | 1.101261 | 1.113524 | 1.162177 |
| avaxusdt | 0.791002 | 0.796316 | 0.850146 |
| linkusdt | 0.704696 | 0.724302 | 0.771142 |
