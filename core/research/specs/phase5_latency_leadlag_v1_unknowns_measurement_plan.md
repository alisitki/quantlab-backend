# Phase-5 latency_leadlag_v1 Unknowns Measurement Plan (Spec-Only)

## 1) GOAL (one paragraph)
This plan converts unknowns into measurable inputs so `total_cost_bps` can be evaluated against the robust threshold (`0.6 bps`) and the hard no-go threshold (`min(mean_bps)=0.704696 bps`) before any Phase-6 design promotion.

## 2) FEE TIERS (manual inputs)
- Fee tiers are contractual/account-specific inputs; they must be entered from official exchange/account documentation and must not be guessed.
- Template path: `core/research/specs/templates/fee_tiers_input.tsv`
- Columns (exact):
- `exchange,market,account_tier,maker_fee_bps,taker_fee_bps,notes,source_link_or_doc`
- Mechanical rule:
- `total_fee_bps = maker_fee_bps` when intended mode is maker.
- `total_fee_bps = taker_fee_bps` when intended mode is taker.

## 3) SLIPPAGE MEASUREMENT PLAN (spec-only, no run now)
- Slippage definition for this hypothesis:
- Primary: `slippage_bps_primary = signed( executed_price vs midprice_at_signal_time_t1 )`.
- Secondary: `slippage_bps_secondary = signed( executed_price vs midprice_at_submit_time )`.
- Size bands (editable placeholders, explicit start set):
- notional_usd bands: `[100, 500, 1000, 5000, 10000]`.
- Horizons:
- Primary measurement horizon: `H=250ms`.
- Optional secondary horizon: `H=500ms`.
- Sampling windows policy (spec-only, no run now):
- Use same eligible selection policy as current Phase-5 evidence: `status=SUCCESS`, `day_quality_post in {GOOD,DEGRADED}`, 3-exchange coverage.
- Template path: `core/research/specs/templates/slippage_measurement_plan.tsv`
- Columns (exact):
- `exchange,symbol,mode(maker|taker),notional_usd_band,reference_mid(t1|submit),H_ms,metric(p50|p90|p99),target_slippage_bps,notes`
- Acceptability mapping against thresholds:
- If estimated `(fee + slippage_p90 + adverse_selection_proxy) <= 0.6` => robust.
- If estimated `(fee + slippage_p90 + adverse_selection_proxy) <= 0.7047` => borderline (needs improvement).
- Else => no-go.

## 4) LATENCY INSTRUMENTATION PLAN (spec-only)
- Stable timestamp identifiers:
- `t_ingest_rx`, `t_signal_start`, `t_signal_end`, `t_order_submit`, `t_exchange_ack`.
- Derived metrics:
- `ingest_delay = t_signal_start - t_ingest_rx`
- `compute_delay = t_signal_end - t_signal_start`
- `route_delay = t_exchange_ack - t_order_submit`
- `ack_delay = t_exchange_ack - t_order_submit` (until finer split is instrumented)
- `total_latency = t_exchange_ack - t_ingest_rx`
- Required summary stats:
- `p50/p90/p99` per metric, per exchange, per symbol.
- Guard threshold placeholder:
- `total_latency_p99_ms <= PARAM_LAT_P99_MAX`.
- Rationale:
- With `dt=25ms` and `H=250ms`, latency variance can erase usable edge even when directional signal is strong.

## 5) ADVERSIVE SELECTION PROXY (spec-only)
- Proxy definition (without full live trading):
- Measure post-submit mid-price move over `250ms` (or next-tick fallback) conditioned on signal direction.
- Example interpretation:
- If predicted direction is up and mid moves down after submit, this contributes adverse selection.
- Output metric to carry forward:
- `adverse_selection_p90_bps` (placeholder target to be filled from measurement stage).

## 6) CHECKLIST TO ENTER PHASE-6 DESIGN (still no execution)
- Required prerequisites:
- `fee_tiers_input.tsv` filled for all target exchanges and intended market/account combinations.
- `slippage_measurement_plan.tsv` populated with at least `p90` targets per relevant mode/size band.
- Latency instrumentation points agreed (`t_ingest_rx`, `t_signal_start`, `t_signal_end`, `t_order_submit`, `t_exchange_ack`) and summary outputs defined.
- Promotion condition:
- Only after the checklist is complete, `GO_TO_PHASE6_DESIGN` is allowed (design only, not execution).
