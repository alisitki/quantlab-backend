# Phase7 Microstructure Observability Patch v0

## Scope
- Target family: `microstructure_imbalance_v1`
- Target lane: `linkusdt` task-local smoke only
- No signal math redesign
- No threshold/hold/hysteresis redesign
- No ranking or promotion mutation

## What Changed
### Runtime intent/audit context
`MicrostructureImbalanceV1Strategy.js` now emits compact namespaced `trade_context` metadata on order intents for:
- flat entries
- flat exits
- reversal exits
- reversal entries

`StrategyRuntime.js` now persists that `trade_context` into `DECISION` and `FILL` audit events when present.

### New context fields
Per trade lifecycle, the new context surface includes:
- `entry_timestamp`
- `entry_pressure`
- `entry_abs_pressure`
- `entry_threshold`
- `entry_side`
- `entry_signal_reason`
- `entry_selected_cell`
  - `delta_ms`
  - `h_ms`
  - `pressure_threshold`
  - `symbol`
  - `exchange`
- `exit_timestamp`
- `exit_pressure`
- `exit_abs_pressure`
- `exit_reason`
- `hold_duration_ms`
- `prior_position_side`
- `trade_sequence_id`
- `was_reversal_trade`
- `max_abs_pressure_seen_during_trade`
- `min_abs_pressure_seen_during_trade`
- `pressure_decay_at_exit`
- `observation_count_during_trade`

### Derived shadow surfaces
The same compact `trade_context` now survives the full shadow-derived chain:
- summary execution events
- observation history
- flattened execution events
- futures paper ledger `episodes[]`

## Versioning
- No schema version bump was required.
- Existing surfaces remain:
  - `shadow_observation_summary_v0`
  - `shadow_observation_history_v0`
  - `shadow_execution_events_v1`
  - `shadow_futures_paper_ledger_v1`
- Change is backward-compatible: new fields are optional and only present when runtime emits `trade_context`.

## Why Each Field Matters
- `entry_pressure`, `entry_abs_pressure`: enables ex-ante pressure bucket attribution
- `entry_signal_reason`, `exit_reason`: separates flat exits from reversal exits
- `trade_sequence_id`: deterministic linkage across entry/exit/reversal lifecycle
- `prior_position_side`, `was_reversal_trade`: distinguishes fresh entries from reversal-born trades
- `hold_duration_ms`: supports direct duration bucket attribution
- `max/min_abs_pressure_seen_during_trade`, `pressure_decay_at_exit`, `observation_count_during_trade`: compact in-trade state summaries without raw traces
- `entry_selected_cell`: ties every trade back to the exact Phase6-selected cell

## Smoke Validation
Smoke command used:
```bash
bash tools/phase7_microstructure_observability_output/run_linkusdt_observability_smoke_v0.sh
```

Validation commands used:
```bash
node --check core/strategy/strategies/MicrostructureImbalanceV1Strategy.js
node --check core/strategy/runtime/StrategyRuntime.js
python3 -m py_compile tools/shadow_observation_summary_v0.py tools/shadow_observation_history_v0.py tools/shadow_execution_events_v1.py tools/shadow_futures_paper_ledger_v1.py
node --test core/strategy/strategies/tests/test-microstructure-imbalance-v1-strategy.js
node --test core/strategy/runtime/tests/test-execution-audit-events.js
python3 -m unittest tests.phase6.test_shadow_execution_events_v1 tests.phase6.test_shadow_futures_paper_ledger_v1 tests.phase6.test_shadow_observation_summary_v0 tests.phase6.test_shadow_observation_history_v0
```

Smoke outcome:
- `verify_soft_live_pass=true`
- `processed_event_count=1330`
- `execution_event_count=98`
- `paper_run_status=FILL_BACKED_POSITION_OPEN`
- output size remained slim: `smoke_linkusdt=1.8M`, `evidence_pack=12K`

## Artifact Paths With New Context
Task-local smoke output root:
- [smoke_linkusdt](/home/deploy/quantlab-backend/tools/phase7_microstructure_observability_output/smoke_linkusdt)

New context populated in:
- [summary.json](/home/deploy/quantlab-backend/tools/phase7_microstructure_observability_output/smoke_linkusdt/batch_out/rank01_microstructure_imbalance_v1_bybit_linkusdt_trade_d100_h500_pt020/summary.json)
- [shadow_observation_history_v0.jsonl](/home/deploy/quantlab-backend/tools/phase7_microstructure_observability_output/smoke_linkusdt/shadow_state/shadow_observation_history_v0.jsonl)
- [shadow_execution_events_v1.jsonl](/home/deploy/quantlab-backend/tools/phase7_microstructure_observability_output/smoke_linkusdt/shadow_state/shadow_execution_events_v1.jsonl)
- [shadow_futures_paper_ledger_v1.json](/home/deploy/quantlab-backend/tools/phase7_microstructure_observability_output/smoke_linkusdt/shadow_state/shadow_futures_paper_ledger_v1.json)

## Exact Next Rerun Command
Same linkusdt lane, same environment, improved observability surface:
```bash
bash tools/phase7_microstructure_observability_output/run_linkusdt_observability_rerun_v0.sh
```

This rerun is now attribution-v2 ready because trade-level pressure, reversal, and hold-duration context is persisted compactly per episode.
