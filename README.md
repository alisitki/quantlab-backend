# QuantLab

QuantLab is the futures research and shadow-trading repo for Binance, Bybit, and OKX.

## Start Here

- Canonical truth and state-governance registry: `tools/system_state/canonical_truth_registry_v0.json`
- Current critical path: `s3 compact state -> phase5 pack -> multi-hypothesis -> promotion/candidate review -> shadow watchlist/observation -> run-shadow-watchlist-v0.js -> run-soft-live.js -> LiveStrategyRunner(live/paper)`
- Current default operator path: `tools/phase5_nightly_orchestrator_v0.py`
- Current default active shadow subset: `tools/shadow_state/shadow_watchlist_v0.json`
- Current runtime bindability truth: `tools/phase6_state/candidate_strategy_runtime_binding_v0.json`

## State Governance

- Read the canonical registry before trusting summary artifacts.
- `shadow_bound_launch_watchlist_v0.json` is a one-shot launch snapshot, not the global shadow subset.
- Docs are secondary to code and generated state. If docs and code/state conflict, trust code/state.

## Parallel Lanes

- `replayd` and `strategyd` service routes exist in parallel, but they are not the default hypothesis->shadow critical path.
- The ML scheduler lane under `core/scheduler/` also remains in-repo and is intentionally not removed by this governance cleanup.
