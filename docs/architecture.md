# Architecture

Current operational architecture is declared in `tools/system_state/canonical_truth_registry_v0.json`.

- Current critical path: `s3 compact state -> phase5 pack -> multi-hypothesis -> promotion/candidate review -> shadow watchlist/observation -> run-shadow-watchlist-v0.js -> run-soft-live.js -> LiveStrategyRunner(live/paper)`
- Current default operator path: `tools/phase5_nightly_orchestrator_v0.py`
- Current default active shadow subset: `tools/shadow_state/shadow_watchlist_v0.json`
- Current runtime bindability truth: `tools/phase6_state/candidate_strategy_runtime_binding_v0.json`

This document is intentionally short. If this file conflicts with code or generated state, trust the registry, code, and state surfaces.
