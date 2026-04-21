# Phase7 Microstructure Fee-Aware v0

- strategy: `microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020__xt010__fa`
- lever: `FEE_AWARE_ENTRY_GATING`, hysteresis kept, fee-aware gate enabled
- verdict: `SMOKE_PASS`
- reason: verify_soft_live_pass=true and processed_event_count>0
- baseline fills: `3207` after fills: `46`
- baseline gross/fill: `0.001264109759900226` after gross/fill: `0.0011739130434783053`
- baseline fees: `23.12428800000001` after fees: `0.3118268`
- baseline net: `-19.070287999999984` after net: `-0.25782679999999797`
- baseline gross: `4.054000000000025` after gross: `0.054000000000002046`

No ranking, promotion, Phase5, hold, threshold, or extra filtering levers were changed.
