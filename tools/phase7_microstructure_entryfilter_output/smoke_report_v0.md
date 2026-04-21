# Phase7 Microstructure Entry Filter v0

- strategy: `microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt030`
- lever: `ENTRY_FILTER`, pressure threshold `0.2 -> 0.3`
- verdict: `SMOKE_PASS`
- reason: verify_soft_live_pass=true and processed_event_count>0
- baseline fills: `2150` after fills: `52`
- baseline gross/fill: `0.001219069767` after gross/fill: `0.000865384615384614`
- baseline fees: `15.3455828` after fees: `0.3799916`
- baseline gross: `2.621` after gross: `0.04499999999999993`

No ranking, promotion, Phase5, or extra strategy levers were changed.
