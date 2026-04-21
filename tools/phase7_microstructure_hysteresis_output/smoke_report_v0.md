# Phase7 Microstructure Hysteresis v0

- strategy: `microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020__xt010`
- lever: `HYSTERESIS`, entry threshold `0.2`, exit threshold `0.1`
- verdict: `SMOKE_PASS`
- reason: verify_soft_live_pass=true and processed_event_count>0
- baseline fills: `2150` after fills: `63`
- baseline reversals/1k events: `39.34432392804106` after reversals/1k events: `66.95464362850971`
- baseline gross/fill: `0.001219069767` after gross/fill: `0.0003015873015872472`
- baseline fees: `15.3455828` after fees: `0.45936800000000005`
- baseline gross: `2.621` after gross: `0.018999999999996575`

No ranking, promotion, Phase5, hold lever, or threshold lever changes were made.
