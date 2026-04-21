# Microstructure Imbalance Fullscan v0

- generated_ts_utc: `2026-04-06T07:53:10Z`
- status: `COMPLETED`
- classification: `STRONG_SIGNAL`
- window: `20260324..20260328` (5 days)
- trade pairs scanned: `22`
- total cells: `2970`
- directional cells: `1246`
- anti-edge cells: `425`
- no-edge cells: `1299`
- insufficient-support cells: `0`
- symbols with directional cells: `13`
- bbo decision: `SKIPPED_RESOURCE_SAFETY` - full-window bbo estimate 2499750738 bytes / 106617255 rows exceeds slim resource threshold 1000000000 bytes

## Best Symbols
- `bybit/linkusdt` directional=135 anti=0 selected=DIRECTIONAL mean=1.050521208427813 t=77.14271887879215
- `bybit/ltcusdt` directional=135 anti=0 selected=DIRECTIONAL mean=0.903639475859555 t=80.49649160419622
- `bybit/avaxusdt` directional=135 anti=0 selected=DIRECTIONAL mean=0.810999528691749 t=86.6930236295316
- `bybit/bnbusdt` directional=135 anti=0 selected=DIRECTIONAL mean=0.658767609596866 t=56.57114935240377
- `bybit/solusdt` directional=135 anti=0 selected=DIRECTIONAL mean=0.647531890835687 t=97.82992372167372

## Worst Symbols
- `binance/adausdt` anti=135 directional=0 selected=ANTI_EDGE mean=-1.434074571421613 t=-192.84988516404405
- `okx/adausdt` anti=120 directional=0 selected=ANTI_EDGE mean=-0.435228593248365 t=-75.93414535708942
- `binance/ltcusdt` anti=100 directional=0 selected=ANTI_EDGE mean=-0.392460638008767 t=-52.26631301886552
- `binance/solusdt` anti=47 directional=0 selected=ANTI_EDGE mean=-0.310233450511394 t=-137.38008088415987
- `okx/bnbusdt` anti=23 directional=0 selected=ANTI_EDGE mean=-0.250396354479495 t=-52.80966231721031

## Exchange Consistency
- `binance` directional=112 anti=282 no_edge=551 symbols_with_directional=3/7
- `bybit` directional=933 anti=0 no_edge=12 symbols_with_directional=7/7
- `okx` directional=201 anti=143 no_edge=736 symbols_with_directional=3/8

## Failure Modes
- Bybit carries most directional cells; this is not uniform across exchanges.
- Binance has anti-edge/no-edge dominance in this window.
- OKX is mixed and needs Phase6 selection to avoid weak symbols.

## Decision
multiple symbols produced directional cells with sufficient support and directional count was not dominated by anti-edge cells.

Next step: `proceed_to_phase6_selection`.
