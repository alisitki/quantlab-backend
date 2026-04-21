# Phase7 Trade Attribution v0

- strategy: `microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020`
- verdict: `NARROW_SUBSET_SALVAGEABLE`
- reason: small subset carries majority of gross pnl and is near net neutral, but not directly actionable from current artifacts
- closed trades analyzed: `2121`
- total gross pnl: `2.620999999999949`
- total net pnl: `-12.720957200000065`
- total fees: `15.341957200000005`
- top 10% trades gross share: `0.5429225486455704`
- top 20% trades gross share: `0.8115223197253137`
- majority-gross subset ratio: `0.08816595945308817`
- majority-gross subset net pnl: `-0.03822080000001067`
- best duration bucket by avg gross/trade: `>20s`
- worst duration bucket by avg net/trade: `<1s`
- reversal trade share of gross: `0.9576497520030505`

Pressure buckets are not available in the current artifact surface because entry/exit pressure is not persisted in the Phase7 ledgers.

Kill vs reshape:
- `NARROW_SUBSET_SALVAGEABLE`
