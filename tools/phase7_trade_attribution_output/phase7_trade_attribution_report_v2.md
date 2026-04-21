# Phase7 Trade Attribution v2

This report joins full-rerun microstructure trade context with paper-ledger trade economics for `linkusdt` only.

- Join coverage: `2723/2723` (100.0%)
- Profit concentration: top 10% trades carry `59.8%` of gross, top 20% carry `88.9%`
- Minimum subset for >50% gross: `206` trades (7.6%), subset net `0.11772`

## Best/Worst Patterns
- Best duration bucket by avg gross: `>20s`
- Worst duration bucket by avg net: `<1s`
- Best pressure bucket by avg gross: `0.3+`
- Worst pressure bucket by avg net: `0.25-0.3`
- Best interaction by avg gross: `high_pressure_plus_long_duration`

## Separability
- Verdict: `NARROW_SUBSET_SALVAGEABLE`
- Simple-feature separability: `False`
- Reason: simple thresholds improve average gross but do not isolate a clean small subset with majority gross
- Strongest simple rule candidate: `hold_duration_ms >= 20000` (trade_ratio=3.5%, gross_share=12.3%, avg_gross=0.004041666667, avg_net=-0.002987725)
- Rule hints: `hold_duration_ms >= 20000`

FINAL DECISION: `RESHAPE (WITH RULE HINTS)`
