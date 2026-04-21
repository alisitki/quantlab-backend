# Phase7 Profitability Analysis v0

Generated: `2026-04-06T06:32:29Z`

## Final Decision

`NEITHER_CONTINUE`

NEITHER_CONTINUE because both candidates have negative gross pnl before fees; no real edge

## Per Strategy

### linkusdt

- Verdict: `DROP`
- Gross PnL before fees/funding: `-0.018`
- Net realized PnL: `-0.1635256` with status `NET_AFTER_FEES_AND_FUNDING`
- Fees: `0.1455256` total, `0.003549404878` per fill
- Funding: `0.0` total
- Gross per fill: `-0.00043902439`; net per fill: `-0.003988429268`
- Gross episode win rate: `0.238095238095`
- Edge statement: `no real edge`
- Reason: gross edge is not positive before costs

### avaxusdt

- Verdict: `DROP`
- Gross PnL before fees/funding: `-0.013`
- Net realized PnL: `-0.4589356` with status `NET_AFTER_FEES_AND_FUNDING`
- Fees: `0.4459356` total, `0.003779115254` per fill
- Funding: `0.0` total
- Gross per fill: `-0.000110169492`; net per fill: `-0.003889284746`
- Gross episode win rate: `0.306451612903`
- Edge statement: `no real edge`
- Reason: gross edge is not positive before costs

## Comparison

Stronger candidate on relative gross efficiency: `avaxusdt`.
avaxusdt has the less negative gross pnl per fill and higher fill-backed activity, but both gross pnl totals are negative before fees

Fee is a major burden for both, but it is not the main distinction: both gross PnL values are already negative before fees. Funding impact is zero in the current artifact surface.

## Next Step

Do not promote this pair from the current 24h evidence. The next sprint should be cost/profitability gating or execution-threshold analysis using existing artifacts; do not treat this as a discovery failure.

