# Phase-5 latency_leadlag_v1 fee baseline note (inputs-only)

## Thresholds vs fee baseline

| Item | Value (bps) | Meaning |
|---|---:|---|
| Robust threshold | 0.6 | Cost target where edge is considered robust in Phase-5 feasibility |
| Hard no-go threshold | 0.704696 | `min(mean_bps)` from current evidence; if total cost is at/above this, expected net edge is non-positive in worst observed window |

## VIP0/regular baseline fee comparison

| Exchange | Market | Maker (bps) | Taker (bps) | Discounted maker (bps) | Discounted taker (bps) |
|---|---|---:|---:|---:|---:|
| Binance | USDⓈ-M Futures | 2.0 | 5.0 | 1.8 (BNB 10% assumption) | 4.5 |
| OKX | Derivatives | 2.0 | 5.0 | N/A | N/A |
| Bybit | Perpetual & Futures | 1.0 | 6.0 | 0.9 (MNT 10% assumption) | 5.4 |

## Mechanical interpretation

- Even discounted maker fees in this baseline are >= 0.9 bps, which is above the robust threshold (0.6 bps).
- Discounted maker baseline (0.9 bps) is also above the hard no-go threshold (0.704696 bps).
- Taker mode is mechanically impossible under VIP0 baseline assumptions because taker fees alone (4.5 to 6.0 bps) are far above the observed edge scale.
- Maker mode under these VIP0 baselines is still above robust feasibility and likely NO-GO unless better economics exist (for example conditional promotions, rebates, or negative maker outcomes).

## Scope note

This is an inputs-only baseline note. It does not assert that promotions/rebates exist; it only states conditional implications if such economics become available.
