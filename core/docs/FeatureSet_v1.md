# FeatureSet v1 Specification

**Version**: 1.0.0
**Target Model**: XGBoost (Binary Classification)

## 1. Overview
Minimal feature set (10 features) using Price and Quantity from BBO stream.

## 2. Feature Definitions

| Feature Name | Formula | Unit | Description |
|--------------|---------|------|-------------|
| `f_mid` | `(bid + ask) / 2` | Price | Mid-price of the market |
| `f_spread` | `ask - bid` | Price | Absolute bid-ask spread |
| `f_spread_bps`| `(spread / mid) * 10000` | BPS | Spread in basis points |
| `f_imbalance` | `(bid_qty - ask_qty) / (bid_qty + ask_qty)` | Ratio | Normalized volume imbalance (-1 to 1) |
| `f_microprice`| `(bid_price * ask_qty + ask_price * bid_qty) / (bid_qty + ask_qty)` | Price | Weighted mid-price |
| `f_ret_1s` | `log(mid[t] / mid[t-1s])` | Log Ret | 1-second log return |
| `f_ret_5s` | `log(mid[t] / mid[t-5s])` | Log Ret | 5-second log return |
| `f_ret_10s` | `log(mid[t] / mid[t-10s])` | Log Ret | 10-second log return |
| `f_ret_30s` | `log(mid[t] / mid[t-30s])` | Log Ret | 30-second log return |
| `f_vol_10s` | `std_dev(ret_1s, window=10)` | Vol | Rolling 10s return standard deviation |

## 3. Implementation Details
- **Log Returns**: Preferred over percentage for symmetry.
- **Handling NaN/Inf**: 
  - `f_imbalance`: If sum of quantities is 0, default to 0.
  - `f_spread_bps`: If mid is 0 (error case), default to 0.
  - **Cold Start**: First 30 seconds of the day will have NaNs for rolling features; these rows MUST be dropped.
- **Microprice**: Sensitive to quantity spikes; useful for short-term direction.

## 4. Determinism
- All time-based windows (1s, 5s, 10s, 30s) must use **event-time** (`ts_event`), not wall-clock time.
- **as-of (backward) fill**: If `mid(t-N)` does not exist at exactly `t-N`, use the value from the last known event where `ts_event <= t-N`.
