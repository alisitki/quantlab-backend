# FeatureBuilderV1 (Contract v1)

Deterministic feature and label generator for BBO data.

## Usage

```bash
node features/run_build_features_v1.js \
  --exchange binance \
  --stream bbo \
  --symbol btcusdt \
  --date 20251229
```

### Parameters
- `--exchange`: Exchange name (e.g., `binance`)
- `--stream`: Stream name (e.g., `bbo`)
- `--symbol`: Trading symbol (e.g., `btcusdt`)
- `--date`: Date in `YYYYMMDD` format.

## Features (Contract v1)

| Feature | Description |
|---------|-------------|
| `f_mid` | Mid-price `(bid + ask) / 2` |
| `f_spread` | `ask - bid` |
| `f_spread_bps` | Spread in basis points |
| `f_imbalance` | Volume imbalance `(bid_qty - ask_qty) / total_qty` |
| `f_microprice` | Weighted mid-price |
| `f_ret_1s, 5s, 10s, 30s` | Log returns using as-of backward fill |
| `f_vol_10s` | Standard deviation of `f_ret_1s` in 10s window |

## Label
- `label_dir_10s`: Binary (1 if `mid[t+10s] > mid[t]`, else 0).
- Forward lookup: uses first event with `ts_event >= t + 10s`.

## Implementation Details
- **No DuckDB**: Uses `hyparquet` + `fzstd` for reading and `parquetjs-lite` for writing.
- **Determinism**: Strict sort by `ts_event, seq`. Stable sort fallback preserves input order.
- **Data Cleaning**: 
  - Drops first 30s of the day (cold start).
  - Drops last 10s of the day (label lookup).
  - Drops any rows with NaNs.

## Output Structure
- `data.parquet`: Main dataset with fixed column order.
- `meta.json`: Dataset metadata including row counts and `config_hash`.
