# Storage Layout Specification v1

**Scope**: S3 Buckets mapping and Hive partition standards.

## 1. Bucket Mapping
QuantLab ML uses three primary buckets with explicit roles and paths.

| Bucket | AWS/S3 Identifier | Role | Content Type |
|--------|-------------------|------|--------------|
| **Raw** | `quantlab-raw` | Source | Read-Only: Original tick data (CSV/JSON/Raw). |
| **Compact** | `quantlab-compact` | Curated & Features | Read/Write: Curated Parquet files and generated Feature datasets. |
| **Artifacts** | `quantlab-artifacts` | Jobs & Models | Read/Write: Training artifacts (JobSpec, binaries) and Production models. |

## 2. Path Standards (Hive Partitioning)
All datasets in `quantlab-compact` follow the Hive format:
`.../exchange={ex}/stream={st}/symbol={sym}/date={YYYYMMDD}/`

### A. Curated Data (ML Input)
Curated BBO ticks used as the raw source for feature extraction.
- **Root**: `s3://quantlab-compact/curated/`
- **Example**: `s3://quantlab-compact/curated/exchange=binance/stream=bbo/symbol=btcusdt/date=20251229/`

### B. Feature Datasets (ML Output)
The final joined Parquet file used directly by the training job.
- **Root**: `s3://quantlab-compact/features/`
- **Prefix**: `featureset=v1/`
- **Example**: `s3://quantlab-compact/features/featureset=v1/exchange=binance/stream=bbo/symbol=btcusdt/date=20251229/data.parquet`

### C. Training Artifacts
Output from the Vast.ai GPU training job.
- **Root**: `s3://quantlab-artifacts/ml-artifacts/`
- **Structure**: `{jobId}/...`
- **Example**: `s3://quantlab-artifacts/ml-artifacts/job-8271/model.bin`

### D. Production Models
Promoted models used by the live trading engine.
- **Root**: `s3://quantlab-artifacts/models/production/`
- **Structure**: `{symbol}/...`
- **Example**: `s3://quantlab-artifacts/models/production/btcusdt/model.bin`

## 3. Metadata Schema (`meta.json`)
Every feature dataset must include a `meta.json` at the leaf directory:
```json
{
  "featureset_version": "v1",
  "symbol": "btcusdt",
  "date": "20251229",
  "rows": 854201,
  "ts_min": 1735516800000,
  "ts_max": 1735603199999,
  "label_horizon_sec": 10,
  "config_hash": "a1b2c3d4..."
}
```
