---
name: data-pipeline
description: Compaction, S3 storage, and data integrity
---

# Data Pipeline

This skill covers the data compaction and S3 storage infrastructure.

## Pipeline Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Collector  │────▶│    Spool    │────▶│  Compactor  │────▶│  S3 Compact │
│  (Python)   │     │  (Parquet)  │     │  (Python)   │     │  (Archive)  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

---

## Key Files

| File | Purpose |
|------|---------|
| `collector/collector.py` | WebSocket → Parquet writer |
| `collector/writer.py` | Atomic file writer (fsync + rename) |
| `core/compressor/compact.py` | K-way merge compaction |
| `core/compressor/run.py` | Compaction orchestration |
| `core/compressor/merge_writer.py` | Streaming merge writer |

---

## S3 Bucket Structure

### Raw (Spool)
```
/opt/quantlab/spool/
└── exchange=binance/
    └── stream=bbo/
        └── symbol=btcusdt/
            └── 2026-02-03_14-30-00.parquet
```

### Compact (S3)
```
s3://quantlab-compact/
└── exchange=binance/
    └── stream=bbo/
        └── symbol=btcusdt/
            └── date=20260203/
                ├── data.parquet
                └── meta.json
```

---

## Compaction Modes

```bash
# Daily compaction (yesterday's data)
python core/compressor/run.py --mode daily

# Backfill specific date range
python core/compressor/run.py --mode backfill --start 20260101 --end 20260115

# Single partition
python core/compressor/run.py --mode single --date 20260203 --symbol btcusdt --stream bbo
```

---

## compact.py Algorithm

Uses **K-way merge** for memory-efficient processing:

1. **Scan inputs** — List all parquet files for partition
2. **Open readers** — One per input file
3. **K-way merge** — Stream-merge by (ts_event, seq)
4. **Write output** — Single data.parquet
5. **Generate meta.json** — Row count, hash, time range

### Critical Properties
- **Bounded memory** — 2-3 GB RSS max
- **Deterministic output** — Same inputs → same output
- **SHA256 fingerprint** — In meta.json for verification

---

## merge_writer.py

Streaming writer that handles memory constraints:

```python
from merge_writer import StreamingMergeWriter

writer = StreamingMergeWriter(output_path, schema)
for row in merged_iterator:
    writer.write(row)
writer.finalize()
```

---

## Atomic Write Protocol (writer.py)

**NEVER modify without explicit instruction!**

```python
# Critical protocol - ensures data integrity
1. Write to .tmp file
2. fsync the file
3. Verify parquet readable
4. Atomic rename to .parquet
5. fsync parent directory
```

---

## meta.json Format

```json
{
  "row_count": 1234567,
  "schema_version": "v1",
  "ts_event_min": "1234567890000000000",
  "ts_event_max": "1234567899999999999",
  "sha256": "abc123...",
  "created_at": "2026-02-03T10:30:00Z",
  "source_files": 42
}
```

---

## QUARANTINE Handling

Files that fail compaction are quarantined:

```
s3://quantlab-compact/QUARANTINE/
└── exchange=binance/
    └── stream=bbo/
        └── symbol=btcusdt/
            └── date=20260203/
                ├── failing_file.parquet
                └── error.json
```

**Error types:**
- `DICT_CONFLICT` — Dictionary encoding mismatch
- `SNAPPY_CORRUPT` — Compression corruption
- `SCHEMA_MISMATCH` — Column schema differs

---

## Verification

```bash
# Verify compact output
python core/compressor/verify_compact.py --date 20260203 --symbol btcusdt

# Check S3 dataset exists
aws s3 ls s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20260203/
```

---

## Cron Configuration

```cron
# Daily compaction - 01:00 UTC
0 1 * * * cd /home/deploy/quantlab-backend && python core/compressor/run.py --mode daily >> /var/log/quantlab-compact.log 2>&1
```

---

## Systemd Service

```bash
# Start compaction service
sudo systemctl start quantlab-compact

# Check status
sudo systemctl status quantlab-compact

# View logs
sudo journalctl -u quantlab-compact -f
```

---

## Troubleshooting

### OOM During Compaction
```bash
# Check memory usage
ps aux | grep compact
cat /proc/$(pgrep -f compact)/status | grep VmRSS
```

### Missing Partitions
```bash
# Check raw spool
ls -la /opt/quantlab/spool/exchange=binance/stream=bbo/symbol=btcusdt/

# Check S3 target
aws s3 ls s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20260203/
```

### Corrupt Parquet
```bash
# Verify parquet file
python -c "import pyarrow.parquet as pq; print(pq.read_table('file.parquet').num_rows)"
```
