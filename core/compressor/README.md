# QuantLab Parquet Compaction Job (Compressor)

Efficiently consolidates small Parquet files from `quantlab-raw` into daily `data.parquet` files in `quantlab-compact`.

## Key Features
- **State-Based Catch-Up**: Uses `compacted/_state.json` to track progress and automatically process missing days.
- **Fast Discovery (O(1) Startup)**: Uses S3 delimiters to walk directory prefixes instead of scanning all files.
- **Deterministic Order**: Adds a `seq` column (BIGINT, 0 to N-1) after `ts_event` for global monotonic ordering. 
- **Operational Audit**: The `_state.json` file includes `last_compacted_date` and `updated_at` timestamps for monitoring.
- **Partial Day Protection**: Automatically ignores today's data to avoid compacting incomplete days.

## Technical Specifications

### Sequence Column (`seq`)
seq provides a stable, monotonic intra-day ordering key that guarantees deterministic replay even when multiple events share the same ts_event value.

### Scheduling (02:30 UTC)
The job is scheduled at **02:30 UTC** for the following reasons:
- **Collector Alignment**: Ensures no collision with the current day's collection process.
- **Day Boundary**: Ensures the previous day is fully closed and all files are flushed to S3.
- **Minimal Risk**: Eliminates partial-day risks while providing fresh compacted data for early-day analysis.
- **Deterministic Context**: Consciously chosen to provide a stable operating window after global exchange activities settle for the specific UTC day.

### Fresh Start Behavior
If no `_state.json` is found (e.g., a cold deployment), the worker will KESİN OLARAK only process **yesterday (UTC)**. It will not attempt to catch up on all historical raw data unless the state file is manually seeded.

## Installation

1. Copy systemd units:
   ```bash
   sudo cp compressor/quantlab-compact.service /etc/systemd/system/
   sudo cp compressor/quantlab-compact.timer /etc/systemd/system/
   ```
2. Reload and enable:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable quantlab-compact.timer
   sudo systemctl start quantlab-compact.timer
   ```

## Monitoring
```bash
# Watch live logs
sudo journalctl -u quantlab-compact.service -f

# Check audit state
# s3://quantlab-compact/compacted/_state.json
```
