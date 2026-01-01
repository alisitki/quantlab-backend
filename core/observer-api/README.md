# QuantLab Observer API

READ-ONLY HTTP API for surfacing QuantLab telemetry for operators.

## Rules of Engagement
- **Strictly READ-ONLY**: No writes, no job triggers, no ML logic.
- **Fail-Closed**: Returns 404/empty if data is missing or corrupted.
- **Native Streams**: Uses Node.js streams to process large JSONL files without memory overhead.
- **S3 Source of Truth**: `/pipeline/status` uses direct S3 metadata checks (HeadObject) for data availability.

## Setup & Execution

### Pre-requisites
- Node.js v20+
- `OBSERVER_MODE=1` environment variable.
- Root `.env` with `S3_COMPACT_*` credentials.

### Start Server
```bash
cd observer-api
npm install
OBSERVER_MODE=1 npx tsx index.ts
```
Default bind: `0.0.0.0:3000` (External access enabled)

## Endpoints

### Health & Pipeline
- `GET /health/today` — Latest local health snapshot.
- `GET /pipeline/status?date=YYYYMMDD` — Aggregated status using S3 metadata as the source of truth.

### Gate Analytics
- `GET /gates/funnel?date=YYYYMMDD` — Funnel counts (Total/Pass/Reject/Skip) per gate stage.
- `GET /gates/reasons?date=YYYYMMDD` — Breakdown of reason codes per gate.

### Decision Traces
- `GET /decisions?date=YYYYMMDD` — List of unique decisions for the date with outcomes.
- `GET /decisions/:id/trace` — Ordered list of all events for a specific decision/intent ID.

### Alerts
- `GET /alerts?last=24h` — Recent alerts joined with outbox delivery status using `alert_id`.

## Example curl Outputs

### `/pipeline/status`
```json
{
  "date": "20251230",
  "compaction_status": "READY",
  "features_present": false,
  "shadow_run_status": "N/A",
  "sources": {
    "health_file": "health/daily_20251230.json",
    "shadow_file": null,
    "compact_head": "EXISTS",
    "features_head": "MISSING"
  }
}
```

---
**READ-ONLY verified; no side effects.**
