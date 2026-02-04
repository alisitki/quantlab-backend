# Testing Strategy

This document defines how to verify system integrity for QuantLab.

---

## Verification Methods Overview

| Method | Purpose | Location |
|--------|---------|----------|
| Determinism Verification | Same input → same output | `core/replay/tools/verify-determinism.js` |
| Twin-Run Verification | Two runs produce identical hashes | `core/strategy/safety/DeterminismValidator.js` |
| Replay Validation | Event order is correct | `core/replay/tools/verify-v1.js` |
| Cursor Resume | Resume from cursor produces consistent results | `core/replay/tools/verify-resume.js` |
| Hash Comparison | State/fills/decisions match | `tools/verify-live-parity.js` |
| Live Parity | Replay and live produce same decisions | `tools/verify-live-parity.js` |
| Go-Live Checks | Pre-flight verification | `tools/go-live-check.js` |

---

## Determinism Verification

### Purpose

Verify that replaying the same dataset produces identical event sequences across multiple runs.

### Script

```bash
node core/replay/tools/verify-determinism.js
```

### Method

1. Run replay twice on same dataset
2. Hash first 100 events
3. Hash last 100 events
4. Compare hashes
5. Count out-of-order events

### Expected Output

```
=== DETERMINISM TEST ===

--- FIRST 100 HASH ---
RUN A: a1b2c3d4...
RUN B: a1b2c3d4...
MATCH: true

--- LAST 100 HASH ---
RUN A: e5f6g7h8...
RUN B: e5f6g7h8...
MATCH: true

--- ORDER CHECK ---
OUT_OF_ORDER_COUNT: 0
```

### Pass Criteria

- First 100 hash MATCH: true
- Last 100 hash MATCH: true
- OUT_OF_ORDER_COUNT: 0

---

## Twin-Run Verification

### Purpose

Verify that two strategy runs with identical inputs produce identical outputs.

### Code Location

`core/strategy/safety/DeterminismValidator.js`

### API

```javascript
import { compareTwinRuns, computeStateHash, computeFillsHash } from './DeterminismValidator.js';

// After run 1
const run1 = {
    stateHash: computeStateHash(runtime1.getSnapshot()),
    fillsHash: computeFillsHash(runtime1.getSnapshot().fills),
    eventCount: runtime1.state.eventCount
};

// After run 2
const run2 = {
    stateHash: computeStateHash(runtime2.getSnapshot()),
    fillsHash: computeFillsHash(runtime2.getSnapshot().fills),
    eventCount: runtime2.state.eventCount
};

// Compare
const result = compareTwinRuns(run1, run2);
console.log(result.match);  // true if deterministic
console.log(result.details);
```

### Pass Criteria

- `stateHash` match
- `fillsHash` match
- `eventCount` match

---

## Replay Validation

### Script

```bash
node core/replay/tools/verify-v1.js
```

### What It Checks

1. Meta.json loads correctly
2. Parquet schema matches expected
3. Row count matches meta
4. Events are in correct order (ts_event ASC, seq ASC)
5. No duplicate (ts_event, seq) tuples

### Pass Criteria

- All validations pass
- No ordering violations
- Row count matches meta.row_count

---

## Cursor Resume Verification

### Script

```bash
node core/replay/tools/verify-resume.js
```

### What It Checks

1. Full replay: 0 → N events
2. Partial replay: 0 → M events, capture cursor
3. Resume replay: cursor → N events
4. Verify full events == partial + resume events

### Pass Criteria

- Event sequences concatenate correctly
- No events skipped
- No events duplicated

---

## Hash Comparison Logic

### State Hash

```javascript
function computeStateHash(state) {
    const normalized = {
        cursor: state.cursor || null,
        executionState: state.executionState || null,
        strategyState: state.strategyState || null
    };
    return sha256(canonicalStringify(normalized));
}
```

### Fills Hash

```javascript
function computeFillsHash(fills) {
    const normalized = fills.map(fill => ({
        id: fill.id,
        side: fill.side,
        price: fill.fillPrice,
        qty: fill.qty,
        ts: String(fill.ts_event)
    }));
    return sha256(canonicalStringify(normalized));
}
```

### Run ID Hash

```javascript
function computeRunId({ dataset, config, seed }) {
    const input = {
        dataset: { parquet, meta, stream, date, symbol },
        config,
        seed
    };
    return `run_${sha256(canonicalStringify(input)).substring(0, 16)}`;
}
```

---

## Live Parity Test

### Script

```bash
node tools/verify-live-parity.js \
    --parquet /path/to/data.parquet \
    --meta /path/to/meta.json \
    --strategy /path/to/strategy.js \
    --seed test-seed
```

### What It Does

1. Runs strategy through `StrategyRuntime.processReplay()`
2. Runs same strategy through `LiveStrategyRunner` with same events
3. Compares `decision_count` and `decision_hash`

### Pass Criteria

```
decision_count_match: true
decision_hash_match: true
PASS: true
```

---

## Go-Live Checks

### Script

```bash
node tools/go-live-check.js
```

### Checks Performed

1. **Config Check** - All required env vars present
2. **S3 Check** - Can read from archive bucket
3. **Determinism Check** - Twin-run verification passes
4. **Parity Check** - Replay/live parity verified

### Output Format

```json
{
    "go_live": true,
    "checks": {
        "config": "ok",
        "s3": "ok",
        "determinism": "ok",
        "parity": "ok"
    }
}
```

### Failure Output

```json
{
    "go_live": false,
    "failed_step": "s3",
    "reason": "BUCKET_NOT_FOUND",
    "details": { ... }
}
```

---

## Service Health Checks

### Endpoints

| Service | Endpoint | Expected |
|---------|----------|----------|
| replayd | `GET /health` | `{"status":"ok"}` |
| strategyd | `GET /health` | `{"status":"ok"}` |
| backtestd | `GET /health` | `{"status":"ok"}` |
| featurexd | `GET /health` | `{"status":"ok"}` |
| labeld | `GET /health` | `{"status":"ok"}` |
| collector | `GET /health` | `{"status":"ok"}` |
| observer-api | `GET /ping` | `{"status":"pong"}` |

### Health Check Script

```bash
#!/bin/bash
for port in 3030 3031 3041 3051 3061 9100; do
    status=$(curl -s http://localhost:$port/health | jq -r '.status // "FAIL"')
    echo "Port $port: $status"
done
```

---

## ML Inference Test Paths

### Model Training Verification

```bash
# Dry run (no GPU)
node core/scheduler/run_daily_ml.js --symbol btcusdt --dry-run

# Verify job spec generation
node core/scheduler/test-scheduler.js
```

### Model Artifacts Check

```bash
# Check model exists in S3
aws s3 ls s3://${S3_COMPACT_BUCKET}/models/production/btcusdt/

# Expected files:
# - model.bin
# - metrics.json
```

### Feature Extraction Test

```bash
# Verify features dataset exists
aws s3 ls s3://${S3_COMPACT_BUCKET}/features/featureset=v1/exchange=binance/stream=bbo/symbol=btcusdt/date=20260115/
```

---

## Verification Scripts Inventory

| Script | Purpose | Usage |
|--------|---------|-------|
| `verify-determinism.js` | Replay determinism | `node core/replay/tools/verify-determinism.js` |
| `verify-v1.js` | Replay validation | `node core/replay/tools/verify-v1.js` |
| `verify-resume.js` | Cursor resume | `node core/replay/tools/verify-resume.js` |
| `verify-cursor.js` | Cursor encoding | `node core/replay/tools/verify-cursor.js` |
| `verify-hard.js` | Hard verification | `node core/replay/tools/verify-hard.js` |
| `verify-multipartition.js` | Multi-file replay | `node core/replay/tools/verify-multipartition.js` |
| `verify-credentials.js` | S3 credentials | `node core/replay/tools/verify-credentials.js` |
| `verify-run-archive.js` | Archive integrity | `node core/replay/tools/verify-run-archive.js` |
| `verify-replay-strategy.js` | Strategy replay | `node core/replay/tools/verify-replay-strategy.js` |
| `verify-live-parity.js` | Replay/live parity | `node tools/verify-live-parity.js` |
| `verify-audit-trail.js` | Audit records | `node tools/verify-audit-trail.js` |
| `go-live-check.js` | Pre-flight | `node tools/go-live-check.js` |

---

## Test Data Requirements

### Minimum Test Dataset

- At least 10,000 events
- Multiple time windows
- BBO stream (for execution tests)
- Valid meta.json

### Test Data Location

```
/tmp/replay-test/data.parquet
/tmp/replay-test/meta.json
```

Or S3:
```
s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20260115/
```

---

## Continuous Verification

### Recommended Cron Jobs

```cron
# Daily determinism check
0 1 * * * node /home/deploy/quantlab-backend/core/replay/tools/verify-determinism.js >> /var/log/quantlab-verify.log 2>&1

# Weekly full verification
0 2 * * 0 node /home/deploy/quantlab-backend/tools/go-live-check.js >> /var/log/quantlab-verify.log 2>&1
```

---

*This testing strategy ensures system integrity through determinism verification and hash comparison.*
