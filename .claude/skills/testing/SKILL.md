---
name: testing
description: Verification scripts and testing strategies for system integrity
---

# Testing

This skill covers verification methods for ensuring system integrity.

## Verification Scripts Inventory

| Script | Purpose | Command |
|--------|---------|---------|
| `verify-determinism.js` | Replay determinism | `node core/replay/tools/verify-determinism.js` |
| `verify-v1.js` | Replay validation | `node core/replay/tools/verify-v1.js` |
| `verify-resume.js` | Cursor resume | `node core/replay/tools/verify-resume.js` |
| `verify-cursor.js` | Cursor encoding | `node core/replay/tools/verify-cursor.js` |
| `verify-live-parity.js` | Replay/live parity | `node tools/verify-live-parity.js` |
| `verify-audit-trail.js` | Audit records | `node tools/verify-audit-trail.js` |
| `go-live-check.js` | Pre-flight checks | `node tools/go-live-check.js` |

---

## Quick Health Check

```bash
#!/bin/bash
echo "=== QuantLab Health Check ==="

# Check systemd services
for svc in quantlab-replayd quantlab-worker quantlab-console-ui; do
    status=$(systemctl is-active $svc 2>/dev/null || echo "not-found")
    echo "$svc: $status"
done

# Check HTTP endpoints
for port in 3030 3031 3041 3051 3061 9100; do
    status=$(curl -s http://localhost:$port/health | jq -r '.status // "FAIL"')
    echo "Port $port: $status"
done
```

---

## Determinism Verification

**Purpose:** Verify same input → same output

```bash
node core/replay/tools/verify-determinism.js
```

**Expected Output:**
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

**Pass Criteria:**
- First 100 hash MATCH: true
- Last 100 hash MATCH: true
- OUT_OF_ORDER_COUNT: 0

---

## Twin-Run Verification

**Purpose:** Two strategy runs with identical inputs produce identical outputs

```javascript
import { compareTwinRuns } from './DeterminismValidator.js';

const result = compareTwinRuns(run1, run2);
console.log(result.match);  // true if deterministic
```

**Pass Criteria:**
- `stateHash` match
- `fillsHash` match
- `eventCount` match

---

## Live Parity Test

**Purpose:** Verify replay and live paths produce same decisions

```bash
node tools/verify-live-parity.js \
    --parquet /path/to/data.parquet \
    --meta /path/to/meta.json \
    --strategy /path/to/strategy.js \
    --seed test-seed
```

**Pass Criteria:**
```
decision_count_match: true
decision_hash_match: true
PASS: true
```

---

## Go-Live Checks

**Purpose:** Pre-flight verification before live trading

```bash
node tools/go-live-check.js
```

**Checks Performed:**
1. Config Check — All required env vars present
2. S3 Check — Can read from archive bucket
3. Determinism Check — Twin-run verification passes
4. Parity Check — Replay/live parity verified

**Output:**
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

---

## Service Health Endpoints

| Service | Endpoint | Expected |
|---------|----------|----------|
| replayd | `GET /health` | `{"status":"ok"}` |
| strategyd | `GET /health` | `{"status":"ok"}` |
| backtestd | `GET /health` | `{"status":"ok"}` |
| featurexd | `GET /health` | `{"status":"ok"}` |
| labeld | `GET /health` | `{"status":"ok"}` |
| collector | `GET /health` | `{"status":"ok"}` |
| observer-api | `GET /ping` | `{"status":"pong"}` |

---

## After Code Modifications

1. Verify imports resolve: `node --check <file>`
2. Run relevant verification script
3. Confirm determinism-critical files unchanged
4. Update SYSTEM_STATE.json if needed
