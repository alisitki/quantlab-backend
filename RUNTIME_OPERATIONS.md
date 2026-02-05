# Runtime Operations

This document describes operational procedures for running QuantLab services.

---

## Service Start Methods

### Systemd-Managed Services

| Service | Unit File | Start Command |
|---------|-----------|---------------|
| replayd | `quantlab-replayd.service` | `sudo systemctl start quantlab-replayd` |
| job-worker | `quantlab-worker.service` | `sudo systemctl start quantlab-worker` |
| console-ui | `quantlab-console-ui.service` | `sudo systemctl start quantlab-console-ui` |
| observer-api | `quantlab-observer.service` | `sudo systemctl start quantlab-observer` |

### NPM Start Services

| Service | Directory | Start Command |
|---------|-----------|---------------|
| strategyd | `services/strategyd` | `npm start` |
| backtestd | `services/backtestd` | `npm start` |
| featurexd | `services/featurexd` | `npm start` |
| labeld | `services/labeld` | `npm start` |

### Manual CLI Services

| Service | Start Command |
|---------|---------------|
| collector | `cd collector && python3 collector.py` |
| observer | `node core/observer/index.js` |

---

## Restart Commands

### Systemd Services

```bash
# Restart specific service
sudo systemctl restart quantlab-replayd
sudo systemctl restart quantlab-worker
sudo systemctl restart quantlab-console-ui
sudo systemctl restart quantlab-observer

# Restart all QuantLab services
sudo systemctl restart quantlab-replayd quantlab-worker quantlab-console-ui quantlab-observer

# Check status
sudo systemctl status quantlab-replayd
```

### NPM Services

```bash
# In service directory
npm start

# Or with PM2 (if configured)
pm2 restart strategyd
```

---

## Log Inspection

### Systemd Journal

```bash
# View recent logs
sudo journalctl -u quantlab-replayd -n 100

# Follow live logs
sudo journalctl -u quantlab-replayd -f

# Logs since today
sudo journalctl -u quantlab-replayd --since today

# All QuantLab logs
sudo journalctl -u "quantlab-*" -f
```

### Service-Specific Logs

| Service | Log Location |
|---------|--------------|
| replayd | `journalctl -u quantlab-replayd` |
| job-worker | `journalctl -u quantlab-worker` |
| console-ui | `journalctl -u quantlab-console-ui` |
| observer-api | `/home/deploy/quantlab-backend/core/observer-api/server.log` |
| collector | stdout (systemd journal if daemonized) |

### ML Scheduler Logs

```bash
# Cron logs
tail -f /var/log/quantlab-ml.log
```

---

## Environment Variable Locations

### Systemd Environment Files

| Service | Env File |
|---------|----------|
| replayd | `/etc/quantlab/replayd.env` |
| job-worker | `/etc/quantlab/worker.env` |
| console-ui | `/etc/quantlab/console.env` |

### Dotenv Files

| Component | Location |
|-----------|----------|
| Core services | `/home/deploy/quantlab-backend/core/.env` |
| Observer API | `/home/deploy/quantlab-backend/core/.env` (loaded from observer-api/config.ts) |

### Critical Environment Variables

| Variable | Purpose | Used By |
|----------|---------|---------|
| `REPLAYD_PORT` | Replay service port | replayd |
| `REPLAYD_TOKEN` | Auth token | replayd clients |
| `STRATEGYD_PORT` | Strategy service port | strategyd |
| `STRATEGYD_TOKEN` | Auth token | strategyd clients |
| `S3_COMPACT_BUCKET` | Data bucket | replayd, featurexd |
| `S3_COMPACT_ENDPOINT` | S3 endpoint | All S3 clients |
| `RUN_ARCHIVE_S3_BUCKET` | Archive bucket | RunArchiveWriter |
| `RUN_ARCHIVE_ENABLED` | Enable archival | strategyd |
| `OBSERVER_MODE` | Enable observer API | observer-api |
| `OBSERVER_TOKEN` | Observer auth | observer, observer-api |

### Memory Optimization Flags (ExecutionEngine)

**Added:** 2026-02-05

These flags enable streaming memory optimizations in ExecutionEngine. Default: OFF (for safety).

| Variable | Purpose | Default | Memory Impact |
|----------|---------|---------|---------------|
| `EXECUTION_STREAMING_MAXDD` | Enable O(1) maxDD calculation | `0` (OFF) | 88.8 MB → 24 bytes |
| `EXECUTION_STREAM_FILLS` | Stream fills to disk (JSONL) | `0` (OFF) | 190 MB → 10 KB |
| `EXECUTION_FILLS_STREAM_PATH` | Custom fills stream path | `/tmp/fills_{ts}.jsonl` | N/A |

**Usage:**
```bash
# Enable all optimizations (99.998% memory reduction)
export EXECUTION_STREAMING_MAXDD=1
export EXECUTION_STREAM_FILLS=1

# Run backtest
node core/strategy/v1/tests/test-strategy-v1.js
```

**Impact:**
- **Total Memory Reduction:** 558 MB → 10 KB (99.998%)
- **Accuracy Loss:** 0% (exact metrics match)
- **Disk I/O Overhead:** ~2-3 seconds for 3.7M events
- **Use Case:** Large backtests (3M+ events) on memory-constrained environments

**Verification:**
```bash
# Run memory optimization tests
node core/execution/tests/test-streaming-maxdd.js
node core/execution/tests/test-fills-stream.js
node core/execution/tests/test-fills-streaming-integration.js
```

---

## Service Dependency Startup Order

**Critical Path (must start in order):**

```
1. replayd          # No dependencies - core data provider
2. collector        # Independent - writes to spool
3. featurexd        # Depends on replayd for SSE
4. labeld           # Depends on featurexd datasets
5. strategyd        # Depends on replayd SSE
6. backtestd        # Spawns strategyd, depends on replayd
7. job-worker       # Depends on ledger, spawns strategyd scripts
8. observer         # Monitors strategyd runs
9. observer-api     # Read-only, depends on runs directory
10. console-ui      # Frontend, depends on backend APIs
```

**Safe Parallel Groups:**

```
Group 1 (Independent):
  - replayd
  - collector

Group 2 (After Group 1):
  - featurexd
  - strategyd

Group 3 (After Group 2):
  - labeld
  - backtestd
  - job-worker

Group 4 (After services ready):
  - observer
  - observer-api
  - console-ui
```

---

## Modules in Code but NOT Active in Runtime

### Not Integrated

| Module | Path | Status |
|--------|------|--------|
| RiskManager | `core/risk/RiskManager.js` | Complete but not wired to runtime |
| LiveStrategyRunner | `core/strategy/live/LiveStrategyRunner.js` | Exists but not exposed via HTTP service |
| Observer (core) | `core/observer/index.js` | Parallel implementation to observer-api |

### Experimental / Phase 4

| Module | Path | Status |
|--------|------|--------|
| LiveWSConsumer | `core/live-ws/LiveWSConsumer.js` | For live trading (not production) |
| LiveEventSequencer | `core/live-ws/LiveEventSequencer.js` | For live trading (not production) |
| PromotionGuardManager | `core/strategy/guards/PromotionGuardManager.js` | Live guards (not active) |
| RunBudgetManager | `core/strategy/limits/RunBudgetManager.js` | Budget limits (not active) |

### Legacy / Deprecated

| Module | Path | Status |
|--------|------|--------|
| FeatureBuilderV1 | `core/features/FeatureBuilderV1.js` | V1, parallel to orchestrated approach |
| observer-api (TypeScript) | `core/observer-api/` | Separate TypeScript implementation |

---

## Cron Jobs

### ML Training (Daily)

```cron
# Daily ML Training - Runs at 00:15 UTC
15 0 * * * cd /home/deploy/quantlab-backend/core && node scheduler/run_daily_ml.js >> /var/log/quantlab-ml.log 2>&1
```

### Available Scheduler Scripts

| Script | Purpose |
|--------|---------|
| `run_daily_ml.js` | Daily ML training orchestration |
| `run_daily_prod.js` | Daily production run |
| `run_daily_prod_dry.js` | Dry run (no GPU) |
| `run_ops_report.js` | Generate ops report |
| `check_compact_ready.js` | Check S3 data readiness |
| `cleanup_retention.js` | Clean old data |
| `generate_daily_health.js` | Generate health report |

---

## Health Check Commands

### Service Health Endpoints

```bash
# Replayd
curl -s http://localhost:3030/health

# Strategyd
curl -H "Authorization: Bearer $STRATEGYD_TOKEN" http://localhost:3031/health

# Backtestd
curl -H "Authorization: Bearer $BACKTESTD_TOKEN" http://localhost:3041/health

# Featurexd
curl -H "Authorization: Bearer $FEATUREXD_TOKEN" http://localhost:3051/health

# Labeld
curl -H "Authorization: Bearer $LABELD_TOKEN" http://localhost:3061/health

# Collector
curl -s http://localhost:9100/health

# Observer API
curl -s http://localhost:3000/ping
```

### Quick Health Check Script

```bash
#!/bin/bash
echo "=== QuantLab Health Check ==="

# Check systemd services
for svc in quantlab-replayd quantlab-worker quantlab-console-ui; do
    status=$(systemctl is-active $svc 2>/dev/null || echo "not-found")
    echo "$svc: $status"
done

# Check HTTP endpoints
curl -s http://localhost:3030/health | jq -r '.status // "FAIL"' | xargs echo "replayd:"
curl -s http://localhost:9100/health | jq -r '.status // "FAIL"' | xargs echo "collector:"
```

---

## Common Operations

### Restart After Deploy

```bash
# 1. Pull latest code
cd /home/deploy/quantlab-backend
git pull

# 2. Restart systemd services
sudo systemctl restart quantlab-replayd quantlab-worker quantlab-console-ui

# 3. Verify health
curl -s http://localhost:3030/health
```

### View Active Runs

```bash
# Observer API
curl -s http://localhost:3000/api/runs | jq

# Direct file listing
ls -la /home/deploy/quantlab-backend/services/strategyd/runs/*.json | head -20
```

### Check Collector Status

```bash
curl -s http://localhost:9100/status | jq
curl -s http://localhost:9100/metrics | jq
```

### Force ML Training Run

```bash
cd /home/deploy/quantlab-backend/core
node scheduler/run_daily_ml.js --symbol btcusdt --date 20260115 --dry-run
```

---

## Troubleshooting

### Service Won't Start

```bash
# Check logs
sudo journalctl -u quantlab-replayd -n 50

# Check port in use
sudo lsof -i :3030

# Check env file exists
cat /etc/quantlab/replayd.env
```

### Replay Not Returning Data

```bash
# Check S3 connectivity
node core/replay/tools/verify-credentials.js

# Check dataset exists
aws s3 ls s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20260115/
```

### Collector Not Writing

```bash
# Check spool directory
ls -la /opt/quantlab/spool/

# Check queue status
curl -s http://localhost:9100/metrics | jq '.queue_size'

# Check writer stats
curl -s http://localhost:9100/metrics | jq '.writer'
```

---

*This document describes operational reality as extracted from the codebase.*
