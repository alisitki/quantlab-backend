---
name: operations
description: Runtime operations, service management, and deployment procedures
---

# Operations

This skill covers runtime operations for QuantLab services.

## Service Overview

| Service | Port | Type | Start Command |
|---------|------|------|---------------|
| replayd | 3030 | Systemd | `sudo systemctl start quantlab-replayd` |
| strategyd | 3031 | NPM | `cd services/strategyd && npm start` |
| backtestd | 3041 | NPM | `cd services/backtestd && npm start` |
| featurexd | 3051 | NPM | `cd services/featurexd && npm start` |
| labeld | 3061 | NPM | `cd services/labeld && npm start` |
| collector | 9100 | Python | `cd collector && python3 collector.py` |
| observer-api | 3000 | Systemd | `sudo systemctl start quantlab-observer` |
| job-worker | — | Systemd | `sudo systemctl start quantlab-worker` |
| console-ui | — | Systemd | `sudo systemctl start quantlab-console-ui` |

---

## Systemd Commands

```bash
# Restart specific service
sudo systemctl restart quantlab-replayd

# Check status
sudo systemctl status quantlab-replayd

# View logs
sudo journalctl -u quantlab-replayd -n 100

# Follow live logs
sudo journalctl -u quantlab-replayd -f

# Restart all QuantLab services
sudo systemctl restart quantlab-replayd quantlab-worker quantlab-console-ui quantlab-observer
```

---

## Startup Order

**Critical Path (must start in order):**

```
1. replayd          # Core data provider
2. collector        # Independent, writes to spool
3. featurexd        # Depends on replayd SSE
4. labeld           # Depends on featurexd
5. strategyd        # Depends on replayd SSE
6. backtestd        # Spawns strategyd
7. job-worker       # Spawns strategy scripts
8. observer         # Monitors runs
9. observer-api     # Read-only API
10. console-ui      # Frontend
```

---

## Environment Variables

### Systemd Env Files
| Service | File |
|---------|------|
| replayd | `/etc/quantlab/replayd.env` |
| job-worker | `/etc/quantlab/worker.env` |
| console-ui | `/etc/quantlab/console.env` |

### Dotenv
| Component | Location |
|-----------|----------|
| Core | `/home/deploy/quantlab-backend/core/.env` |

### Critical Variables
| Variable | Purpose |
|----------|---------|
| `S3_COMPACT_BUCKET` | Data bucket |
| `S3_COMPACT_ENDPOINT` | S3 endpoint |
| `RUN_ARCHIVE_S3_BUCKET` | Archive bucket |
| `RUN_ARCHIVE_ENABLED` | Enable archival |
| `*_TOKEN` | Auth tokens |

---

## Log Locations

| Service | Log Location |
|---------|--------------|
| replayd | `journalctl -u quantlab-replayd` |
| job-worker | `journalctl -u quantlab-worker` |
| observer-api | `core/observer-api/server.log` |
| ML scheduler | `/var/log/quantlab-ml.log` |

---

## Health Checks

```bash
# Replayd
curl -s http://localhost:3030/health

# Strategyd (with auth)
curl -H "Authorization: Bearer $STRATEGYD_TOKEN" http://localhost:3031/health

# Collector
curl -s http://localhost:9100/health

# Observer API
curl -s http://localhost:3000/ping
```

---

## Common Operations

### Restart After Deploy
```bash
cd /home/deploy/quantlab-backend
git pull
sudo systemctl restart quantlab-replayd quantlab-worker quantlab-console-ui
curl -s http://localhost:3030/health
```

### View Active Runs
```bash
curl -s http://localhost:3000/api/runs | jq
ls -la services/strategyd/runs/*.json | head -20
```

### Check Collector Status
```bash
curl -s http://localhost:9100/status | jq
curl -s http://localhost:9100/metrics | jq
```

### Force ML Training Run
```bash
cd core
node scheduler/run_daily_ml.js --symbol btcusdt --date 20260115 --dry-run
```

---

## Troubleshooting

### Service Won't Start
```bash
sudo journalctl -u quantlab-replayd -n 50
sudo lsof -i :3030
cat /etc/quantlab/replayd.env
```

### Replay Not Returning Data
```bash
node core/replay/tools/verify-credentials.js
aws s3 ls s3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20260115/
```

### Collector Not Writing
```bash
ls -la /opt/quantlab/spool/
curl -s http://localhost:9100/metrics | jq '.queue_size'
```
