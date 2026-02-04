---
name: services
description: Microservice architecture, ports, and inter-service communication
---

# Services

This skill covers the QuantLab microservice architecture.

## Service Map

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend                                 │
│  ┌─────────────┐                                                │
│  │ console-ui  │ ─────────────────────────────────────┐        │
│  │   (React)   │                                      │        │
│  └─────────────┘                                      ▼        │
├───────────────────────────────────────────────────────────────┤
│                        API Layer                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │
│  │ observer-api│  │  strategyd  │  │     backtestd       │   │
│  │   :3000     │  │   :3031     │  │       :3041         │   │
│  └─────────────┘  └─────────────┘  └─────────────────────┘   │
├───────────────────────────────────────────────────────────────┤
│                      Core Services                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           │
│  │   replayd   │  │  featurexd  │  │    labeld   │           │
│  │   :3030     │  │   :3051     │  │    :3061    │           │
│  └─────────────┘  └─────────────┘  └─────────────┘           │
├───────────────────────────────────────────────────────────────┤
│                      Data Layer                                │
│  ┌─────────────┐  ┌─────────────┐                             │
│  │  collector  │  │  compressor │                             │
│  │   :9100     │  │  (batch)    │                             │
│  └─────────────┘  └─────────────┘                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Port Mapping

| Service | Port | Protocol | Auth |
|---------|------|----------|------|
| replayd | 3030 | HTTP + SSE | `REPLAYD_TOKEN` |
| strategyd | 3031 | HTTP | `STRATEGYD_TOKEN` |
| backtestd | 3041 | HTTP | `BACKTESTD_TOKEN` |
| featurexd | 3051 | HTTP | `FEATUREXD_TOKEN` |
| labeld | 3061 | HTTP | `LABELD_TOKEN` |
| collector | 9100 | HTTP | None |
| observer-api | 3000 | HTTP | `OBSERVER_TOKEN` (some routes) |
| console-ui | 3001 | HTTP | None (frontend) |

---

## Service Details

### replayd
**Purpose:** Replay engine HTTP service
**Location:** `services/replayd/`

Key endpoints:
- `GET /health` — Health check
- `GET /stream/sse` — SSE event stream
- `POST /cursor` — Cursor operations

### strategyd
**Purpose:** Strategy execution service
**Location:** `services/strategyd/`

Key endpoints:
- `GET /health` — Health check
- `POST /run` — Start strategy run
- `POST /control` — Kill/pause/resume run
- `GET /runs` — List runs

### backtestd
**Purpose:** Backtest orchestration
**Location:** `services/backtestd/`

Key endpoints:
- `POST /backtest` — Start backtest
- `GET /backtest/:id` — Get backtest status

### featurexd
**Purpose:** Feature extraction
**Location:** `services/featurexd/`

Key endpoints:
- `POST /extract` — Extract features
- `GET /datasets` — List feature datasets

### labeld
**Purpose:** Label generation
**Location:** `services/labeld/`

Key endpoints:
- `POST /generate` — Generate labels
- `GET /datasets` — List label datasets

---

## Systemd Units

| Service | Unit File |
|---------|-----------|
| replayd | `ops/systemd/quantlab-replayd.service` |
| job-worker | `ops/systemd/quantlab-worker.service` |
| console-ui | `ops/systemd/quantlab-console-ui.service` |
| observer-api | `core/observer-api/quantlab-observer.service` |
| compactor | `core/compressor/quantlab-compact.service` |

---

## Authentication

All services use Bearer token authentication:

```bash
curl -H "Authorization: Bearer $STRATEGYD_TOKEN" http://localhost:3031/health
```

Factory: `core/common/authMiddlewareFactory.js`

---

## Inter-Service Communication

```
strategyd ──SSE──▶ replayd      # Get event stream
backtestd ──HTTP──▶ strategyd   # Spawn strategy runs
featurexd ──SSE──▶ replayd      # Get data for features
labeld ──HTTP──▶ featurexd      # Get feature datasets
observer-api ──File──▶ runs/    # Read run manifests
```

---

## Starting Services

### Systemd (Production)
```bash
sudo systemctl start quantlab-replayd
sudo systemctl start quantlab-worker
sudo systemctl start quantlab-observer
```

### NPM (Development)
```bash
cd services/strategyd && npm start
cd services/backtestd && npm start
cd services/featurexd && npm start
cd services/labeld && npm start
```

---

## Health Check Script

```bash
#!/bin/bash
echo "=== Service Health ==="

# Systemd services
for svc in quantlab-replayd quantlab-worker quantlab-observer; do
    echo -n "$svc: "
    systemctl is-active $svc 2>/dev/null || echo "not-found"
done

# HTTP endpoints
echo ""
echo "=== HTTP Health ==="
curl -s http://localhost:3030/health | jq -r '.status // "FAIL"' | xargs echo "replayd:"
curl -s http://localhost:3000/ping | jq -r '.status // "FAIL"' | xargs echo "observer:"
curl -s http://localhost:9100/health | jq -r '.status // "FAIL"' | xargs echo "collector:"
```

---

## Logs

| Service | Command |
|---------|---------|
| replayd | `journalctl -u quantlab-replayd -f` |
| strategyd | `tail -f services/strategyd/strategyd.log` |
| observer-api | `tail -f core/observer-api/server.log` |
