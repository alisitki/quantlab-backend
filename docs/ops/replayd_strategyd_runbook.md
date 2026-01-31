# Replayd & Strategyd Operational Runbook

This guide covers the deployment, management, and troubleshooting of `replayd` and `strategyd` services.

## 🚀 Installation & Setup

### 1. Requirements
- Node.js v18+
- Hardware: Hetzner CPX21 (4 vCPU, 8GB RAM) or similar.

### 2. Deploy Code
Clone the repository to `/opt/quantlab-backend`:
```bash
git clone https://github.com/alisitki/quantlab-backend /opt/quantlab-backend
cd /opt/quantlab-backend
npm install
```

### 3. Environment Configuration
Create the configuration directory and copy templates:
```bash
sudo mkdir -p /etc/quantlab
sudo cp deploy/env/replayd.env.example /etc/quantlab/replayd.env
sudo cp deploy/env/strategyd.env.example /etc/quantlab/strategyd.env
# Edit the files with production values
sudo nano /etc/quantlab/replayd.env
```

### 4. systemd Installation
```bash
sudo cp deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable replayd strategyd
```

---

## 🛠️ Service Management

### Start / Stop / Restart
```bash
sudo systemctl start replayd strategyd
sudo systemctl stop strategyd
sudo systemctl restart strategyd
```

### Check Status
```bash
systemctl status replayd
systemctl status strategyd
```

### View Logs
```bash
# Tail logs for strategyd
journalctl -u strategyd -f

# View errors only
journalctl -u strategyd -p err
```

---

### Health Checks (Public)
```bash
curl http://localhost:3030/health
curl http://localhost:3031/health
```

### Metrics (Protected)
```bash
curl -H "Authorization: Bearer mytoken" http://localhost:3030/metrics
curl -H "Authorization: Bearer mytoken" http://localhost:3031/metrics
```

### Active Runs & State (Protected)
```bash
curl -H "Authorization: Bearer mytoken" http://localhost:3031/runs
curl -H "Authorization: Bearer mytoken" http://localhost:3031/state
curl -H "Authorization: Bearer mytoken" http://localhost:3031/trades
```


---

## 🚨 Common Failures

| Symptom | Probable Cause | Action |
|---------|----------------|--------|
| `EADDRINUSE` | Service already running or port leak | `pkill -f node` and restart systemd |
| `STALLED` Log | Replayd stopped sending data | Restart `replayd` then `strategyd` |
| `S3_403` Error | Credentials expired or missing | Update `/etc/quantlab/replayd.env` |
| `High Reconnects` | Network instability between services | Check `REPLAYD_URL` latency |

---

## 🛑 Graceful Shutdown
Both services handle `SIGTERM`. When stopped via systemd, `strategyd` will:
1. Stop the runner.
2. Finalize the `run_id.json` manifest with `ended_reason: interrupted`.
3. Close the Fastify server.
