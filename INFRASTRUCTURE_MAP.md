# QuantLab Infrastructure Map

Generated: 2026-02-03

This document describes the operational topology of the QuantLab Backend system, derived from codebase analysis.

---

## Service Registry

| Service | Entry File | Port | Auth | Token Env | Managed By | Depends On |
|---------|-----------|------|------|-----------|------------|------------|
| replayd | `services/replayd/server.js` | 3030 | Yes | `REPLAYD_TOKEN` | systemd (`quantlab-replayd.service`) | None |
| strategyd | `services/strategyd/server.js` | 3031 | Yes | `STRATEGYD_TOKEN` | npm start / backtestd | replayd (SSE) |
| backtestd | `services/backtestd/server.js` | 3041 | Yes | `BACKTESTD_TOKEN` | npm start | replayd, strategyd |
| featurexd | `services/featurexd/server.js` | 3051 | Yes | `FEATUREXD_TOKEN` | npm start | replayd (SSE) |
| labeld | `services/labeld/server.js` | 3061 | Yes | `LABELD_TOKEN` | npm start | featurexd |
| collector | `collector/collector.py` | 9100 | No | N/A | manual / systemd | None |
| job-worker | `core/worker/job_worker.js` | N/A | N/A | N/A | systemd (`quantlab-worker.service`) | job ledger |
| console-ui | `console-ui/` (Next.js) | 3000 | No | N/A | systemd (`quantlab-console-ui.service`) | strategyd, replayd |
| observer | `core/observer/index.js` | 9150 | Yes | `OBSERVER_TOKEN` | npm start | strategyd |
| observer-api | `core/observer-api/index.ts` | 3000 | Partial | `OBSERVER_TOKEN` | systemd (`quantlab-observer.service`) | S3, runs |

### Port Summary

| Port | Service | Protocol |
|------|---------|----------|
| 3000 | console-ui / observer-api | HTTP |
| 3030 | replayd | HTTP + SSE |
| 3031 | strategyd | HTTP |
| 3041 | backtestd | HTTP |
| 3051 | featurexd | HTTP |
| 3061 | labeld | HTTP |
| 9100 | collector (status API) | HTTP |
| 9150 | observer | HTTP |

---

## Data Storage Map

| Data Type | Location | Format | Used By |
|-----------|----------|--------|---------|
| Collector Spool | `/opt/quantlab/spool` | Parquet (Hive partitioned) | collector (write), uploader (read) |
| Compact Data (Replay) | S3: `quantlab-compact` | Parquet + meta.json | replayd, featurexd |
| Run Archive | S3: `RUN_ARCHIVE_S3_BUCKET` | JSON (manifest, decisions, stats) | strategyd, RunArchiveWriter |
| Strategy Runs | `services/strategyd/runs/` | JSON manifests | strategyd, observer-api |
| Run Health | `services/strategyd/runs/health/` | JSON | strategyd |
| Run Summaries | `services/strategyd/runs/summary/` | JSON | strategyd |
| Run Archive (local) | `services/strategyd/runs/archive/` | JSON.gz | strategyd |
| Backtests | `services/strategyd/backtests/` | JSON | backtestd |
| Feature Datasets | `services/featurexd/datasets/` | Parquet | featurexd, labeld |
| Label Datasets | `services/labeld/datasets/` | Parquet | labeld |
| Quality Ledger | `/opt/quantlab/quality` | JSON (15m windows) | collector |
| Job Ledger | `core/research/jobs.jsonl` | JSONL | job-worker |
| Experiments | `services/strategyd/experiments/` | JSON | strategyd, observer-api |
| Candidates | `services/strategyd/candidates/` | JSON | strategyd, observer-api |

### S3 Bucket Configuration

| Bucket Purpose | Env Variable | Default |
|----------------|--------------|---------|
| Compact Data | `S3_COMPACT_BUCKET` | `quantlab-compact` |
| Run Archive | `RUN_ARCHIVE_S3_BUCKET` | (required) |

### S3 Credentials

| Purpose | Endpoint | Access Key | Secret Key |
|---------|----------|------------|------------|
| Compact | `S3_COMPACT_ENDPOINT` | `S3_COMPACT_ACCESS_KEY` | `S3_COMPACT_SECRET_KEY` |
| Archive | `RUN_ARCHIVE_S3_ENDPOINT` | `RUN_ARCHIVE_S3_ACCESS_KEY` | `RUN_ARCHIVE_S3_SECRET_KEY` |

---

## Auth Map

| Service | Accepts Bearer Token | Token Env Variable | Rate Limits |
|---------|---------------------|-------------------|-------------|
| replayd | Yes | `REPLAYD_TOKEN` | 120/min default, 30/min `/stream` |
| strategyd | Yes | `STRATEGYD_TOKEN` | 120/min default, 10/min `/control` |
| backtestd | Yes | `BACKTESTD_TOKEN` | 60/min default, 20/min `/backtests` |
| featurexd | Yes | `FEATUREXD_TOKEN` | 60/min default, 20/min `/features` |
| labeld | Yes | `LABELD_TOKEN` | 60/min default, 20/min `/labels` |
| observer | Yes | `OBSERVER_TOKEN` | 120/min default, 30/min `/stop` |
| observer-api | Yes (`/v1`, `/api`) | `OBSERVER_TOKEN` | N/A |
| collector | No | N/A | N/A |
| console-ui | No | N/A | N/A |
| job-worker | N/A | N/A | N/A |

### Auth Middleware

All Fastify services use the shared factory:

```
core/common/authMiddlewareFactory.js
```

Services import and configure:
- `services/*/middleware/auth.js` → `createAuthMiddleware({ tokenEnvVar, defaultLimit, pathLimits })`

Health endpoints (`/health`) bypass auth on all services.

---

## Service Boot Order

Safe startup sequence based on dependencies:

1. **replayd** — Core data provider, no dependencies
2. **collector** — Independent data ingestion
3. **featurexd** — Depends on replayd for replay data
4. **labeld** — Depends on featurexd for feature datasets
5. **strategyd** — Depends on replayd (SSE connection)
6. **backtestd** — Depends on replayd and spawns strategyd instances
7. **job-worker** — Depends on job ledger and strategyd scripts
8. **observer** — Monitors running strategyd instances
9. **observer-api** — Read-only API, depends on runs directory
10. **console-ui** — Frontend, depends on backend APIs

### Critical Path

```
replayd → strategyd → backtestd
       ↘ featurexd → labeld
```

### Systemd Order

For systemd-managed services, configure `After=` directives:

```ini
# quantlab-replayd.service
After=network.target

# quantlab-worker.service
After=network.target

# quantlab-console-ui.service
After=network.target

# quantlab-observer.service (in core/observer-api/)
After=network.target
```

---

## Network Classification

| Category | Services | Access Pattern |
|----------|----------|----------------|
| **Public API** | console-ui, observer-api | External (with auth for API routes) |
| **Internal API** | replayd, strategyd, backtestd, featurexd, labeld | Internal only (token-protected) |
| **Ops** | collector (status API), observer | Ops/monitoring only |
| **Background** | job-worker | No network exposure |

### Firewall Recommendations

| Port Range | Exposure | Services |
|------------|----------|----------|
| 3000 | Public (via reverse proxy) | console-ui, observer-api |
| 3030-3061 | Internal only | replayd, strategyd, backtestd, featurexd, labeld |
| 9100 | Internal only | collector |
| 9150 | Internal only | observer |

---

## Systemd Unit Files

| Unit File | Location | Service |
|-----------|----------|---------|
| `quantlab-replayd.service` | `ops/systemd/` | replayd |
| `quantlab-worker.service` | `ops/systemd/` | job-worker |
| `quantlab-console-ui.service` | `ops/systemd/` | console-ui |
| `quantlab-observer.service` | `core/observer-api/` | observer-api |

### Environment Files

Systemd units reference environment files:

| Service | Env File |
|---------|----------|
| replayd | `/etc/quantlab/replayd.env` |
| job-worker | `/etc/quantlab/worker.env` |
| console-ui | `/etc/quantlab/console.env` |

---

## Service Communication Patterns

### SSE (Server-Sent Events)

```
strategyd → replayd (SSE /stream endpoint)
featurexd → replayd (SSE /stream endpoint)
```

### HTTP

```
backtestd → replayd (HTTP /health, /meta)
backtestd → strategyd (spawns child processes)
labeld → featurexd (reads datasets directory)
console-ui → strategyd, replayd (HTTP APIs)
observer-api → runs directory (filesystem)
```

### Filesystem

```
collector → /opt/quantlab/spool (write)
uploader → /opt/quantlab/spool (read, delete)
collector → /opt/quantlab/quality (write)
job-worker → core/research/jobs.jsonl (read/write)
strategyd → services/strategyd/runs/ (write)
observer-api → services/strategyd/runs/ (read)
```

### S3

```
replayd → S3_COMPACT_BUCKET (read)
featurexd → S3_COMPACT_BUCKET (read)
strategyd → RUN_ARCHIVE_S3_BUCKET (write via ManifestArchiver)
RunArchiveWriter → RUN_ARCHIVE_S3_BUCKET (write)
```

---

## Write Protocol Summary

| Component | Write Target | Protocol |
|-----------|--------------|----------|
| collector/writer.py | spool | fsync + atomic rename |
| strategyd/ManifestManager | runs/*.json | direct write |
| RunArchiveWriter | S3 | PutObject |
| job-worker | jobs.jsonl | append |
| Quality Ledger | quality/*.json | atomic rename |

---

## Health Check Endpoints

All HTTP services expose health checks:

| Service | Endpoint | Response |
|---------|----------|----------|
| replayd | `GET /health` | `{ status: 'ok', service: 'replayd' }` |
| strategyd | `GET /health` | `{ status: 'ok', service: 'strategyd' }` |
| backtestd | `GET /health` | `{ status: 'ok', service: 'backtestd' }` |
| featurexd | `GET /health` | `{ status: 'ok', service: 'featurexd' }` |
| labeld | `GET /health` | `{ status: 'ok', service: 'labeld' }` |
| collector | `GET /health` | `{ status: 'ok', uptime: N }` |
| observer-api | `GET /health` | pipeline status |
| observer-api | `GET /ping` | `{ status: 'pong' }` |

---

*This document is auto-generated from codebase analysis. Do not manually edit.*
