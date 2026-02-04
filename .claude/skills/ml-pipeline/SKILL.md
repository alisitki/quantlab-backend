---
name: ml-pipeline
description: Vast.ai GPU orchestration, XGBoost training, and model promotion
---

# ML Pipeline

This skill covers the ML training infrastructure using Vast.ai GPU instances.

## Architecture Overview

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  run_daily_ml   │────▶│ VastClient   │────▶│ GPU Instance    │
│  (scheduler)    │     │ (provision)  │     │ (Vast.ai)       │
└─────────────────┘     └──────────────┘     └─────────────────┘
         │                                            │
         │                                            │
         ▼                                            ▼
┌─────────────────┐                          ┌─────────────────┐
│ RemoteJobRunner │◀─────── SSH ─────────────│ XGBoost Train   │
│ (orchestrate)   │                          │ (Python)        │
└─────────────────┘                          └─────────────────┘
         │
         ▼
┌─────────────────┐
│ Model Promotion │
│ (metrics check) │
└─────────────────┘
```

---

## Key Files

| File | Purpose |
|------|---------|
| `core/scheduler/run_daily_ml.js` | Daily ML orchestration |
| `core/vast/VastClient.js` | Vast.ai API client |
| `core/vast/RemoteJobRunner.js` | Remote job execution via SSH |
| `core/vast/reap_orphans.js` | Cleanup orphan instances |
| `core/ml/` | ML training scripts (Python) |
| `core/promotion/` | Model promotion logic |

---

## Daily ML Flow

```bash
# Trigger daily ML training
cd /home/deploy/quantlab-backend/core
node scheduler/run_daily_ml.js --symbol btcusdt

# Dry run (no GPU provisioning)
node scheduler/run_daily_ml.js --symbol btcusdt --dry-run
```

### Flow Steps

1. **Check data readiness** — Verify compact data exists for date
2. **Generate job spec** — `JobSpecGenerator.js`
3. **Provision GPU instance** — `VastClient.searchOffers()` + `createInstance()`
4. **Wait for SSH ready** — Retry with backoff
5. **Execute training** — `RemoteJobRunner.run()`
6. **Download artifacts** — Model, metrics, logs
7. **Evaluate promotion** — Compare with production model
8. **Destroy instance** — Always cleanup

---

## VastClient API

```javascript
import { VastClient } from './VastClient.js';

const client = new VastClient(apiKey);

// Search for GPU offers
const offers = await client.searchOffers({
  gpu_name: 'RTX 4090',
  num_gpus: 1,
  disk_space: 50
});

// Create instance
const instance = await client.createInstance(offerId, {
  image: 'pytorch/pytorch:2.0-cuda11.7-runtime',
  disk: 50,
  onstart: 'bash /workspace/setup.sh'
});

// Get instance status
const status = await client.getInstanceStatus(instanceId);

// Destroy instance
await client.destroyInstance(instanceId);
```

---

## RemoteJobRunner

```javascript
import { RemoteJobRunner } from './RemoteJobRunner.js';

const runner = new RemoteJobRunner({
  host: instance.ssh_host,
  port: instance.ssh_port,
  username: 'root',
  privateKey: fs.readFileSync(keyPath)
});

// Execute job
const result = await runner.run({
  script: '/workspace/train.py',
  args: ['--symbol', 'btcusdt', '--date', '20260115'],
  timeout: 3600000  // 1 hour
});
```

---

## Orphan Instance Reaping

Vast.ai instances can become orphaned if script crashes.

```bash
# Check for orphan instances
node core/vast/reap_orphans.js --dry-run

# Actually destroy orphans
node core/vast/reap_orphans.js
```

---

## Model Promotion

Models are promoted if metrics improve over production:

| Metric | Threshold |
|--------|-----------|
| Directional accuracy | Must improve |
| Max drawdown | Must not worsen significantly |
| Sharpe ratio | Should improve |

```javascript
// Promotion logic
if (candidate.accuracy > production.accuracy &&
    candidate.maxDrawdown <= production.maxDrawdown * 1.1) {
  await promoteModel(candidate);
}
```

---

## S3 Model Storage

```
s3://${S3_COMPACT_BUCKET}/models/
├── production/
│   └── btcusdt/
│       ├── model.bin
│       └── metrics.json
└── candidates/
    └── btcusdt/
        └── 2026-02-03/
            ├── model.bin
            └── metrics.json
```

---

## Cron Configuration

```cron
# Daily ML Training - 00:15 UTC
15 0 * * * cd /home/deploy/quantlab-backend/core && node scheduler/run_daily_ml.js >> /var/log/quantlab-ml.log 2>&1
```

---

## Troubleshooting

### Instance Won't Start
```bash
# Check Vast.ai API key
echo $VAST_API_KEY

# Check offers availability
node -e "require('./VastClient').searchOffers().then(console.log)"
```

### SSH Connection Failed
```bash
# Test SSH manually
ssh -i ~/.ssh/vast_key -p $PORT root@$HOST
```

### Training Crashed
```bash
# Check remote logs
ssh root@$HOST "cat /workspace/train.log"
```
