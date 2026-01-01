# ML Scheduler & Vast GPU Orchestrator

Daily ML training orchestration system that launches ephemeral GPU instances on Vast.ai, executes training jobs, and promotes models that outperform production.

## Architecture

```
                    ┌──────────────────────────────────────┐
                    │         16GB VPS (Orchestrator)       │
                    │                                       │
                    │  ┌─────────────┐  ┌─────────────────┐ │
                    │  │ Scheduler   │  │ JobSpecGenerator│ │
                    │  │ (Cron/CLI)  │──│ (Deterministic) │ │
                    │  └─────────────┘  └─────────────────┘ │
                    │         │                              │
                    │         ▼                              │
                    │  ┌─────────────┐  ┌─────────────────┐ │
                    │  │ VastClient  │  │ RemoteJobRunner │ │
                    │  │ (REST API)  │──│ (SSH Execution) │ │
                    │  └─────────────┘  └─────────────────┘ │
                    │         │                 │           │
                    │         ▼                 ▼           │
                    │  ┌─────────────────────────────────┐  │
                    │  │         Promoter                │  │
                    │  │   (Compare & Promote Models)    │  │
                    │  └─────────────────────────────────┘  │
                    └──────────────────────────────────────┘
                              │                    │
         ┌────────────────────┼────────────────────┼───────────────┐
         ▼                    ▼                    ▼               ▼
  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  ┌────────────┐
  │  Vast.ai     │    │  Ephemeral   │    │     S3       │  │ Production │
  │  Marketplace │    │  GPU Instance│    │  Artifacts   │  │   Models   │
  └──────────────┘    └──────────────┘    └──────────────┘  └────────────┘
```

## Daily ML Lifecycle

1. **Scheduler Trigger**: Cron or manual invocation at 00:15 UTC
2. **JobSpec Generation**: Create deterministic job specs for each symbol
3. **GPU Search**: Find cheapest GPU meeting requirements on Vast.ai
4. **Instance Launch**: Create GPU instance with CUDA image
5. **Job Execution**: Clone repo, install deps, run training
6. **Artifact Upload**: Upload model.bin, metrics.json to S3
7. **GPU Destruction**: Always destroy instance (success or failure)
8. **Model Promotion**: Compare new metrics vs production, promote if better

## Feature-Based Training (v1.1)

The training flow now prioritizes pre-calculated features:
1. **Existence Check**: Scheduler checks S3 for `dataset.featurePath`.
2. **Optional Build**: If missing and `--ensure-features` is set, local `FeatureBuilderV1` is called before training.
3. **Remote Training**: GPU instance downloads features directly from S3 (bypassing on-the-fly calculation).

## Usage

### Dry Run (No GPU)
```bash
cd /home/deploy/quantlab/core
node scheduler/run_daily_ml.js --dry-run --symbol btcusdt
```

### Train Specific Symbol & Date
```bash
node scheduler/run_daily_ml.js --symbol btcusdt --date 20251228
```

### Full Daily Run (All Symbols)
```bash
node scheduler/run_daily_ml.js
```

### Command Line Options
| Option | Description |
|--------|-------------|
| `--symbol <symbol>` | Train specific symbol (default: all from config) |
| `--date <YYYYMMDD>` | Train on specific date (default: yesterday) |
| `--dry-run` | Generate JobSpecs without launching GPU |
| `--ensure-features`| Build features if missing on S3 (default: false) |
| `--featureset <v1>`| Feature set version to use (default: v1) |
| `--promote <mode>` | Promotion mode: `off` (default), `dry`, or `auto` |
| `--canary <bool>` | Force canary mode (downgrades `auto` to `dry`) |
| `--live` | Manual test run (treated as canary by default) |
| `--help` | Show help message |

## Cron Setup

Add to crontab (`crontab -e`):

```cron
# Daily ML Training - Runs at 00:15 UTC
15 0 * * * cd /home/deploy/quantlab/core && node scheduler/run_daily_ml.js >> /var/log/quantlab-ml.log 2>&1
```

## Configuration

Edit `scheduler/config.js`:

```javascript
export const SCHEDULER_CONFIG = {
  // Symbols to train daily
  defaultSymbols: ['btcusdt', 'ethusdt', 'solusdt'],
  
  // GPU requirements
  gpu: {
    preferredTypes: ['RTX_3090', 'RTX_4090', 'A100'],
    maxHourlyCost: 1.0,      // USD
    minGpuMemory: 16,        // GB
    maxRuntimeMin: 60        // Safety timeout
  },
  
  // Model config
  model: {
    type: 'xgboost',
    params: { nround: 100, maxDepth: 6 }
  }
};
```

## Model Promotion (Promotion Guard v2)

Promotion is now strictly guarded. Production models are only updated if `auto` mode is explicitly requested and the run is not a canary/live test.

### Promotion Modes
| Mode | Behavior |
|------|----------|
| `off` (default) | **Safe Default**: No evaluation, no S3 production writes. |
| `dry` | Calculates decision, logs "Dry Pass", but **skips S3 production writes**. |
| `auto` | Promotes to production prefix only if metrics pass and **not in canary/live mode**. |

### Canary & Live Test Rules
- **Canary Guard**: If `--canary true`, `--live`, or `RUN_MODE=canary` is detected, `auto` mode is automatically downgraded to `dry`.
- **Logic**: S3 production writes are consolidated in `#promoteModelS3` and guarded by these mode checks.

### Evaluation Rules (v1)
| Priority | Condition | Action |
|----------|-----------|--------|
| 1 | No production model exists | Promote (First model) |
| 2 | Higher `directionalHitRate` | Promote |
| 3 | Lower `directionalHitRate` | Reject |
| 4 | Equal hit rate, lower `maxDrawdown` | Promote (Tie-breaker) |
| 5 | Equal or worse on all metrics | Reject |

## Artifacts

Training produces these files in S3:
- `ml-artifacts/<jobId>/model.bin` - Trained model binary
- `ml-artifacts/<jobId>/metrics.json` - Evaluation metrics
- `ml-artifacts/<jobId>/runtime.json` - Execution timing/info
- `ml-artifacts/<jobId>/job.json` - Original job spec

Production models are stored at:
- `models/production/<symbol>/model.bin`
- `models/production/<symbol>/metrics.json`

## Testing

```bash
# Test JobSpecGenerator determinism
node scheduler/test-scheduler.js

# Test VastClient mock
node vast/test-vast-mock.js

# Test Promoter decision logic
node promotion/test-promoter.js
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VAST_API_KEY` | Vast.ai API key for GPU access |
| `S3_COMPACT_ENDPOINT` | S3 endpoint for artifact storage |
| `S3_COMPACT_BUCKET` | S3 bucket name |
| `S3_COMPACT_ACCESS_KEY` | S3 access key |
| `S3_COMPACT_SECRET_KEY` | S3 secret key |

## Safety Guarantees

- **Fail-Safe Cleanup**: GPU destroyed on ANY error
- **Determinism**: No randomness in job generation
- **No Wall-Clock Dependency**: Training logic is date-parameterized
- **Logging**: All steps logged with timestamps
- **Cost Control**: `maxHourlyCost` prevents expensive instances
- **Timeout**: `maxRuntimeMin` prevents runaway jobs

## Troubleshooting

### No GPU offers found
- Check `maxHourlyCost` - increase if market is expensive
- Check `minGpuMemory` - some GPUs have less VRAM

### SSH connection failed
- GPU instance may not be fully booted yet
- Check Vast.ai console for instance status

### Training failed on GPU
- Check S3 credentials passed to remote instance
- Check data availability for requested date
