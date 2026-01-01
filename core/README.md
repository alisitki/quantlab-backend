# QuantLab ML Implementation

## Decision Config Contract
Each promoted model produces a `decision.json` file in S3 which determines how signals are generated in production.

**S3 Path:** `s3://quantlab-artifacts/models/production/{symbol}/decision.json`

**Structure:**
```json
{
  "symbol": "btcusdt",
  "featuresetVersion": "v1",
  "labelHorizonSec": 10,
  "primaryMetric": "f1_pos",
  "bestThreshold": 0.55,
  "thresholdGrid": [0.5, 0.55, 0.6, 0.65, 0.7],
  "probaSource": "model|pseudo_sigmoid",
  "jobId": "job-...",
  "createdAt": "ISO-8601",
  "configHash": "deterministic-sha256"
}
```

## Signal Generation Tools

### DecisionLoader & applyDecision
- `DecisionLoader.js`: Loads `decision.json` from S3 with 60s in-memory caching and fallback defaults.
- `applyDecision.js`: Applies the `bestThreshold` from the decision config to a probability array.

### Dry-Run Tool
Test signal generation with real production config and features without executing trades.

```bash
# Run signal generation for 1000 events on a specific date
node ml/decision/run_signal_dry.js --symbol btcusdt --date 20251229 --limit 1000
```

**Verification Criteria:**
- Loads `decision.json` from S3 (Cache/Fallback logic included).
- Generates probabilities (PSEUDO_PROBA support).
- Applies deterministic threshold.
- Reports `pred_pos_count` and `pred_pos_rate`.

## Deploy Discipline v1.2
Starting from v1.2, all code changes deployed to remote GPU instances must be committed to Git. The legacy hot-patching system is now disabled.

### Enforcement
- `REMOTE_HOTPATCH=1` will now trigger a hard failure: `REMOTE_HOTPATCH is disabled in v1.2. Please deploy your changes via git push.`
- Remote jobs automatically perform a `git clone` from the configured repository.

### Configuration Overrides
You can use environment variables to control which code is executed on the GPU:
- `REPO_BRANCH`: Target branch to clone (default: `main`)
- `REPO_COMMIT`: Specific commit hash to enforce (optional override for debugging)

Example:
```bash
REPO_BRANCH=feat-new-model node scheduler/run_daily_ml.js --symbol btcusdt --live
```

---

## Automation & Scheduling

The ML pipeline is designed to be run daily via a centralized orchestrator that supports production safety guards and multi-day backfills.

### Daily Production Orchestrator (Dry Run)
This script runs the ML pipeline for "yesterday" (or a range) in `--promote dry` mode. It verifies that the production models remain untouched and that all GPU instances are correctly cleaned up.

```bash
# Run for yesterday (default)
PSEUDO_PROBA=0 node scheduler/run_daily_prod_dry.js --symbol btcusdt --live --ensure-features

# Run backfill for specific range
PSEUDO_PROBA=0 node scheduler/run_daily_prod_dry.js --symbol btcusdt --date-from 20251228 --date-to 20251229 --live
```

### CRON Configuration (Production)
To automate daily LIVE training with lockfile and alerting:

```cron
# Every day at 02:00 AM UTC
0 2 * * * /home/deploy/quantlab/api/scripts/run_daily_live.sh >> /home/deploy/quantlab/api/logs/cron_daily.log 2>&1
```

Features:
- Lockfile prevents overlapping runs
- Alerts on failure (`logs/alerts.jsonl`)
- Unified DRY/LIVE orchestrator

### Manual LIVE Run
```bash
cd /home/deploy/quantlab/api && \
PSEUDO_PROBA=0 node scheduler/run_daily_prod.js --mode live --symbol btcusdt --ensure-features
```

---

## Automation & Maintenance
### CRON Configuration

| Job | Schedule (UTC) | Command |
| :--- | :--- | :--- |
| Daily LIVE Run | `00 02 * * *` | `scripts/run_daily_live.sh` |
| Retention (Dry) | `00 15 * * *` | `scripts/run_retention_dry.sh` |
| Ops Dequeue (Dry)| `30 15 * * *` | `scripts/run_ops_dequeue.sh` |
| Ops Dequeue (Apply)| `10 15 * * *` | `scripts/run_ops_dequeue_apply.sh` |

### Operational Pipeline Overview
```text
Daily ML Run (02:00 UTC)
   ↓
Health + Digest Data Generation
   ↓
generate_ops_message (Payload Aggregation)
   ↓
enqueue_ops_outbox (Queueing)
   ↓
dequeue_ops_outbox (Processing)
   ↓
Telegram Delivery (15:10 UTC)
```

### Retention Apply — Manual Gate Policy
**STRICT POLICY**: The `cleanup_retention.js --apply` command is NEVER automated. Cleanup must be triggered manually by an operator.

**Pre-Apply Checklist**:
1. Review the latest `cleanup/plan_*.json` file.
2. Verify all candidate labels (e.g., S3_FAILED, S3_REJECTED).
3. Ensure the plan timestamp is within the last 24h.
4. Confirm no active training jobs are running.

> [!WARNING]
> **ROLLBACK IMPOSSIBLE**: S3 and local deletions are permanent. Double-check the plan before execution.

### Telegram Configuration
The Telegram adapter requires the following environment variables in `.env`:
- `TELEGRAM_BOT_TOKEN`: Your Telegram Bot API token.
- `TELEGRAM_CHAT_ID`: The target Chat ID or Channel ID.
- `TELEGRAM_DISABLE_PREVIEW`: (Optional) `true` (default) or `false`.

### Delivery Commands
```bash
# Dry-run (Preview)
node scheduler/dequeue_ops_outbox.js --max 20

# Apply (Real Notifications via Telegram)
node scheduler/dequeue_ops_outbox.js --apply --adapter telegram --max 20
```

## Tests
Run unit tests for the decision layer:
```bash
node ml/tests/test-decision-loader.js
```

Run deploy discipline smoke test:
```bash
node vast/test-no-hotpatch.js
```

Run daily orchestrator logic test:
```bash
node scheduler/test-daily-prod-dry.js
```
