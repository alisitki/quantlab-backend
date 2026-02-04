---
name: safety-risk
description: Risk management, guards, budget limits, and kill switches
---

# Safety & Risk

This skill covers risk management and safety mechanisms.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Strategy Runtime                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐│
│  │  Strategy   │──▶│ RiskManager │──▶│ ExecutionEngine   ││
│  │  onEvent()  │  │ evaluate()  │  │ execute()         ││
│  └─────────────┘  └─────────────┘  └─────────────────────┘│
│         │                │                    │            │
│         ▼                ▼                    ▼            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐│
│  │   Guards    │  │   Budget    │  │    Audit Trail     ││
│  │ (Promotion) │  │  Manager    │  │    (S3 Archive)    ││
│  └─────────────┘  └─────────────┘  └─────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## Key Files

| File | Purpose | Status |
|------|---------|--------|
| `core/risk/RiskManager.js` | Risk evaluation | NOT INTEGRATED |
| `core/risk/rules/MaxPositionRule.js` | Position limits | Available |
| `core/risk/rules/CooldownRule.js` | Trade cooldown | Available |
| `core/risk/rules/MaxDailyLossRule.js` | Daily loss limit | Available |
| `core/risk/rules/StopLossTakeProfitRule.js` | SL/TP orders | Available |
| `core/strategy/guards/PromotionGuardManager.js` | Live trading gates | Available |
| `core/strategy/limits/RunBudgetManager.js` | Budget limits | Available |

> **Note:** RiskManager is fully implemented but currently NOT wired to runtime. See SYSTEM_GAPS_AND_ROADMAP.md Gap 1.

---

## RiskManager

```javascript
import { RiskManager } from './RiskManager.js';
import { MaxPositionRule } from './rules/MaxPositionRule.js';
import { CooldownRule } from './rules/CooldownRule.js';

const riskManager = new RiskManager();
riskManager.addRule(new MaxPositionRule({ maxQty: 1.0 }));
riskManager.addRule(new CooldownRule({ minIntervalMs: 60000 }));

// Evaluate order intent
const result = riskManager.evaluate(orderIntent, context);
if (result.allowed) {
  // Proceed with execution
} else {
  // Log rejection: result.reason
}
```

---

## Risk Rules

### MaxPositionRule
Limits maximum position size per symbol.

```javascript
new MaxPositionRule({
  maxQty: 1.0,           // Max quantity
  maxNotional: 10000     // Max notional value
})
```

### CooldownRule
Enforces minimum time between trades.

```javascript
new CooldownRule({
  minIntervalMs: 60000   // 1 minute between trades
})
```

### MaxDailyLossRule
Stops trading after daily loss threshold.

```javascript
new MaxDailyLossRule({
  maxLossPercent: 2.0    // 2% max daily loss
})
```

### StopLossTakeProfitRule
Automatic SL/TP order generation.

```javascript
new StopLossTakeProfitRule({
  stopLossPercent: 1.0,   // 1% stop loss
  takeProfitPercent: 3.0  // 3% take profit
})
```

---

## PromotionGuardManager

Guards that must pass before live trading activation:

```javascript
import { PromotionGuardManager } from './PromotionGuardManager.js';

const guardManager = new PromotionGuardManager();

// Check all guards
const result = await guardManager.checkAll(context);
if (!result.passed) {
  console.log('Failed guards:', result.failedGuards);
}
```

**Guard Types:**
- Pre-flight checks pass
- RiskManager integrated
- Kill switch tested
- Audit trail verified
- Budget limits configured
- Human approval obtained

---

## RunBudgetManager

Limits on strategy run resources:

```javascript
import { RunBudgetManager } from './RunBudgetManager.js';

const budget = new RunBudgetManager({
  maxDecisions: 1000,        // Max decisions per run
  maxExecutionTimeMs: 3600000, // 1 hour max
  maxDailyRuns: 10           // Max runs per day
});

// Check if within budget
if (!budget.canContinue()) {
  runtime.stop('BUDGET_EXCEEDED');
}
```

---

## Kill Switch

Emergency stop mechanism:

### Environment Variables
| Variable | Purpose |
|----------|---------|
| `ML_ACTIVE_KILL` | Instant kill for ML autonomous mode |
| `ML_ACTIVE_MAX_DAILY_IMPACT_PCT` | Daily impact limit |

### HTTP Endpoint (strategyd)
```bash
# Kill specific run
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://localhost:3031/control \
  -d '{"action":"kill","runId":"run_abc123"}'
```

### Manual Emergency Stop
```bash
# Kill all strategy processes
pkill -f "node.*strategy"

# Or via systemd
sudo systemctl stop quantlab-worker
```

---

## Audit Trail

All executions logged to S3:

```
s3://run-archive/
└── replay_runs/
    └── replay_run_id=run_abc123/
        ├── manifest.json
        ├── decisions.jsonl
        └── stats.json
```

### Verify Audit Trail
```bash
node tools/verify-audit-trail.js --runId run_abc123
```

---

## Phase 4 Safety Requirements

Before activating live trading:

1. ✅ RiskManager integrated and tested
2. ✅ PromotionGuards all passing
3. ✅ Kill switch tested and working
4. ✅ Audit trail verified complete
5. ✅ Budget limits enforced
6. ✅ Human approval gate for first N runs
7. ✅ Pre-flight checks pass

---

## Integration TODO

RiskManager integration pattern (Gap 1 in roadmap):

```javascript
// In StrategyRuntime.js
class StrategyRuntime {
  attachRiskManager(riskManager) {
    this.riskManager = riskManager;
  }
  
  async processOrder(intent) {
    if (this.riskManager) {
      const check = this.riskManager.evaluate(intent, this.context);
      if (!check.allowed) {
        this.logRejection(check.reason);
        return null;
      }
    }
    return this.executionEngine.execute(intent);
  }
}
```

---

## Current System State

From `SYSTEM_STATE.json`:
```json
{
  "risk_layer": "NOT_INTEGRATED",
  "live_trading_path": "INACTIVE",
  "ml_mode": "ADVISORY_ONLY"
}
```

**Do NOT change these values without explicit user instruction!**
