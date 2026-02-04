# System Gaps and Roadmap

This document identifies engineering gaps and organizes them by implementation phase.

---

## Current System State

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 0 — Data Integrity | STABLE | Collector, compaction, S3 storage |
| Phase 1 — Strategy Runtime | STABLE | Replay, execution, strategy interface |
| Phase 2 — Safety Guards | STABLE | Ordering guard, error containment |
| Phase 3 — ML Advisory | PARTIAL | XGBoost training, advisory mode only |
| Phase 4 — Live Trading | EXPERIMENTAL | LiveStrategyRunner exists but not wired |
| Phase 5 — Ops & Monitoring | PARTIAL | Observer API, some tooling |

---

## Identified Gaps

### Gap 1: RiskManager Not Integrated

**Location:** `core/risk/RiskManager.js`

**Status:** Complete implementation, zero imports in runtime

**What Exists:**
```javascript
export { RiskManager } from './RiskManager.js';
export { MaxPositionRule } from './rules/MaxPositionRule.js';
export { CooldownRule } from './rules/CooldownRule.js';
export { MaxDailyLossRule } from './rules/MaxDailyLossRule.js';
export { StopLossTakeProfitRule } from './rules/StopLossTakeProfitRule.js';
```

**Gap:** RiskManager is not attached to StrategyRuntime or ExecutionEngine. Strategies can place unlimited orders without risk checks.

**Integration Point:** Should attach to `StrategyRuntime` via `attachRiskManager()` method (pattern exists for other components).

---

### Gap 2: LiveStrategyRunner Not Exposed via Service

**Location:** `core/strategy/live/LiveStrategyRunner.js`

**Status:** Complete implementation, not wired to any HTTP service

**What Exists:**
- Connects to `LiveWSConsumer` for real-time data
- Uses `LiveEventSequencer` for ordering
- Integrates `PromotionGuardManager` and `RunBudgetManager`
- Archives runs to S3
- Registers with `ObserverRegistry`

**Gap:** No HTTP endpoint to start/stop live runs. Only CLI invocation possible.

**Missing:** HTTP route in strategyd or dedicated livyd service.

---

### Gap 3: Dual Observer Implementations

**Locations:**
- `core/observer/index.js` (Express, JS)
- `core/observer-api/index.ts` (Express, TypeScript)

**Status:** Both exist, overlapping functionality

**Differences:**

| Feature | core/observer | core/observer-api |
|---------|---------------|-------------------|
| Language | JavaScript | TypeScript |
| Port | 9150 | 3000 |
| Routes | /observer/runs, /observer/stop | /v1/jobs, /api/runs, /gates, /decisions |
| Auth | OBSERVER_TOKEN | OBSERVER_TOKEN (partial routes) |
| Systemd | No | Yes |

**Gap:** Unclear which is authoritative. Maintenance burden of two implementations.

---

### Gap 4: ML Advisory vs Autonomous Gap

**Current State:** ML operates in `ADVISORY_ONLY` mode

**What Works:**
- XGBoost model training (via Vast.ai GPU)
- Model promotion with metrics comparison
- Feature extraction (`FeatureBuilderV1`)

**What's Missing for Autonomous:**
- ML model integration into runtime decision path
- `MLDecisionAdapter` exists but is advisory-only
- `MLActiveGate` has `ML_ACTIVE_ENABLED` flag but defaults to false
- No automatic position sizing based on ML confidence

**Current Safety:**
- `ML_ACTIVE_KILL` env var can disable
- `ML_ACTIVE_MAX_DAILY_IMPACT_PCT` limits daily impact

---

### Gap 5: Live Trading Safety Path Incomplete

**Components Exist:**
- `LiveStrategyRunner`
- `PromotionGuardManager`
- `RunBudgetManager`
- `LiveWSConsumer`
- `LiveEventSequencer`
- `AuditWriter`

**Components Missing:**
- Pre-flight checks service
- Real exchange execution bridge
- Position reconciliation
- Live risk monitoring dashboard
- Kill switch HTTP endpoint

**Current State:** `live_trading_path: "INACTIVE"` in system state.

---

### Gap 6: TODO Markers in Code

**Found TODOs:**

| File | TODO |
|------|------|
| `core/execution/engine.js:62` | "Add runId parameter for multi-run isolation (v2)" |

**Implication:** ExecutionEngine doesn't support parallel runs with isolated state. Each run must use separate engine instance.

---

## Phase 3: ML Advisory (PARTIAL)

### Completed

- [x] XGBoost model training infrastructure
- [x] Feature extraction (`FeatureBuilderV1`)
- [x] Model promotion logic with metrics comparison
- [x] Vast.ai GPU orchestration
- [x] Model artifacts storage in S3
- [x] MLDecisionAdapter (advisory mode)

### Outstanding

- [ ] Integrate RiskManager into runtime
- [ ] Add ML confidence to context object
- [ ] Create ML backtest comparison tooling
- [ ] Add model version tracking in runs
- [ ] Build ML metrics dashboard

### Entry Points

- `core/scheduler/run_daily_ml.js` — Daily training
- `core/features/FeatureBuilderV1.js` — Feature extraction
- `core/ml/` — Model training (runs on GPU)

---

## Phase 4: Live Trading (EXPERIMENTAL)

### Completed

- [x] LiveStrategyRunner implementation
- [x] LiveWSConsumer for real-time data
- [x] LiveEventSequencer for ordering
- [x] PromotionGuardManager for safety gates
- [x] RunBudgetManager for budget limits
- [x] AuditWriter for audit trail
- [x] ObserverRegistry for run tracking

### Outstanding

- [ ] HTTP API for live run management
- [ ] Exchange execution bridge
- [ ] Position reconciliation service
- [ ] Live monitoring dashboard
- [ ] Kill switch endpoint
- [ ] Pre-flight verification checks
- [ ] Gradual rollout mechanism (canary → shadow → active)

### Critical Blockers

| Blocker | Description |
|---------|-------------|
| No execution bridge | LiveStrategyRunner has no real exchange connection |
| No API exposure | Cannot start/stop live runs via HTTP |
| No monitoring | ObserverRegistry exists but no real-time dashboard |

### Safety Requirements (Before Activation)

1. Mandatory pre-flight checks pass
2. RiskManager integrated and configured
3. Kill switch tested and working
4. Audit trail verified
5. Budget limits enforced
6. Human approval gate for first N runs

---

## Phase 5: Ops & Monitoring (PARTIAL)

### Completed

- [x] Observer API with health endpoints
- [x] Quality ledger (collector)
- [x] Uploader status API
- [x] Basic health check routes
- [x] Run manifest persistence
- [x] ops/outbox message system

### Outstanding

- [ ] Unified observability dashboard
- [ ] Alerting integration (PagerDuty/Slack)
- [ ] Metrics aggregation (Prometheus/Grafana)
- [ ] SLO/SLA monitoring
- [ ] Runbook automation
- [ ] Incident response tooling
- [ ] Cost tracking for GPU usage

### Available Tooling

| Tool | Purpose | Status |
|------|---------|--------|
| `tools/go-live-check.js` | Pre-flight verification | Available |
| `tools/verify-live-parity.js` | Replay vs live parity | Available |
| `tools/verify-audit-trail.js` | Audit verification | Available |
| `tools/run-archive-retention.js` | Archive cleanup | Available |

---

## Integration Priorities

### Immediate (Low Risk)

1. **Add RiskManager to StrategyRuntime**
   - Follow existing `attachXxx()` pattern
   - Call `riskManager.evaluate(intent)` before `onOrder()`
   - No runtime logic changes

2. **Consolidate Observer implementations**
   - Decide on JS or TS
   - Merge routes
   - Single systemd unit

### Medium Term (Medium Risk)

3. **HTTP API for LiveStrategyRunner**
   - Add `/live/start`, `/live/stop` routes
   - Use existing ObserverRegistry
   - Require explicit ACTIVE flag

4. **ML confidence in context**
   - Add `context.getMlAdvice()` method
   - Return advisory signals only
   - No automatic execution

### Long Term (High Risk)

5. **Exchange execution bridge**
   - Requires extensive testing
   - Gradual rollout (paper → canary → live)
   - Full audit trail

6. **Autonomous ML**
   - Only after all safety gates proven
   - Budget limits enforced
   - Kill switch tested

---

## Recommended Development Order

```
Phase 3 Completion:
├── RiskManager integration
├── ML confidence API
└── ML backtest tooling

Phase 4 Preparation:
├── HTTP API for live runs
├── Pre-flight checks service
└── Kill switch endpoint

Phase 4 Activation (BLOCKED until safety verified):
├── Exchange bridge
├── Position reconciliation
└── Live monitoring

Phase 5 Completion:
├── Observability dashboard
├── Alerting integration
└── Incident response
```

---

*This document reflects gaps identified through codebase analysis on 2026-02-03.*
