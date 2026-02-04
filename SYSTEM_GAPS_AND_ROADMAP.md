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

### Gap 1: RiskManager Not Integrated — ✅ DONE

**Location:** `core/risk/RiskManager.js`

**Status:** ✅ COMPLETE — Fully integrated into StrategyRuntime

**What Exists:**
```javascript
export { RiskManager } from './RiskManager.js';
export { MaxPositionRule } from './rules/MaxPositionRule.js';
export { CooldownRule } from './rules/CooldownRule.js';
export { MaxDailyLossRule } from './rules/MaxDailyLossRule.js';
export { StopLossTakeProfitRule } from './rules/StopLossTakeProfitRule.js';
```

**Resolution (2026-02-04):**
- `StrategyRuntime.attachRiskManager()` method added (line 303-322)
- `#processEvent()` calls `riskManager.onEvent()` and `checkForExit()` (line 589-618)
- `#placeOrder()` validates orders via `riskManager.allow()` (line 693-716)
- Manifest includes risk stats (line 870-873)

---

### Gap 2: LiveStrategyRunner Not Exposed via Service — ✅ DONE

**Location:** `core/strategy/live/LiveStrategyRunner.js`

**Status:** ✅ COMPLETE — HTTP API implemented in strategyd

**What Exists:**
- Connects to `LiveWSConsumer` for real-time data
- Uses `LiveEventSequencer` for ordering
- Integrates `PromotionGuardManager` and `RunBudgetManager`
- Archives runs to S3
- Registers with `ObserverRegistry`

**Resolution (2026-02-04):**
- `services/strategyd/routes/live.routes.js` implements:
  - `POST /live/start` - Start live strategy run (30 req/min rate limit)
  - `POST /live/stop` - Stop live strategy run (60 req/min rate limit)
  - `GET /live/status` - Get all runs status
  - `GET /live/status/:id` - Get specific run status
- Input validation (exchange, symbols, strategyPath)
- Config defaults for executionConfig and riskConfig
- Audit logging for all operations
- Auth via STRATEGYD_TOKEN

---

### Gap 3: Dual Observer Implementations — ✅ DONE

**Locations:**
- `core/observer/index.js` (Express, JS) — **DEPRECATED**
- `core/observer-api/index.ts` (Express, TypeScript) — **AUTHORITATIVE**

**Status:** ✅ RESOLVED — `core/observer` deprecated, `core/observer-api` is the single source

**Differences:**

| Feature | core/observer | core/observer-api |
|---------|---------------|-------------------|
| Language | JavaScript | TypeScript |
| Port | 9150 | 3000 |
| Routes | /observer/runs, /observer/stop | /v1/jobs, /api/runs, /gates, /decisions |
| Auth | OBSERVER_TOKEN | OBSERVER_TOKEN (partial routes) |
| Systemd | No | Yes |

**Resolution (2026-02-04):**
- `core/observer/index.js` marked as `@deprecated` with console warning
- All new development should use `core/observer-api` (port 3000)
- Run routes migrated to observer-api (line 46: `app.use('/', runsRoutes)`)

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

- [x] Integrate RiskManager into runtime — ✅ DONE (Gap 1)
- [x] Add ML confidence to context object — ✅ DONE (2026-02-04)
- [x] Create ML backtest comparison tooling — ✅ DONE (2026-02-04)
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

- [x] HTTP API for live run management — ✅ DONE (Gap 2)
- [ ] Exchange execution bridge
- [ ] Position reconciliation service
- [ ] Live monitoring dashboard
- [x] Kill switch endpoint — ✅ DONE (2026-02-04)
- [x] Pre-flight verification checks — ✅ DONE (2026-02-04)
- [ ] Gradual rollout mechanism (canary → shadow → active)

### Critical Blockers

| Blocker | Description |
|---------|-------------|
| No execution bridge | LiveStrategyRunner has no real exchange connection |
| ~~No API exposure~~ | ✅ RESOLVED - strategyd /live/* endpoints |
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
- [x] Alerting integration (Slack) — ✅ DONE (2026-02-04)
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
| `tools/ml-compare.js` | ML model comparison | Available |
| `core/alerts/AlertManager.js` | Slack/file alerting | Available |
| `tools/verify-audit-trail.js` | Audit verification | Available |
| `tools/run-archive-retention.js` | Archive cleanup | Available |

---

## Integration Priorities

### Immediate (Low Risk)

1. ~~**Add RiskManager to StrategyRuntime**~~ — ✅ DONE
   - Follow existing `attachXxx()` pattern
   - Call `riskManager.evaluate(intent)` before `onOrder()`
   - No runtime logic changes

2. ~~**Consolidate Observer implementations**~~ — ✅ DONE
   - Decided on TypeScript (`core/observer-api`)
   - `core/observer` deprecated
   - Single systemd unit (`observer-api`)

### Medium Term (Medium Risk)

3. ~~**HTTP API for LiveStrategyRunner**~~ — ✅ DONE
   - Added `/live/start`, `/live/stop`, `/live/status` routes
   - Uses ObserverRegistry
   - Rate limited, auth protected

4. ~~**ML confidence in context**~~ — ✅ DONE
   - Added `context.getMlAdvice()` method
   - Returns advisory signals only
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
├── ✅ RiskManager integration (DONE)
├── ✅ ML confidence API (DONE)
└── ✅ ML backtest tooling (DONE)

Phase 4 Preparation:
├── ✅ HTTP API for live runs (DONE)
├── ✅ Pre-flight checks service (DONE)
└── ✅ Kill switch endpoint (DONE)

Phase 4 Activation (BLOCKED until safety verified):
├── Exchange bridge
├── Position reconciliation
└── Live monitoring

Phase 5 Completion:
├── Observability dashboard
├── ✅ Alerting integration (DONE)
└── Incident response
```

---

*This document reflects gaps identified through codebase analysis. Last updated: 2026-02-04.*
