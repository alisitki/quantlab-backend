# System Gaps and Roadmap

This document identifies engineering gaps and organizes them by implementation phase.

---

## Current System State

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 0 — Data Integrity | STABLE | Collector, compaction, S3 storage |
| Phase 1 — Strategy Runtime | STABLE | Replay, execution, strategy interface |
| Phase 2 — Safety Guards | STABLE | Ordering guard, error containment |
| Phase 3 — ML Advisory | STABLE | XGBoost training, advisory mode, metrics dashboard |
| Phase 4 — Live Trading | READY | Exchange bridge ready, awaiting alpha layer |
| Phase 5 — Ops & Monitoring | STABLE | Prometheus, SLO, Runbook, Incident Response |

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
- Model version tracking in run manifests (job_id, job_hash, decision_path) — ✅ DONE (2026-02-04)
- ML metrics dashboard API endpoints — ✅ DONE (2026-02-04)

**What's Missing for Autonomous:**
- ML model integration into runtime decision path
- `MLDecisionAdapter` exists but is advisory-only
- `MLActiveGate` has `ML_ACTIVE_ENABLED` flag but defaults to false
- No automatic position sizing based on ML confidence

**Current Safety:**
- `ML_ACTIVE_KILL` env var can disable
- `ML_ACTIVE_MAX_DAILY_IMPACT_PCT` limits daily impact

---

### Gap 5: Live Trading Safety Path — ✅ DONE

**Components Exist:**
- `LiveStrategyRunner`
- `PromotionGuardManager`
- `RunBudgetManager`
- `LiveWSConsumer`
- `LiveEventSequencer`
- `AuditWriter`

**Resolution (2026-02-04):**
- ✅ Pre-flight checks service (`tools/go-live-check.js`)
- ✅ Exchange execution bridge (`core/exchange/` - Binance, Bybit, OKX)
- ✅ Position reconciliation (`core/exchange/reconciliation/`)
- ✅ Live monitoring dashboard (`/v1/monitor/*` endpoints)
- ✅ Kill switch HTTP endpoint (`/v1/kill-switch/*`)
- ✅ Human approval gate (`/v1/approval/*`)

**Current State:** `live_trading_path: "ACTIVE"` in system state.

---

### Gap 6: TODO Markers in Code

**Found TODOs:**

| File | TODO |
|------|------|
| `core/execution/engine.js:62` | "Add runId parameter for multi-run isolation (v2)" |

**Implication:** ExecutionEngine doesn't support parallel runs with isolated state. Each run must use separate engine instance.

---

## Phase 3: ML Advisory (STABLE)

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
- [x] Add model version tracking in runs — ✅ DONE (2026-02-04)
- [x] Build ML metrics dashboard API — ✅ DONE (2026-02-04)

### Entry Points

- `core/scheduler/run_daily_ml.js` — Daily training
- `core/features/FeatureBuilderV1.js` — Feature extraction
- `core/ml/` — Model training (runs on GPU)

---

## Phase 4: Live Trading (ACTIVE)

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
- [x] Exchange execution bridge — ✅ DONE (2026-02-04)
- [x] Position reconciliation service — ✅ DONE (2026-02-04)
- [x] Live monitoring dashboard — ✅ DONE (2026-02-04)
- [x] Kill switch endpoint — ✅ DONE (2026-02-04)
- [x] Pre-flight verification checks — ✅ DONE (2026-02-04)
- [x] Gradual rollout mechanism (canary → shadow → active) — ✅ DONE (2026-02-04)

### Critical Blockers

| Blocker | Description |
|---------|-------------|
| ~~No execution bridge~~ | ✅ RESOLVED - `core/exchange/` module with Binance adapter |
| ~~No API exposure~~ | ✅ RESOLVED - strategyd /live/* endpoints |
| ~~No monitoring~~ | ✅ RESOLVED - `/v1/monitor/*` endpoints in strategyd |

### Safety Requirements (Before Activation)

1. Mandatory pre-flight checks pass
2. RiskManager integrated and configured
3. Kill switch tested and working
4. Audit trail verified
5. Budget limits enforced
6. Human approval gate for first N runs

---

## Phase 5: Ops & Monitoring (STABLE)

### Completed

- [x] Observer API with health endpoints
- [x] Quality ledger (collector)
- [x] Uploader status API
- [x] Basic health check routes
- [x] Run manifest persistence
- [x] ops/outbox message system

### Outstanding

- [x] Unified observability dashboard — ✅ DONE (2026-02-04)
- [x] Alerting integration (Slack) — ✅ DONE (2026-02-04)
- [x] Metrics aggregation (Prometheus/Grafana) — ✅ DONE (2026-02-04)
- [x] SLO/SLA monitoring — ✅ DONE (2026-02-04)
- [x] Runbook automation — ✅ DONE (2026-02-04)
- [x] Cost tracking for GPU usage — ✅ DONE (2026-02-04)
- [x] Incident response tooling — ✅ DONE (2026-02-04)

### Available Tooling

| Tool | Purpose | Status |
|------|---------|--------|
| `tools/go-live-check.js` | Pre-flight verification | Available |
| `tools/verify-live-parity.js` | Replay vs live parity | Available |
| `tools/ml-compare.js` | ML model comparison | Available |
| `core/alerts/AlertManager.js` | Slack/file alerting | Available |
| `tools/verify-audit-trail.js` | Audit verification | Available |
| `tools/run-archive-retention.js` | Archive cleanup | Available |
| `core/scheduler/report_ml_costs.js` | GPU cost reporting | Available |

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

5. ~~**Exchange execution bridge**~~ — ✅ DONE (2026-02-04)
   - `core/exchange/` module with Binance, Bybit, OKX adapters
   - Gradual rollout implemented (CANARY default mode)
   - Full audit trail via AuditWriter

6. **Autonomous ML** — BLOCKED
   - Only after all safety gates proven
   - Budget limits enforced
   - Kill switch tested
   - Currently in ADVISORY_ONLY mode

---

## Recommended Development Order

```
Phase 3 Completion:
├── ✅ RiskManager integration (DONE)
├── ✅ ML confidence API (DONE)
├── ✅ ML backtest tooling (DONE)
├── ✅ Model version tracking (DONE)
└── ✅ ML metrics dashboard API (DONE)

Phase 4 Preparation:
├── ✅ HTTP API for live runs (DONE)
├── ✅ Pre-flight checks service (DONE)
└── ✅ Kill switch endpoint (DONE)

Phase 4 Activation (BLOCKED until safety verified):
├── Exchange bridge
├── Position reconciliation
└── Live monitoring

Phase 5 Completion:
├── ✅ Observability dashboard (DONE)
├── ✅ Alerting integration (DONE)
├── ✅ GPU cost tracking (DONE)
└── ✅ Incident response (DONE)
```

---

---

## Alpha Layer Development

> **Current Focus:** Signal Layer Construction (`alpha_layer.status: ACTIVE`)
>
> Infrastructure is COMPLETE. Development focus is now on SIGNAL/ALPHA layer.

### Feature Development (COMPLETE) - 2026-02-05

| Feature | Location | Status |
|---------|----------|--------|
| mid_price | core/features/builders/MidPriceFeature.js | LIVE |
| spread | core/features/builders/SpreadFeature.js | LIVE |
| return_1 | core/features/builders/ReturnFeature.js | LIVE |
| volatility | core/features/builders/VolatilityFeature.js | LIVE |
| ema | core/features/builders/EMAFeature.js | LIVE |
| rsi | core/features/builders/RSIFeature.js | LIVE |
| atr | core/features/builders/ATRFeature.js | LIVE |
| roc | core/features/builders/ROCFeature.js | LIVE |
| regime_volatility | core/features/builders/VolatilityRegimeFeature.js | LIVE |
| regime_trend | core/features/builders/TrendRegimeFeature.js | LIVE |
| regime_spread | core/features/builders/SpreadRegimeFeature.js | LIVE |
| microprice | core/features/builders/MicropriceFeature.js | LIVE |
| imbalance_ema | core/features/builders/ImbalanceEMAFeature.js | LIVE |
| ema_slope | core/features/builders/EMASlopeFeature.js | LIVE |
| bollinger_pos | core/features/builders/BollingerPositionFeature.js | LIVE |

**Target:** 15 live features - **ACHIEVED**

### Feature Analysis & Intelligence (COMPLETE) - 2026-02-05

| Module | Location | Status |
|--------|----------|--------|
| FeatureCorrelation | core/ml/analysis/FeatureCorrelation.js | LIVE |
| FeatureLabelCorrelation | core/ml/analysis/FeatureLabelCorrelation.js | LIVE |
| LabelDistribution | core/ml/analysis/LabelDistribution.js | LIVE |
| PermutationImportance | core/ml/analysis/PermutationImportance.js | LIVE |
| FeatureDistribution | core/ml/analysis/FeatureDistribution.js | LIVE |
| FeatureStability | core/ml/analysis/FeatureStability.js | LIVE |
| FeatureReportGenerator | core/ml/analysis/FeatureReportGenerator.js | LIVE |
| DecisionLogger | core/ml/logging/DecisionLogger.js | LIVE |
| RegimeLogger | core/ml/logging/RegimeLogger.js | LIVE |

**CLI Tool:** `tools/ml-feature-analysis.js`
**Alpha Score:** `0.4 * importance + 0.3 * labelCorr + 0.3 * stability`

### Strategy Development (ACTIVE) - 2026-02-05

**Status:** ✅ StrategyV1 implemented, tests passing

| Component | Location | Status |
|-----------|----------|--------|
| BaselineStrategy | core/strategy/baseline/ | PRODUCTION |
| StrategyV1 | core/strategy/v1/StrategyV1.js | READY |
| RegimeModeSelector | core/strategy/v1/decision/RegimeModeSelector.js | READY |
| SignalGenerator | core/strategy/v1/decision/SignalGenerator.js | READY |
| Combiner | core/strategy/v1/decision/Combiner.js | READY |

**Key Features:**
- ✅ Dynamic feature selection (from analysis report, NOT hardcoded)
- ✅ Regime mode switching (HIGH vol → mean reversion, LOW vol → momentum)
- ✅ Alpha-weighted signal combination
- ✅ 5 config presets (default, high_frequency, quality, aggressive, conservative)

**Next Steps:**
- [ ] Backtest StrategyV1 vs BaselineStrategy
- [ ] Compare performance metrics
- [ ] Feature report generation on historical data

### ML Integration (ADVISORY_ACTIVE)

ML operates in advisory mode. Next steps:
- [x] Expose confidence to strategy — ✅ DONE (context.getMlAdvice())
- [ ] Enable position scaling based on confidence
- [ ] Integrate ML signals with StrategyV1

### Validation (READY) - 2026-02-05

Decision and regime logging implemented:
- [x] Feature values at decision time — ✅ DecisionLogger.logDecision()
- [x] Regime state logging — ✅ RegimeLogger.logRegimeState()
- [x] Transition detection — ✅ RegimeLogger detects regime changes
- [ ] Decision reasoning traces (future: explain StrategyV1 decisions)

---

*This document reflects gaps identified through codebase analysis. Last updated: 2026-02-05.*
