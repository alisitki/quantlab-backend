# QuantLab Roadmap Tasks

This file contains the prioritized task list derived from SYSTEM_GAPS_AND_ROADMAP.md.

---

## Current Phase: 2 (Safety Guards)

---

## Immediate Priority (Low Risk)

### [ ] RiskManager Integration (Gap 1)

**Status:** NOT_INTEGRATED
**Location:** `core/risk/RiskManager.js`

**What exists:**
- Complete RiskManager implementation
- MaxPositionRule, CooldownRule, MaxDailyLossRule, StopLossTakeProfitRule
- Zero imports in runtime

**What to do:**
1. Add `attachRiskManager()` method to StrategyRuntime
2. Call `riskManager.evaluate(intent)` before `onOrder()`
3. Log rejections with reason codes
4. Update SYSTEM_STATE.json: `risk_layer: "INTEGRATED"`

**Pattern reference:** 
```javascript
runtime.attachRiskManager(riskManager);
```

---

### [ ] Observer Consolidation (Gap 3)

**Status:** Two parallel implementations exist
**Locations:**
- `core/observer/index.js` (JS, port 9150)
- `core/observer-api/index.ts` (TS, port 3000)

**Decision needed:**
- Choose authoritative implementation (recommend TS observer-api)
- Merge routes
- Single systemd unit
- Deprecate other

---

## Medium Term Priority (Medium Risk)

### [ ] LiveStrategyRunner HTTP API (Gap 2)

**Status:** Complete implementation, not exposed via HTTP
**Location:** `core/strategy/live/LiveStrategyRunner.js`

**What to do:**
1. Add `/live/start` route to strategyd
2. Add `/live/stop` route to strategyd
3. Require explicit ACTIVE flag
4. Use ObserverRegistry for tracking

**Phase Gate:** This is Phase 4 work — requires explicit approval

---

### [ ] ML Confidence in Context (Gap 4)

**Status:** ML in ADVISORY_ONLY mode
**What to do:**
1. Add `context.getMlAdvice()` method
2. Return advisory signals only
3. No automatic execution

---

## Long Term Priority (High Risk - BLOCKED)

### [ ] Exchange Execution Bridge

**Status:** BLOCKED until safety verified
**Prerequisites:**
- RiskManager integrated ✅
- Kill switch tested
- Audit trail verified
- Gradual rollout mechanism

### [ ] Autonomous ML

**Status:** BLOCKED
**Prerequisites:**
- All safety gates proven
- Budget limits enforced
- Kill switch tested
- Human approval required

---

## Phase 5: Ops & Monitoring

### [ ] Unified Observability Dashboard
### [ ] Alerting Integration (PagerDuty/Slack)
### [ ] Metrics Aggregation (Prometheus/Grafana)
### [ ] SLO/SLA Monitoring
### [ ] Incident Response Tooling
### [ ] Cost Tracking for GPU Usage

---

## Completed

- [x] Phase 0 — Data Integrity: STABLE
- [x] Phase 1 — Strategy Runtime: STABLE
- [x] Phase 2 — Safety Guards: STABLE
- [x] XGBoost training infrastructure
- [x] Feature extraction (FeatureBuilderV1)
- [x] Model promotion logic
- [x] Vast.ai GPU orchestration
