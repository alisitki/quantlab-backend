# QuantLab Roadmap Tasks

> **⚠️ PHASE SHIFT: Infra → Alpha Engineering**
>
> System audit completed 2026-02-04. Result: Infra COMPLETE, Alpha PRIMITIVE.
> All development now focuses on SIGNAL LAYER.

## State Tracking

Progress is tracked in `SYSTEM_STATE.json` (v2.0):
- `alpha_layer.status: ACTIVE`
- `alpha_layer.current_focus: feature_layer`
- `infrastructure.status: COMPLETE`
- `live_trading_path: READY`

---

## Current Phase: α-1 (Signal Layer Construction)

---

## 🔥 PRIORITY 1: Live Feature Layer (Critical)

### [ ] Move Batch Indicators to Live

**Problem:** RSI, EMA, ATR exist in `core/worker/feature.js` but NOT in live path.

**Action:**
1. Create streaming versions in `core/features/builders/`
2. Integrate with FeatureRegistry
3. Make available to strategies

**Indicators to migrate:**
- [ ] RSI (14, 28 windows)
- [ ] EMA (12, 26)
- [ ] ATR (14)
- [ ] ROC (5s, 30s, 2m)

---

### [ ] Create Regime Detection Features

**Problem:** No regime awareness. System trades chop same as trend.

**Action:**
1. Volatility regime detector (low/normal/high)
2. Trend regime detector (slope persistence)
3. Volatility compression detector

---

### [ ] Enhance Microstructure Features

- [ ] Spread regime (tight/normal/wide)
- [ ] Orderbook imbalance smoothing
- [ ] Liquidity pressure proxy

---

## 🎯 PRIORITY 2: Strategy Upgrade

### [ ] Demote BaselineStrategy to Test

Move to `core/strategy/test/` folder.

### [ ] Create StrategyV1

**Requirements:**
- Combines momentum + volatility + regime filters
- Avoids trading in chop regime
- Adapts position size based on volatility
- Uses feature inputs (not raw price)
- Logs decision reasoning

---

## 📊 PRIORITY 3: ML Integration

### [ ] Expose ML Confidence to Strategy

Currently ML is shadow-only. Strategy should see confidence.

### [ ] Position Scaling by Confidence

Allow strategy to scale position based on ML confidence score.

---

## 🔍 PRIORITY 4: Alpha Validation

### [ ] Add Decision Logging

Log at decision time:
- Feature values
- Regime state
- Decision reason
- ML confidence (if available)

---

## ⏸️ PAUSED - Infra Work (Do Not Touch)

These items are COMPLETE. Do not work on them unless critical bug:

- [x] Phase 0 — Data Integrity: STABLE
- [x] Phase 1 — Strategy Runtime: STABLE
- [x] Phase 2 — Safety Guards: STABLE
- [x] Multi-exchange collectors (3 exchanges)
- [x] Multi-stream ingestion (5 stream types)
- [x] Exchange bridges (Binance, Bybit, OKX)
- [x] Deterministic replay engine
- [x] Kill switch & approval gates

---

## Previous Roadmap (Reference Only)

The following were identified as gaps but are now LOWER PRIORITY than alpha work:

- RiskManager Integration (Gap 1) — Useful but not urgent
- Observer Consolidation (Gap 3) — Cleanup, not value-add
- LiveStrategyRunner API (Gap 2) — After strategies are ready

---

## Success Metrics

| Metric | Before | Target |
|--------|--------|--------|
| Live features | 4 | 15+ |
| Regime awareness | None | 3 regimes |
| Production strategies | 1 (primitive) | 1 (alpha-aware) |
| Decision logging | None | Full trace |
