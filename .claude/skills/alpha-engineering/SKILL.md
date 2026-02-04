---
name: alpha-engineering
description: Live feature stack, strategy development, and signal intelligence
---

# Alpha Engineering

> **PHASE SHIFT**: QuantLab infra is COMPLETE. Focus is now ALPHA LAYER.

## Current System Reality

```
┌─────────────────────────────────────────────────────────────┐
│                    QUANTLAB TRUTH TABLE                      │
├─────────────────────────────────────┬───────────────────────┤
│ Infrastructure (Tier-1)             │ ✅ COMPLETE           │
│ Alpha/Signal Layer (Tier-5)         │ ❌ PRIMITIVE          │
└─────────────────────────────────────┴───────────────────────┘
```

**Translation:** System knows HOW to trade, not WHEN to trade.

---

## STOP Working On

- Collectors
- Replay engine
- Exchange bridges
- Infra refactors
- Monitoring improvements
- Deployment changes

Unless critical bug exists.

---

## Primary Objective: LIVE ALPHA STACK

### Task Group 1: Live Feature Layer

Create REAL-TIME features (not batch worker):

**Momentum Layer:**
- RSI (multiple windows)
- ROC (5s, 30s, 2m)
- EMA slopes (fast vs slow)

**Volatility Layer:**
- ATR-like rolling true range
- Volatility regime flag (low/normal/high)

**Microstructure Layer:**
- Spread regime (tight/normal/wide)
- Orderbook imbalance smoothing
- Liquidity pressure proxy

**Regime Layer:**
- Volatility compression detector
- Trend regime detector (slope persistence)

All features MUST be:
- Deterministic
- Replay-safe
- Usable by BOTH ML and strategies

---

### Task Group 2: Strategy Upgrade

**BaselineStrategy → DEMOTED to test role**

Create **StrategyV1** that:
- Combines momentum + volatility + regime filters
- Avoids trading in chop regime
- Adapts position size based on volatility
- Uses feature inputs (not raw price)

---

### Task Group 3: ML Integration (Controlled)

ML remains ADVISORY, but:
- Expose ML confidence to strategy
- Allow position scaling based on ML confidence

---

### Task Group 4: Alpha Validation

Add logging for:
- Feature values at decision time
- Regime state
- Strategy decision reasons

We must see **WHY** trades happen.

---

## Feature Gap Analysis

| Feature | Status | Priority |
|---------|--------|----------|
| RSI | Batch only | HIGH - Move to live |
| EMA | Batch only | HIGH - Move to live |
| ATR | Batch only | HIGH - Move to live |
| VWAP | Batch only | MEDIUM |
| Regime detection | NOT PRESENT | HIGH - Create |
| Liquidity shift | NOT PRESENT | MEDIUM |
| Volatility regime | NOT PRESENT | HIGH - Create |

---

## What "Alpha-Primitive" Means

Current live strategy uses:

| Feature | Level |
|---------|-------|
| mid_price | Raw data |
| spread | Raw microstructure |
| return_1 | Single tick momentum |
| volatility | Basic rolling std |

This is NOT alpha. This is noise trading.

---

## Success Criteria

```
Before: System trades on 4 basic features
After:  System trades on 15+ regime-aware features
        with adaptive position sizing
        and decision reasoning logs
```

---

## File Locations for Alpha Work

| Component | Location |
|-----------|----------|
| Live features | `core/features/builders/` |
| Feature registry | `core/features/FeatureRegistry.js` |
| Strategies | `core/strategy/strategies/` |
| ML adapter | `services/strategyd/runtime/MLDecisionAdapter.js` |
| Batch indicators | `core/worker/feature.js` (reference only) |
