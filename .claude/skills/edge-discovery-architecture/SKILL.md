---
name: edge-discovery-architecture
description: Edge discovery system design, behavior modeling, and strategy factory principles
---

# Edge Discovery Architecture

> **QUANTLAB CORE IDENTITY:** Edge Discovery & Strategy Factory System
>
> Strategy is OUTPUT, not CENTER. Edge discovery is the engine.

---

## What is an Edge? (QuantLab Context)

An **edge** is a statistically validated, exploitable market inefficiency with:

1. **Entry Condition** — When does the edge appear?
2. **Exit Condition** — When does the edge disappear?
3. **Expected Magnitude** — How much alpha does it generate?
4. **Decay Function** — How does the edge erode over time/usage?
5. **Regime Dependency** — In which market conditions does it work?

### Edge vs Signal vs Feature

| Concept | Definition | Example |
|---------|------------|---------|
| **Feature** | Raw market measurement | RSI, spread, volatility |
| **Signal** | Directional prediction | BUY/SELL indicator |
| **Edge** | Validated exploitable pattern | "RSI < 30 in low-vol regime → mean reversion with 60% win rate over 5s horizon" |

**Key insight:** Features and signals don't guarantee profit. Edges do (statistically).

---

## Behavior Modeling Principles

### What is Behavior?

Market behavior = observable patterns in price, volume, and order flow that indicate participant intentions.

### Behavior Categories

1. **Momentum Behavior** — Trend persistence, breakout patterns
2. **Mean Reversion Behavior** — Overextension, snap-back patterns
3. **Absorption Behavior** — Large orders absorbing flow
4. **Liquidity Behavior** — Spread dynamics, depth changes
5. **Cross-Asset Behavior** — Correlation shifts, lead-lag

### Behavior Extraction Pipeline

```
Raw Data → Microstructure Features → Behavior Indicators → Regime Classification
```

### Current State (Primitive)

Only 3 regime features exist:
- `regime_volatility` (LOW/NORMAL/HIGH)
- `regime_trend` (DOWN/SIDE/UP)
- `regime_spread` (TIGHT/NORMAL/WIDE)

### Target State (Rich)

- Order flow imbalance persistence
- Liquidity absorption detection
- Cross-timeframe momentum divergence
- Volume profile anomalies
- Quote stuffing detection

---

## Regime Detection Rules

### What is a Regime?

A **regime** is a distinct market state where:
- Certain edges work
- Certain edges fail
- Behavior patterns are consistent

### Regime Types

| Regime | Characteristics | Edge Implications |
|--------|-----------------|-------------------|
| Trending | Persistent direction, low reversion | Momentum edges work |
| Mean-Reverting | High reversion rate, range-bound | Mean reversion edges work |
| Volatile | High ATR, unpredictable | Most edges fail |
| Quiet | Low ATR, tight spreads | Scalping edges work |
| Transitional | Regime change in progress | All edges unreliable |

### Regime Detection Methods

1. **Threshold-based** (current) — Simple but brittle
2. **Clustering-based** (target) — Data-driven, adaptive
3. **HMM-based** (advanced) — Probabilistic regime transitions

### Regime Change Protocol

When regime changes:
1. Close all edge-dependent positions
2. Re-evaluate which edges are valid
3. Adjust position sizing accordingly

---

## Edge Validation Methods

### Why Validation Matters

Backtest PnL is NOT sufficient because:
- Overfitting to historical data
- Survivorship bias
- Look-ahead bias
- Transaction cost underestimation

### Statistical Validation Requirements

1. **Sample Size** — Minimum N trades for significance
2. **Out-of-Sample Testing** — Train/test split mandatory
3. **Walk-Forward Analysis** — Rolling validation windows
4. **Monte Carlo Simulation** — Randomized path testing
5. **Regime Stratification** — Edge must work in target regime

### Edge Validation Checklist

```
[ ] Win rate statistically > 50% (or asymmetric payoff)
[ ] Sharpe ratio > 1.0 after costs
[ ] Max drawdown within tolerance
[ ] Consistent across multiple time periods
[ ] Works in target regime (not all regimes)
[ ] Decay rate acceptable
[ ] Not explained by transaction costs
```

### Invalidation Triggers

An edge is INVALID if:
- Win rate degrades below threshold
- Sharpe ratio drops below 0.5
- Drawdown exceeds 2x historical
- Regime no longer appears in market

---

## Strategy Factory Principles

### What is Strategy Factory?

Automatic conversion of validated edges into executable strategies.

### Factory Pipeline

```
Validated Edge → Strategy Template → Parameter Optimization → Backtest → Deploy
```

### Strategy Template Types

| Template | Suitable For |
|----------|--------------|
| Momentum | Trend-following edges |
| Mean Reversion | Overextension edges |
| Breakout | Range-boundary edges |
| Scalping | Microstructure edges |

### Strategy Lifecycle

```
CANDIDATE → PAPER → CANARY → SHADOW → LIVE → RETIRED
```

| Stage | Capital | Monitoring |
|-------|---------|------------|
| CANDIDATE | 0 | Simulated only |
| PAPER | 0 | Paper trading |
| CANARY | 1% | Live with limits |
| SHADOW | 5% | Full monitoring |
| LIVE | 100% | Production |
| RETIRED | 0 | Archived |

### Promotion Rules

Promote if:
- Sharpe > threshold for N days
- Drawdown < threshold
- Slippage within expectations

Demote if:
- Sharpe < threshold for M days
- Drawdown exceeds tolerance
- Edge decay detected

---

## Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| Edge Abstraction | NOT IMPLEMENTED | — |
| Behavior Extraction | PRIMITIVE | `core/features/builders/*Regime*.js` |
| Regime Detection | BASIC (threshold) | `RegimeModeSelector.js` |
| Edge Validation | NOT IMPLEMENTED | — |
| Strategy Factory | NOT IMPLEMENTED | — |
| Strategy Lifecycle | NOT IMPLEMENTED | — |

---

## Next Steps

1. **Define Edge Interface** — `core/edge/Edge.js`
2. **Build Behavior Feature Stack** — Enhanced order flow features
3. **Implement Regime Clustering** — Unsupervised regime detection
4. **Create Edge Validator** — Statistical validation framework
5. **Build Strategy Factory** — Edge → Strategy automation

---

*This skill defines the architectural principles for QuantLab's edge discovery system.*
