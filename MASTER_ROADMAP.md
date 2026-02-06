# QuantLab Master Roadmap
## Edge Discovery & Strategy Factory System

> **CANONICAL REFERENCE DOCUMENT**
> Last Updated: 2026-02-05

---

QuantLab sıradan bir strategy runner değildir.
QuantLab, edge discovery & strategy factory sistemidir.

Strategy merkez değildir.
Edge discovery sistemin motorudur.

---

## Phase 0 — Deterministic Data Infrastructure ✅

**Status:** COMPLETE

**Components:**
- Multi-exchange WebSocket collector (Binance, Bybit, OKX)
- Atomic write protocol (fsync + rename)
- Parquet compaction pipeline
- S3 storage with corruption quarantine
- DuckDB-based replay engine
- Cursor-based resume capability
- ORDERING_CONTRACT enforcement (`ts_event ASC, seq ASC`)

**Guarantees:**
- Deterministic event ordering
- Data durability
- Replay consistency

---

## Phase 1 — Execution & Safety Platform ✅

**Status:** COMPLETE

**Components:**
- Paper trading execution engine (memory-optimized: 99.998% reduction)
- RiskManager with 4 rule types
- Kill switch (HTTP + environment variable)
- Human approval gate
- Budget limits
- Exchange adapters (Binance, Bybit, OKX)
- Position reconciliation
- Promotion guards

**Guarantees:**
- Deterministic fills
- Risk enforcement
- Emergency stop capability

---

## Phase 2 — Behavior Modeling Layer

**Status:** NOT STARTED

**Amaç:** Piyasayı fiyat değil davranış olarak modellemek

**Target Features:**
- Liquidity pressure detection
- Aggression persistence tracking
- Absorption detection (large orders eating flow)
- Sweep events (stop hunt patterns)
- Micro trend strength
- Volatility compression/expansion
- Quote stuffing detection
- Cross-timeframe momentum divergence
- Volume profile anomalies

**Current State:**
- Only 3 primitive regime features exist (volatility/trend/spread)
- No order flow analysis
- No liquidity modeling

**Çıktı:** Market Behavior Vector (rich feature set capturing participant intentions)

**Implementation Notes:**
- Build on top of existing FeatureRegistry
- Must maintain determinism
- Must be replay-safe
- Should be composable

---

## Phase 3 — Regime Detection Engine

**Status:** PRIMITIVE (threshold-based only)

**Amaç:** Data-driven market state classification

**Components:**
- Behavior clustering (unsupervised learning)
- Regime labeling (interpretable states)
- Regime transition tracking
- Stability score computation
- Regime persistence measurement

**Methods:**
1. Clustering-based (K-means, DBSCAN on behavior vectors)
2. HMM-based (probabilistic regime transitions)
3. Change-point detection

**Current State:**
- Simple threshold-based regime detection
- No clustering
- No probabilistic transitions

**Çıktı:**
- Discrete regime labels (e.g., TRENDING, MEAN_REVERTING, VOLATILE, QUIET)
- Regime transition probabilities
- Regime stability metrics

---

## Phase 4 — Edge Abstraction

**Status:** NOT IMPLEMENTED

**Amaç:** Formalize what an "edge" is in QuantLab

**Edge Object Definition:**

```javascript
{
  id: string,                    // Unique edge identifier
  name: string,                  // Human-readable name

  // Pattern definition
  entry: {
    behaviorPattern: [],         // Required behavior signatures
    regimes: [],                 // Valid regimes for this edge
    conditions: {}               // Entry logic
  },

  exit: {
    behaviorPattern: [],         // Exit behavior signatures
    conditions: {},              // Exit logic
    timeHorizon: number          // Expected holding period
  },

  // Expected properties
  expectedAdvantage: {
    mean: number,                // Expected return
    std: number,                 // Return volatility
    distribution: string         // Return distribution type
  },

  riskProfile: {
    maxDrawdown: number,
    sharpeRatio: number,
    winRate: number
  },

  // Edge characteristics
  regimeDependency: {
    regimes: [],                 // Which regimes does this work in?
    sensitivity: number          // How regime-dependent?
  },

  decayFunction: {
    halfLife: number,            // How fast does edge decay?
    mechanism: string            // Why does it decay? (usage, market adaptation)
  },

  // Validation
  confidence: {
    score: number,               // Statistical confidence
    sampleSize: number,
    lastValidated: timestamp
  },

  // Metadata
  discovered: timestamp,
  discoveryMethod: string,
  status: string                 // CANDIDATE, VALIDATED, DEPLOYED, DECAYED, RETIRED
}
```

**File Location:** `core/edge/Edge.js`

---

## Phase 5 — Edge Discovery Engine

**Status:** NOT IMPLEMENTED

**Amaç:** Automatically discover edges from behavior data

**Components:**

1. **Pattern Mining**
   - Sequential pattern detection in behavior vectors
   - Regime-conditioned pattern search
   - Minimum support/confidence filtering

2. **Regime-Conditioned Statistics**
   - Calculate return distributions per regime
   - Test for statistically significant differences
   - Identify regime-specific advantages

3. **Anomaly Detection**
   - Detect unusual behavior patterns
   - Test if anomalies predict returns
   - Validate anomaly persistence

4. **Edge Candidate Generation**
   - Combine patterns + regimes + statistics
   - Generate edge hypotheses
   - Queue for validation

**Input:** Behavior vectors + regime labels + price outcomes
**Output:** Edge candidates (unvalidated)

**File Location:** `core/edge/EdgeDiscoveryEngine.js`

---

## Phase 6 — Edge Validation Framework

**Status:** NOT IMPLEMENTED

**Amaç:** Statistically validate edge candidates

**Validation Tests:**

1. **Stability Test**
   - Does edge persist across time?
   - Walk-forward analysis
   - Monte Carlo simulation

2. **Regime Dependency Test**
   - Does edge work in claimed regimes?
   - Does edge fail in other regimes?
   - Regime transition robustness

3. **Distribution Test**
   - Does return distribution match expectation?
   - Are tails acceptable?
   - Is skew/kurtosis in line?

4. **Decay Tracking**
   - Is edge decaying faster than expected?
   - Should edge be retired?

**Validation Checklist:**
- [ ] Sample size > minimum threshold
- [ ] Out-of-sample Sharpe > 1.0
- [ ] Win rate statistically > 50% (or asymmetric payoff)
- [ ] Max drawdown within tolerance
- [ ] Consistent across multiple periods
- [ ] Works in target regime
- [ ] Decay rate acceptable
- [ ] Not explained by transaction costs

**Output:** VALIDATED or REJECTED edge

**File Location:** `core/edge/EdgeValidator.js`

---

## Phase 7 — Strategy Factory

**Status:** NOT IMPLEMENTED

**Amaç:** Automatically generate strategies from validated edges

**Pipeline:**

```
Validated Edge → Strategy Template → Parameter Optimization → Backtest → Deploy
```

**Strategy Templates:**

| Template | Suitable For |
|----------|--------------|
| Momentum | Trend-following edges |
| Mean Reversion | Overextension edges |
| Breakout | Range-boundary edges |
| Scalping | Microstructure edges |
| Arbitrage | Cross-venue edges |

**Process:**
1. Select template based on edge characteristics
2. Map edge entry/exit to strategy logic
3. Optimize parameters (position sizing, timeouts, etc.)
4. Backtest with transaction costs
5. Generate strategy code
6. Register in lifecycle system

**Output:** Executable strategy (ready for lifecycle)

**File Location:** `core/strategy/factory/StrategyFactory.js`

---

## Phase 8 — Strategy Lifecycle System

**Status:** NOT IMPLEMENTED (PromotionGuardManager exists but not integrated)

**Amaç:** Manage strategy birth, growth, and death

**Lifecycle Stages:**

```
CANDIDATE → PAPER → CANARY → SHADOW → LIVE → RETIRED
```

| Stage | Capital | Monitoring | Promotion Criteria |
|-------|---------|------------|--------------------|
| CANDIDATE | 0% | Simulated only | Pass backtest + validation |
| PAPER | 0% | Paper trading | N days positive Sharpe |
| CANARY | 1% | Live with limits | Sharpe > threshold, DD < limit |
| SHADOW | 5% | Full monitoring | Consistent performance |
| LIVE | 100% | Production | Proven at scale |
| RETIRED | 0% | Archived | Edge decayed or violated limits |

**Promotion Rules:**
- Promote if Sharpe > threshold for N days
- Promote if drawdown < tolerance
- Promote if slippage within expectations

**Demotion Rules:**
- Demote if Sharpe < threshold for M days
- Demote if drawdown exceeds 2x historical
- Demote if edge decay detected

**Kill Conditions:**
- Immediate retirement if max drawdown breached
- Immediate retirement if statistical edge invalidated
- User-initiated kill switch

**File Location:** `core/strategy/lifecycle/StrategyLifecycleManager.js`

---

## Phase 9 — Closed-Loop Learning

**Status:** NOT IMPLEMENTED

**Amaç:** System learns from its own trade outcomes

**Feedback Loops:**

1. **Trade Outcomes → Behavior Refinement**
   - Which behavior features predicted wins?
   - Which features were noise?
   - Update feature weights

2. **Performance → Edge Scoring**
   - Update edge confidence based on live results
   - Adjust edge decay estimates
   - Retire edges faster if not performing

3. **Edge Performance → Strategy Adaptation**
   - Adjust position sizing based on recent edge strength
   - Modify entry thresholds dynamically
   - Adapt to changing market conditions

**Metrics to Track:**
- Feature importance over time
- Edge persistence vs prediction
- Strategy performance vs expected
- Regime detection accuracy

**Update Frequency:**
- Real-time: Individual trade outcomes
- Daily: Feature importance recalculation
- Weekly: Edge validation refresh
- Monthly: Strategy lifecycle review

**File Location:** `core/learning/ClosedLoopLearning.js`

---

## Phase 10 — Controlled Live Deployment

**Status:** INFRASTRUCTURE READY, waiting for edge layer

**Components:**
- ✅ Exchange adapters (Binance, Bybit, OKX)
- ✅ Live WebSocket consumer
- ✅ Event sequencing
- ✅ Kill switch
- ✅ Human approval gate
- ✅ Position reconciliation
- ✅ Risk management
- ✅ Audit trail

**Deployment Protocol:**
1. Edge validated in Phase 6
2. Strategy generated in Phase 7
3. Strategy enters lifecycle at CANDIDATE
4. Progressive promotion: PAPER → CANARY → SHADOW → LIVE
5. Continuous monitoring
6. Automatic demotion on violation

**Guardrails:**
- Max position size per strategy
- Max total exposure across all strategies
- Max daily loss limit
- Max number of simultaneous live strategies
- Human approval for first N trades

---

## Phase 11 — Capital Scaling Logic

**Status:** NOT IMPLEMENTED

**Amaç:** Dynamically allocate capital based on edge strength

**Allocation Principles:**
1. Allocate more capital to higher-confidence edges
2. Reduce capital to decaying edges
3. Diversify across regimes
4. Respect correlation constraints

**Formula:**

```
Strategy Capital = Base Capital × Edge Confidence × Regime Stability × (1 - Correlation Penalty)
```

**Constraints:**
- Min capital per strategy (to remain viable)
- Max capital per strategy (to avoid market impact)
- Max total exposure (risk management)

**Dynamic Adjustment:**
- Increase capital after consecutive wins
- Decrease capital after drawdown
- Withdraw capital on edge decay

**File Location:** `core/capital/CapitalAllocator.js`

---

## Core Principle

```
OLD MODEL (Deprecated):
Data → Features → Strategy → ML → Execution

NEW MODEL (Active):
Data → Behavior → Regime → EDGE → Strategy → Risk → Execution
```

---

## Development Rules

### Mandatory Question
For ANY development request:
> "Bu değişiklik roadmap'te hangi faza hizmet ediyor?"

### Prohibited Actions
- Implementing features not aligned with current roadmap phase
- Writing strategies without edge definition
- Adding ML for signal prediction (ML is for edge discovery)
- Treating backtest PnL as proof of edge

### Priority Order
1. Complete current phase before starting next
2. Phases 2-6 are CRITICAL (edge discovery core)
3. Execution optimization is LOW priority (already complete)
4. Strategy variants are LOW priority (need edge factory first)

---

## Current Status (2026-02-06)

| Phase | Status |
|-------|--------|
| Phase 0 — Data Infrastructure | ✅ COMPLETE |
| Phase 1 — Execution & Safety | ✅ COMPLETE |
| Phase 2 — Behavior Modeling | ✅ COMPLETE (9 features) |
| Phase 3 — Regime Detection | ✅ COMPLETE (continuous + clustering) |
| Phase 4 — Edge Abstraction | ✅ COMPLETE (Edge class + 3 manual edges) |
| Phase 5 — Edge Discovery | ❌ NOT STARTED |
| Phase 6 — Edge Validation | ⚠️ READY (needs data) |
| Phase 7 — Strategy Factory | ❌ NOT STARTED |
| Phase 8 — Strategy Lifecycle | ⚠️ PARTIAL |
| Phase 9 — Closed-Loop Learning | ❌ NOT STARTED |
| Phase 10 — Controlled Live | ⏸️ READY (waiting for edge validation) |
| Phase 11 — Capital Scaling | ❌ NOT STARTED |

**Next Focus:** Phase 6 (Edge Validation) - requires sample data

---

## Success Criteria

**System is ready for production when:**
1. Multiple validated edges exist (Phase 6 complete)
2. Strategy factory is operational (Phase 7 complete)
3. Lifecycle system manages 5+ concurrent strategies (Phase 8 complete)
4. Closed-loop learning is active (Phase 9 complete)
5. At least 3 strategies are in LIVE stage (Phase 10 active)

**System is NOT ready until:**
- Edge discovery engine produces validated edges
- Strategy factory can generate strategies from edges
- Lifecycle system proves strategies can be promoted/demoted

---

*This document is the canonical reference for QuantLab development direction.*
*All development decisions must align with this roadmap.*
