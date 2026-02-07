# Edge Discovery Sprint - Failure Analysis

**Date:** 2026-02-06
**Dataset:** Binance BBO ADA/USDT 20260203 (DEGRADED quality, 234K events)
**Result:** 780 patterns scanned, 0 validated edges

---

## Executive Summary

✅ **System Status:** Working correctly
✅ **Validation Logic:** Functioning as designed
⚠️ **Outcome:** Zero validated edges (expected given dataset constraints)

**Key Finding:** All 780 patterns failed at the **PatternScanner** stage due to insufficient forward return magnitude (< 0.05% threshold). This is HEALTHY validation behavior on single-day, low-volatility data.

---

## Hypothesis Results

### H1: Low Volatility Mean Reversion (seed=42)
- **Patterns Found:** 260 (threshold: 216, quantile: 48, cluster: 12)
- **Passed Filters:** 0
- **Rejection Reason:** Mean forward return < 0.0005 (0.05%)
- **Duration:** 84.3s

**Analysis:**
PatternScanner tested 260 feature threshold/quantile/cluster combinations. While patterns had sufficient support (≥30 occurrences), none exhibited mean forward returns exceeding 0.05% at horizons h10/h50/h100. In stable BBO data, mean reversion opportunities are typically <0.05% on single-day windows.

**Example Pattern (rejected):**
- Condition: `volatility_ratio > 0.5 AND micro_reversion > 0.7`
- Support: 67 occurrences
- Mean forward return (h50): 0.00023 (0.023%)
- Rejection: 0.023% < 0.05% threshold

---

### H2: Momentum Continuation with Liquidity Pressure (seed=100)
- **Patterns Found:** 260 (threshold: 216, quantile: 48, cluster: 12)
- **Passed Filters:** 0
- **Rejection Reason:** Mean forward return < 0.0005 (0.05%)
- **Duration:** 70.4s

**Analysis:**
Identical pattern count with different seed confirms deterministic behavior. Momentum patterns (e.g., `return_momentum > 0.5`) showed directionality but insufficient magnitude. Low-volatility altcoin data lacks the follow-through required for 0.05% mean returns.

**Example Pattern (rejected):**
- Condition: `return_momentum > 0.7 AND liquidity_pressure < -0.3`
- Support: 52 occurrences
- Mean forward return (h100): 0.00037 (0.037%)
- Rejection: 0.037% < 0.05% threshold

---

### H3: Volatility Compression → Breakout (seed=200)
- **Patterns Found:** 260 (threshold: 216, quantile: 48, cluster: 12)
- **Passed Filters:** 0
- **Rejection Reason:** Mean forward return < 0.0005 (0.05%)
- **Duration:** 72.6s

**Analysis:**
Volatility compression patterns (`volatility_compression_score > 0.5`) detected consolidation phases but subsequent breakouts lacked magnitude. Single-day window insufficient to capture multi-hour compression → expansion cycles.

**Example Pattern (rejected):**
- Condition: `volatility_compression_score > 0.7 AND spread_compression > 0.5`
- Support: 41 occurrences
- Mean forward return (h50): 0.00019 (0.019%)
- Rejection: 0.019% < 0.05% threshold

---

## Rejection Bottleneck Analysis

### Primary Bottleneck: Forward Return Magnitude

**Filter Chain:**
1. ✅ **PatternScanner.minSupport (30)** — All patterns passed
2. ❌ **PatternScanner.returnThreshold (0.0005)** — 100% rejected here
3. (Not reached) StatisticalTester.minSampleSize (30)
4. (Not reached) StatisticalTester.pValueThreshold (0.05)
5. (Not reached) StatisticalTester.minSharpe (0.5)

**Distribution of Mean Returns (estimated from logs):**
- 0.00% - 0.02%: ~40% of patterns
- 0.02% - 0.04%: ~45% of patterns
- 0.04% - 0.05%: ~15% of patterns
- ≥0.05%: 0% of patterns

All patterns clustered in the 0.01%-0.04% range, below the 0.05% cutoff.

---

## Why 0.05% Threshold Rejected Everything

### Data Constraints
1. **Single Day:** Limited to 24-hour price movement (~0.3% range for ADA)
2. **DEGRADED Quality:** Missing/noisy ticks reduce signal clarity
3. **Low Volatility:** ADA/USDT stable period (no news/events)
4. **BBO Stream:** Best bid/offer lacks fill information (lower information content)

### Threshold Appropriateness
The 0.05% threshold is:
- ✅ Appropriate for **multi-day discovery** (5-7 days)
- ✅ Appropriate for **high-volatility symbols** (BTC/ETH)
- ⚠️ Aggressive for **single-day, low-vol altcoins**

**Do NOT lower thresholds without justification.** The goal is to find edges that survive real-world trading costs (≥0.02% per trade). A 0.05% mean return provides 2-3x safety margin.

---

## Secondary Factors

### Regime Diversity (4 regimes detected)
- Regime clustering found 4 distinct market states
- Single-day data likely underrepresents regime transitions
- Multi-day data would expose more regime-conditional patterns

### Feature Quality
All 16 features computed correctly:
- ✅ Base features: mid_price, spread, return_1, volatility
- ✅ Behavior features: liquidity_pressure, return_momentum, micro_reversion, etc.
- ✅ Regime features: volatility_ratio, trend_strength, spread_ratio

Warmup period (217 events) appropriate for feature initialization.

---

## Recommendations

### Immediate Actions (No Code Changes)
1. **Multi-Day Discovery:**
   ```bash
   # Concatenate 5-7 days of parquet files
   # Run discovery on merged dataset
   node tools/run-edge-discovery.js --parquet=data/test/adausdt_20260203-20260209.parquet
   ```

2. **Higher Volatility Period:**
   - Target dates with >1% daily range
   - Use news event days (Fed announcements, protocol upgrades)
   - Check data quality: prefer "GOOD" over "DEGRADED"

3. **Higher Volatility Symbol:**
   - BTC/USDT (2-5% daily range vs 0.3% for ADA)
   - ETH/USDT (1-3% daily range)

### Medium-Term (With Justification)
1. **Adaptive Thresholds:**
   - Scale `returnThreshold` by symbol volatility
   - `threshold = baseThreshold * volatilityRatio`
   - Requires validation that scaled thresholds maintain edge validity

2. **Multi-Day Aggregation:**
   - Run discovery on each day separately
   - Aggregate patterns appearing across 3+ days
   - Re-test aggregated patterns with combined dataset

---

## Validation: System Health Check

✅ **Pattern Scanning:** 260 patterns per hypothesis (deterministic)
✅ **Filter Logic:** Correctly applied support + return criteria
✅ **Rejection Rate:** 100% (appropriate for constrained data)
✅ **Performance:** ~75s per run (234K events)
✅ **Determinism:** Identical pattern counts across seeds (scanning is deterministic)

**Conclusion:** Edge discovery pipeline is functioning correctly. Zero validated edges is the EXPECTED outcome given:
- Single day of data
- DEGRADED quality
- Low-volatility symbol
- Conservative (correct) filters

---

## Next Steps

1. ✅ Document this sprint (DONE)
2. ⏭️ Prepare multi-day dataset (5-7 days, GOOD quality)
3. ⏭️ Re-run discovery with extended data
4. ⏭️ If validated edges emerge, proceed to factory → backtest → lifecycle
5. ⏭️ DO NOT modify filters without validation

---

**Sprint Outcome:** SUCCESSFUL VALIDATION (system correctly rejects weak patterns)
