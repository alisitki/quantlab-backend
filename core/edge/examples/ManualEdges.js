/**
 * Manual Edge Definitions
 *
 * These are hand-crafted edges based on domain knowledge and theory.
 * They serve as:
 * 1. Test cases for the edge discovery pipeline
 * 2. Baseline comparisons for discovered edges
 * 3. Examples of edge definition patterns
 *
 * If these edges produce alpha, the behavior features are working.
 * If they don't, the features need iteration before building automatic discovery.
 */

import { Edge } from '../Edge.js';

/**
 * Edge 1: Mean Reversion in Low Volatility
 *
 * Theory: In low-volatility regimes, prices tend to revert to mean.
 * High micro_reversion score + negative momentum = entry signal.
 *
 * Expected: Works in low_vol or sideways regimes
 * Fails in: High volatility or strong trends
 */
export const meanReversionLowVolEdge = new Edge({
  id: 'mean_rev_low_vol_v1',
  name: 'Mean Reversion in Low Volatility',

  entryCondition: (features, regime) => {
    // Must be in low volatility regime
    const lowVol = features.regime_volatility === 0 || features.volatility_ratio < 0.5;
    if (!lowVol) return { active: false };

    // High reversion tendency
    const highReversion = features.micro_reversion > 0.6;

    // Momentum should be negative (price extended)
    const negMomentum = features.return_momentum < -0.3;

    // Optional: Check for divergence (momentum vs pressure)
    const divergence = features.behavior_divergence !== null
      ? Math.abs(features.behavior_divergence) > 0.4
      : false;

    const active = highReversion && negMomentum;

    if (active) {
      // Revert UP (momentum is down, expect reversal)
      return {
        active: true,
        direction: 'LONG',
        confidence: features.micro_reversion
      };
    }

    return { active: false };
  },

  exitCondition: (features, regime, entryTime) => {
    // Exit when momentum turns positive (reversion complete)
    if (features.return_momentum > 0.2) {
      return { exit: true, reason: 'reversion_complete' };
    }

    // Exit if volatility spikes (edge invalidated)
    if (features.regime_volatility === 2 || features.volatility_ratio > 2.0) {
      return { exit: true, reason: 'volatility_spike' };
    }

    return { exit: false };
  },

  regimes: [0, 'low_vol', 'sideways'], // Works in low vol or sideways
  timeHorizon: 10000, // 10 seconds max hold

  expectedAdvantage: {
    mean: 0.0005,      // 0.05% per trade
    std: 0.002,
    sharpe: 0.25,
    winRate: 0.55
  },

  riskProfile: {
    maxDrawdown: 0.02,
    maxLoss: 0.01,
    tailRisk: 0.03
  },

  decayFunction: {
    halfLife: 86400000 * 7, // 1 week (pattern may adapt)
    mechanism: 'market_adaptation'
  },

  discoveryMethod: 'manual_theory',
  status: 'CANDIDATE'
});

/**
 * Edge 2: Momentum Continuation in High Volume
 *
 * Theory: Strong momentum + high liquidity pressure = continuation
 *
 * Expected: Works in trending regimes
 * Fails in: Mean-reverting or choppy markets
 */
export const momentumContinuationEdge = new Edge({
  id: 'momentum_continuation_v1',
  name: 'Momentum Continuation with Pressure',

  entryCondition: (features, regime) => {
    // Strong momentum (trending)
    const strongMomentum = Math.abs(features.return_momentum) > 0.5;

    // Liquidity pressure aligned with momentum
    const pressureAligned = (features.return_momentum > 0 && features.liquidity_pressure > 0.3) ||
                           (features.return_momentum < 0 && features.liquidity_pressure < -0.3);

    // Low reversion (trending, not reverting)
    const lowReversion = features.micro_reversion < 0.4;

    // Trend regime
    const trending = features.regime_trend !== 0;

    const active = strongMomentum && pressureAligned && lowReversion && trending;

    if (active) {
      const direction = features.return_momentum > 0 ? 'LONG' : 'SHORT';
      const confidence = Math.abs(features.return_momentum);

      return { active: true, direction, confidence };
    }

    return { active: false };
  },

  exitCondition: (features, regime, entryTime) => {
    // Exit when momentum weakens
    if (Math.abs(features.return_momentum) < 0.2) {
      return { exit: true, reason: 'momentum_weakened' };
    }

    // Exit if pressure reverses
    const pressureReversed = (features.return_momentum > 0 && features.liquidity_pressure < -0.3) ||
                            (features.return_momentum < 0 && features.liquidity_pressure > 0.3);

    if (pressureReversed) {
      return { exit: true, reason: 'pressure_reversal' };
    }

    // Exit if regime changes to sideways
    if (features.regime_trend === 0) {
      return { exit: true, reason: 'regime_change' };
    }

    return { exit: false };
  },

  regimes: [-1, 1, 'uptrend', 'downtrend'], // Trending regimes only
  timeHorizon: 15000, // 15 seconds

  expectedAdvantage: {
    mean: 0.001,       // 0.1% per trade
    std: 0.003,
    sharpe: 0.33,
    winRate: 0.6
  },

  riskProfile: {
    maxDrawdown: 0.03,
    maxLoss: 0.015,
    tailRisk: 0.05
  },

  decayFunction: {
    halfLife: 86400000 * 3, // 3 days (momentum edges decay fast)
    mechanism: 'crowding'
  },

  discoveryMethod: 'manual_theory',
  status: 'CANDIDATE'
});

/**
 * Edge 3: Volatility Breakout after Compression
 *
 * Theory: Volatility compression precedes expansion (breakout)
 * High compression score = imminent move
 *
 * Expected: Works when volatility_compression_score is high
 * Fails in: Already volatile markets
 */
export const volatilityBreakoutEdge = new Edge({
  id: 'vol_breakout_compression_v1',
  name: 'Volatility Breakout after Compression',

  entryCondition: (features, regime) => {
    // High compression score
    const highCompression = features.volatility_compression_score > 0.7;

    // Spread is narrowing
    const narrowingSpread = features.spread_compression > 0.3;

    // Quote intensity increasing (activity spike)
    const activitySpike = features.quote_intensity > 0.7;

    // Current volatility still low
    const stillLowVol = features.volatility_ratio < 1.0;

    const active = highCompression && narrowingSpread && activitySpike && stillLowVol;

    if (active) {
      // Direction: follow the momentum bias (if any)
      const direction = features.return_momentum > 0.1 ? 'LONG' :
                       features.return_momentum < -0.1 ? 'SHORT' :
                       'LONG'; // Default long if no bias

      return {
        active: true,
        direction,
        confidence: features.volatility_compression_score
      };
    }

    return { active: false };
  },

  exitCondition: (features, regime, entryTime) => {
    // Exit when volatility has expanded
    if (features.volatility_ratio > 1.5) {
      return { exit: true, reason: 'volatility_expanded' };
    }

    // Exit if compression score drops (false signal)
    if (features.volatility_compression_score < 0.4) {
      return { exit: true, reason: 'compression_failed' };
    }

    return { exit: false };
  },

  regimes: null, // Works in any regime (compression can happen anywhere)
  timeHorizon: 20000, // 20 seconds (breakouts take time)

  expectedAdvantage: {
    mean: 0.0015,      // 0.15% per trade
    std: 0.005,
    sharpe: 0.3,
    winRate: 0.5
  },

  riskProfile: {
    maxDrawdown: 0.04,
    maxLoss: 0.02,
    tailRisk: 0.06
  },

  decayFunction: {
    halfLife: 86400000 * 14, // 2 weeks (pattern more stable)
    mechanism: 'market_structure_change'
  },

  discoveryMethod: 'manual_theory',
  status: 'CANDIDATE'
});

/**
 * Export all manual edges
 */
export const MANUAL_EDGES = [
  meanReversionLowVolEdge,
  momentumContinuationEdge,
  volatilityBreakoutEdge
];
