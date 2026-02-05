/**
 * Combiner - Alpha-Weighted Signal Combination
 *
 * Birden fazla sinyali birleştirir ve final karar verir.
 * Supports: majority, unanimous, weighted modes.
 */

import { SIGNAL_DIRECTION } from './SignalGenerator.js';

/**
 * Action types
 */
export const ACTION = {
  LONG: 'LONG',
  SHORT: 'SHORT',
  HOLD: 'HOLD'
};

/**
 * Combination modes
 */
export const COMBINE_MODE = {
  MAJORITY: 'majority',       // Çoğunluk aynı yönde → trade
  UNANIMOUS: 'unanimous',     // Tüm sinyaller aynı yönde → trade
  WEIGHTED: 'weighted'        // Alpha score ağırlıklı ortalama
};

/**
 * Default combiner config
 */
const DEFAULT_CONFIG = {
  mode: COMBINE_MODE.WEIGHTED,
  minStrength: 0.3,           // Minimum signal strength
  minSignals: 2,              // Minimum agreeing signals
  confidenceThreshold: 0.5    // Minimum confidence for action
};

export class Combiner {
  #config;

  /**
   * @param {Object} config
   */
  constructor(config = {}) {
    this.#config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Combine signals into a trading decision
   * @param {Array} signals - Array of Signal objects
   * @returns {Object} { action, confidence, details }
   */
  combine(signals) {
    // Filter weak signals
    const validSignals = signals.filter(s =>
      s.strength >= this.#config.minStrength &&
      s.direction !== SIGNAL_DIRECTION.NEUTRAL
    );

    if (validSignals.length === 0) {
      return {
        action: ACTION.HOLD,
        confidence: 0,
        reason: 'no_valid_signals',
        details: { totalSignals: signals.length, validSignals: 0 }
      };
    }

    // Route to appropriate combination method
    switch (this.#config.mode) {
      case COMBINE_MODE.MAJORITY:
        return this.#combineMajority(validSignals);
      case COMBINE_MODE.UNANIMOUS:
        return this.#combineUnanimous(validSignals);
      case COMBINE_MODE.WEIGHTED:
      default:
        return this.#combineWeighted(validSignals);
    }
  }

  /**
   * Majority voting combination
   * @param {Array} signals
   * @returns {Object}
   */
  #combineMajority(signals) {
    let longCount = 0;
    let shortCount = 0;
    let longStrength = 0;
    let shortStrength = 0;

    for (const signal of signals) {
      if (signal.direction === SIGNAL_DIRECTION.LONG) {
        longCount++;
        longStrength += signal.strength;
      } else if (signal.direction === SIGNAL_DIRECTION.SHORT) {
        shortCount++;
        shortStrength += signal.strength;
      }
    }

    const totalSignals = signals.length;
    const majorityRequired = Math.ceil(totalSignals / 2);

    let action = ACTION.HOLD;
    let confidence = 0;
    let reason = 'no_majority';

    if (longCount > shortCount && longCount >= this.#config.minSignals) {
      action = ACTION.LONG;
      confidence = (longCount / totalSignals) * (longStrength / longCount);
      reason = `majority_long (${longCount}/${totalSignals})`;
    } else if (shortCount > longCount && shortCount >= this.#config.minSignals) {
      action = ACTION.SHORT;
      confidence = (shortCount / totalSignals) * (shortStrength / shortCount);
      reason = `majority_short (${shortCount}/${totalSignals})`;
    }

    return {
      action,
      confidence,
      reason,
      details: {
        longCount,
        shortCount,
        longStrength,
        shortStrength,
        totalSignals
      }
    };
  }

  /**
   * Unanimous agreement combination
   * @param {Array} signals
   * @returns {Object}
   */
  #combineUnanimous(signals) {
    if (signals.length < this.#config.minSignals) {
      return {
        action: ACTION.HOLD,
        confidence: 0,
        reason: `insufficient_signals (${signals.length}/${this.#config.minSignals})`,
        details: { totalSignals: signals.length }
      };
    }

    const firstDirection = signals[0].direction;
    const isUnanimous = signals.every(s => s.direction === firstDirection);

    if (!isUnanimous) {
      return {
        action: ACTION.HOLD,
        confidence: 0,
        reason: 'not_unanimous',
        details: {
          longCount: signals.filter(s => s.direction === SIGNAL_DIRECTION.LONG).length,
          shortCount: signals.filter(s => s.direction === SIGNAL_DIRECTION.SHORT).length
        }
      };
    }

    // Calculate average strength
    const avgStrength = signals.reduce((sum, s) => sum + s.strength, 0) / signals.length;

    return {
      action: firstDirection === SIGNAL_DIRECTION.LONG ? ACTION.LONG : ACTION.SHORT,
      confidence: avgStrength,
      reason: `unanimous_${firstDirection === SIGNAL_DIRECTION.LONG ? 'long' : 'short'}`,
      details: {
        signalCount: signals.length,
        avgStrength,
        signals: signals.map(s => ({ feature: s.feature, strength: s.strength }))
      }
    };
  }

  /**
   * Alpha-weighted combination
   * @param {Array} signals
   * @returns {Object}
   */
  #combineWeighted(signals) {
    let weightedLong = 0;
    let weightedShort = 0;
    let totalWeight = 0;

    for (const signal of signals) {
      const weight = signal.alphaScore || signal.strength;

      if (signal.direction === SIGNAL_DIRECTION.LONG) {
        weightedLong += weight * signal.strength;
      } else if (signal.direction === SIGNAL_DIRECTION.SHORT) {
        weightedShort += weight * signal.strength;
      }

      totalWeight += weight;
    }

    if (totalWeight === 0) {
      return {
        action: ACTION.HOLD,
        confidence: 0,
        reason: 'zero_weight',
        details: {}
      };
    }

    // Normalize
    const normalizedLong = weightedLong / totalWeight;
    const normalizedShort = weightedShort / totalWeight;

    // Calculate net direction and confidence
    const netDirection = normalizedLong - normalizedShort;
    const confidence = Math.abs(netDirection);

    let action = ACTION.HOLD;
    let reason = 'below_threshold';

    if (confidence >= this.#config.confidenceThreshold) {
      if (netDirection > 0) {
        action = ACTION.LONG;
        reason = `weighted_long (conf: ${confidence.toFixed(3)})`;
      } else {
        action = ACTION.SHORT;
        reason = `weighted_short (conf: ${confidence.toFixed(3)})`;
      }
    }

    return {
      action,
      confidence,
      reason,
      details: {
        weightedLong,
        weightedShort,
        totalWeight,
        normalizedLong,
        normalizedShort,
        netDirection,
        signalCount: signals.length
      }
    };
  }

  /**
   * Apply mode-based adjustments to signals before combining
   * @param {Array} signals
   * @param {Object} mode - RegimeModeSelector output
   * @returns {Array} Adjusted signals
   */
  applyModeAdjustments(signals, mode) {
    if (!mode || !mode.combined) return signals;

    return signals.map(signal => {
      let adjustedStrength = signal.strength;

      // Apply trend penalties
      if (signal.direction === SIGNAL_DIRECTION.LONG && mode.combined.longPenalty) {
        adjustedStrength *= mode.combined.longPenalty;
      }
      if (signal.direction === SIGNAL_DIRECTION.SHORT && mode.combined.shortPenalty) {
        adjustedStrength *= mode.combined.shortPenalty;
      }

      // Apply breakout penalty in sideways mode
      if (mode.combined.breakoutPenalty && this.#isBreakoutSignal(signal)) {
        adjustedStrength *= mode.combined.breakoutPenalty;
      }

      return {
        ...signal,
        strength: adjustedStrength,
        originalStrength: signal.strength,
        modeAdjusted: true
      };
    });
  }

  /**
   * Check if signal is a breakout-type signal
   * @param {Object} signal
   * @returns {boolean}
   */
  #isBreakoutSignal(signal) {
    // Breakout signals typically have high strength and specific features
    const breakoutFeatures = ['roc', 'ema_slope', 'momentum'];
    return breakoutFeatures.some(f => signal.feature?.includes(f)) && signal.strength > 0.7;
  }

  /**
   * Get current configuration
   * @returns {Object}
   */
  getConfig() {
    return { ...this.#config };
  }

  /**
   * Update configuration
   * @param {Object} updates
   */
  updateConfig(updates) {
    this.#config = { ...this.#config, ...updates };
  }
}

export default Combiner;
