/**
 * SignalGate - Decision Gating Layer
 *
 * Prevents noise trading by applying structural filters between
 * signal generation and trade execution.
 *
 * A trade decision must pass ALL gate rules to be executed:
 * - Regime Gate: Trade only in favorable regime conditions
 * - Signal Strength Gate: Minimum signal quality threshold
 * - Cooldown Gate: Prevent over-trading with time-based cooldown
 * - Spread Penalty Gate: Avoid trading in wide spread conditions
 *
 * This layer is TRANSPARENT to strategy logic.
 */

/**
 * Gate rule names for logging
 */
export const GATE_RULE = {
  REGIME_TREND: 'regime_trend',
  REGIME_VOLATILITY: 'regime_volatility',
  REGIME_SPREAD: 'regime_spread',
  SIGNAL_STRENGTH: 'signal_strength',
  COOLDOWN: 'cooldown',
  SPREAD_PENALTY: 'spread_penalty'
};

/**
 * Default gate configuration
 */
const DEFAULT_GATE_CONFIG = {
  // Regime thresholds
  regimeTrendMin: -0.5,         // Allow trend >= -0.5 (not strong downtrend for longs)
  regimeVolMin: 0,              // Allow all volatility regimes (0=LOW, 1=NORMAL, 2=HIGH)
  regimeSpreadMax: 2,           // Block only VERY_WIDE spread (>2)

  // Signal quality
  minSignalScore: 0.6,          // Minimum confidence threshold

  // Cooldown
  cooldownMs: 5000,             // 5 seconds between trades (prevents rapid fire)

  // Spread penalty
  maxSpreadNormalized: 0.001,   // Max spread/mid ratio (0.1%)

  // Logging
  logBlockedTrades: true
};

export class SignalGate {
  #config;
  #blockedCount = 0;
  #passedCount = 0;
  #blockReasons = new Map(); // reason -> count

  /**
   * @param {Object} config - Gate configuration
   */
  constructor(config = {}) {
    this.#config = { ...DEFAULT_GATE_CONFIG, ...config };
  }

  /**
   * Evaluate if a trade decision should be allowed
   *
   * @param {Object} params
   * @param {number} params.signalScore - Decision confidence (0-1)
   * @param {Object} params.features - Current feature values
   * @param {Object} params.regime - Regime values {volatility, trend, spread}
   * @param {Object} params.mode - Current mode selection
   * @param {number|null} params.lastTradeTime - Timestamp of last trade (ms)
   * @param {number} params.now - Current timestamp (ms)
   * @returns {{ allow: boolean, reason: string }}
   */
  evaluate({ signalScore, features, regime, mode, lastTradeTime, now }) {
    // (A) Regime Gate
    const regimeCheck = this.#checkRegimeGate(regime, mode);
    if (!regimeCheck.pass) {
      this.#recordBlock(regimeCheck.reason);
      return { allow: false, reason: regimeCheck.reason };
    }

    // (B) Signal Strength Gate
    if (signalScore < this.#config.minSignalScore) {
      const reason = `${GATE_RULE.SIGNAL_STRENGTH}: ${signalScore.toFixed(3)} < ${this.#config.minSignalScore}`;
      this.#recordBlock(reason);
      return { allow: false, reason };
    }

    // (C) Cooldown Gate
    if (lastTradeTime !== null && lastTradeTime !== undefined) {
      const timeSinceLastTrade = now - lastTradeTime;
      if (timeSinceLastTrade < this.#config.cooldownMs) {
        const reason = `${GATE_RULE.COOLDOWN}: ${timeSinceLastTrade}ms < ${this.#config.cooldownMs}ms`;
        this.#recordBlock(reason);
        return { allow: false, reason };
      }
    }

    // (D) Spread Penalty Gate
    const spreadNormalized = this.#calculateSpreadNormalized(features);
    if (spreadNormalized > this.#config.maxSpreadNormalized) {
      const reason = `${GATE_RULE.SPREAD_PENALTY}: ${spreadNormalized.toFixed(6)} > ${this.#config.maxSpreadNormalized}`;
      this.#recordBlock(reason);
      return { allow: false, reason };
    }

    // All gates passed
    this.#passedCount++;
    return { allow: true, reason: 'all_gates_passed' };
  }

  /**
   * Check regime-based gate rules
   * @private
   */
  #checkRegimeGate(regime, mode) {
    const { regimeTrendMin, regimeVolMin, regimeSpreadMax } = this.#config;

    // Map categorical regime values to scores
    const trendScore = this.#getTrendScore(regime.trend);
    const volScore = regime.volatility ?? 1;
    const spreadScore = regime.spread ?? 1;

    // Trend check
    if (trendScore < regimeTrendMin) {
      return {
        pass: false,
        reason: `${GATE_RULE.REGIME_TREND}: ${trendScore.toFixed(2)} < ${regimeTrendMin}`
      };
    }

    // Volatility check
    if (volScore < regimeVolMin) {
      return {
        pass: false,
        reason: `${GATE_RULE.REGIME_VOLATILITY}: ${volScore} < ${regimeVolMin}`
      };
    }

    // Spread check
    if (spreadScore > regimeSpreadMax) {
      return {
        pass: false,
        reason: `${GATE_RULE.REGIME_SPREAD}: ${spreadScore} > ${regimeSpreadMax}`
      };
    }

    return { pass: true };
  }

  /**
   * Convert trend regime to score
   * -1 (DOWNTREND) = -1.0
   *  0 (SIDEWAYS)  =  0.0
   *  1 (UPTREND)   =  1.0
   * @private
   */
  #getTrendScore(trendRegime) {
    if (typeof trendRegime === 'number') {
      return trendRegime; // Already numeric
    }
    return 0; // Default to neutral
  }

  /**
   * Calculate normalized spread
   * @private
   */
  #calculateSpreadNormalized(features) {
    const spread = features.spread ?? 0;
    const midPrice = features.mid_price ?? 1;

    if (midPrice === 0) return 0;
    return spread / midPrice;
  }

  /**
   * Record a blocked trade
   * @private
   */
  #recordBlock(reason) {
    this.#blockedCount++;
    const count = this.#blockReasons.get(reason) || 0;
    this.#blockReasons.set(reason, count + 1);
  }

  /**
   * Get gate statistics
   * @returns {Object}
   */
  getStats() {
    const total = this.#passedCount + this.#blockedCount;
    const passRate = total > 0 ? (this.#passedCount / total) : 0;

    return {
      passed: this.#passedCount,
      blocked: this.#blockedCount,
      total,
      passRate: passRate.toFixed(3),
      blockReasons: Object.fromEntries(this.#blockReasons)
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.#blockedCount = 0;
    this.#passedCount = 0;
    this.#blockReasons.clear();
  }

  /**
   * Get current configuration
   * @returns {Object}
   */
  getConfig() {
    return { ...this.#config };
  }

  /**
   * Update configuration at runtime
   * @param {Object} newConfig
   */
  updateConfig(newConfig) {
    this.#config = { ...this.#config, ...newConfig };
  }
}

export default SignalGate;
