/**
 * RegimeModeSelector - Regime-based Strategy Mode Selection
 *
 * Trade'i ENGELLEMEZ, strateji MODUNU değiştirir.
 * Her regime için farklı strateji davranışı tanımlar.
 */

/**
 * Regime labels for readability
 */
export const REGIME_LABELS = {
  volatility: { 0: 'low', 1: 'normal', 2: 'high' },
  trend: { '-1': 'down', 0: 'side', 1: 'up' },
  spread: { 0: 'tight', 1: 'normal', 2: 'wide' }
};

/**
 * Default mode configurations
 */
const DEFAULT_MODES = {
  // Volatility-based modes
  vol_high: {
    name: 'MEAN_REVERSION',
    description: 'High vol = price overreacts, fade moves',
    signalBias: 'contrarian',
    positionScale: 0.5,          // Küçük pozisyon
    holdDuration: 'short',       // Hızlı çıkış
    thresholdMultiplier: 1.5     // Daha geniş threshold
  },
  vol_low: {
    name: 'MOMENTUM',
    description: 'Low vol = trends persist',
    signalBias: 'trend_following',
    positionScale: 1.0,
    holdDuration: 'medium',
    thresholdMultiplier: 0.8
  },
  vol_normal: {
    name: 'BALANCED',
    description: 'Normal vol = mixed signals',
    signalBias: 'neutral',
    positionScale: 0.75,
    holdDuration: 'medium',
    thresholdMultiplier: 1.0
  },

  // Trend-based adjustments
  trend_up: {
    signalBias: 'long_preferred',
    shortPenalty: 0.5            // Short sinyalleri cezalandır
  },
  trend_down: {
    signalBias: 'short_preferred',
    longPenalty: 0.5             // Long sinyalleri cezalandır
  },
  trend_side: {
    name: 'RANGE_BOUND',
    signalBias: 'mean_reversion',
    breakoutPenalty: 0.7         // Breakout sinyalleri cezalandır
  },

  // Spread-based adjustments
  spread_wide: {
    executionDelay: true,        // Spread daralmasını bekle
    minSpreadImprovement: 0.2
  },
  spread_normal: {
    executionDelay: false
  },
  spread_tight: {
    executionDelay: false,
    preferExecution: true        // Sıkı spread = hızlı execution
  }
};

export class RegimeModeSelector {
  #modes;
  #lastMode = null;

  /**
   * @param {Object} config - Custom mode configurations
   */
  constructor(config = {}) {
    // Merge default modes with custom config
    this.#modes = { ...DEFAULT_MODES };

    if (config.modes) {
      for (const [key, overrides] of Object.entries(config.modes)) {
        if (this.#modes[key]) {
          this.#modes[key] = { ...this.#modes[key], ...overrides };
        }
      }
    }
  }

  /**
   * Get volatility label from numeric value
   * @param {number} v - 0, 1, or 2
   * @returns {string} 'low', 'normal', or 'high'
   */
  getVolLabel(v) {
    return REGIME_LABELS.volatility[v] || 'normal';
  }

  /**
   * Get trend label from numeric value
   * @param {number} t - -1, 0, or 1
   * @returns {string} 'down', 'side', or 'up'
   */
  getTrendLabel(t) {
    return REGIME_LABELS.trend[String(t)] || 'side';
  }

  /**
   * Get spread label from numeric value
   * @param {number} s - 0, 1, or 2
   * @returns {string} 'tight', 'normal', or 'wide'
   */
  getSpreadLabel(s) {
    return REGIME_LABELS.spread[s] || 'normal';
  }

  /**
   * Combine signal biases from volatility and trend modes
   * @param {Object} volMode
   * @param {Object} trendMode
   * @returns {string}
   */
  combineSignalBias(volMode, trendMode) {
    // Mean reversion in high vol takes priority
    if (volMode.signalBias === 'contrarian') {
      return 'contrarian';
    }

    // Trend following modes
    if (volMode.signalBias === 'trend_following') {
      if (trendMode.signalBias === 'long_preferred') return 'long_momentum';
      if (trendMode.signalBias === 'short_preferred') return 'short_momentum';
      return 'trend_following';
    }

    // Balanced mode uses trend bias
    return trendMode.signalBias || 'neutral';
  }

  /**
   * Select strategy mode based on current regime
   * @param {Object} regimes - { volatility: 0|1|2, trend: -1|0|1, spread: 0|1|2 }
   * @returns {Object} Mode configuration
   */
  selectMode(regimes) {
    const { volatility = 1, trend = 0, spread = 1 } = regimes;

    // Get mode configurations
    const volMode = this.#modes[`vol_${this.getVolLabel(volatility)}`] || this.#modes.vol_normal;
    const trendMode = this.#modes[`trend_${this.getTrendLabel(trend)}`] || this.#modes.trend_side;
    const spreadMode = this.#modes[`spread_${this.getSpreadLabel(spread)}`] || this.#modes.spread_normal;

    // Build combined mode
    const mode = {
      primary: volMode.name,
      volatility: { ...volMode },
      trend: { ...trendMode },
      spread: { ...spreadMode },

      // Labels for logging
      labels: {
        volatility: this.getVolLabel(volatility),
        trend: this.getTrendLabel(trend),
        spread: this.getSpreadLabel(spread)
      },

      // Combined parameters for easy access
      combined: {
        positionScale: volMode.positionScale,
        thresholdMultiplier: volMode.thresholdMultiplier,
        holdDuration: volMode.holdDuration,
        signalBias: this.combineSignalBias(volMode, trendMode),
        executionDelay: spreadMode.executionDelay || false,
        longPenalty: trendMode.longPenalty || 1.0,
        shortPenalty: trendMode.shortPenalty || 1.0,
        breakoutPenalty: trendMode.breakoutPenalty || 1.0
      }
    };

    // Track mode transitions
    const modeChanged = this.#lastMode !== mode.primary;
    this.#lastMode = mode.primary;

    return {
      ...mode,
      modeChanged,
      previousMode: modeChanged ? this.#lastMode : null
    };
  }

  /**
   * Get current mode name
   * @returns {string|null}
   */
  getCurrentMode() {
    return this.#lastMode;
  }

  /**
   * Get all available modes
   * @returns {Object}
   */
  getModes() {
    return { ...this.#modes };
  }

  /**
   * Check if mode transition occurred
   * @param {string} fromMode
   * @param {string} toMode
   * @returns {Object} Transition info
   */
  describeTransition(fromMode, toMode) {
    if (fromMode === toMode) {
      return { transitioned: false };
    }

    const transitions = {
      'MOMENTUM->MEAN_REVERSION': 'Volatility spike detected, switching to fade mode',
      'MEAN_REVERSION->MOMENTUM': 'Volatility normalized, switching to trend mode',
      'BALANCED->MOMENTUM': 'Clear trend forming, switching to momentum mode',
      'BALANCED->MEAN_REVERSION': 'High volatility detected, switching to fade mode',
      'MOMENTUM->BALANCED': 'Trend weakening, switching to balanced mode',
      'MEAN_REVERSION->BALANCED': 'Volatility normalizing, switching to balanced mode'
    };

    const key = `${fromMode}->${toMode}`;

    return {
      transitioned: true,
      from: fromMode,
      to: toMode,
      description: transitions[key] || `Mode changed from ${fromMode} to ${toMode}`
    };
  }

  /**
   * Reset mode tracking
   */
  reset() {
    this.#lastMode = null;
  }
}

export default RegimeModeSelector;
