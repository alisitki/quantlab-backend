/**
 * RegimeLogger - Regime state tracking and logging
 *
 * Tracks regime changes over time and logs transitions
 * for analysis and strategy development.
 */

import fs from 'fs';
import path from 'path';

/**
 * Regime state definitions
 */
export const REGIME_STATES = {
  volatility: { 0: 'LOW', 1: 'NORMAL', 2: 'HIGH' },
  trend: { '-1': 'DOWNTREND', 0: 'SIDEWAYS', 1: 'UPTREND' },
  spread: { 0: 'TIGHT', 1: 'NORMAL', 2: 'WIDE' }
};

/**
 * RegimeLogger singleton class
 */
class RegimeLoggerClass {
  #logPath = null;
  #buffer = [];
  #bufferSize = 50;
  #enabled = false;
  #stream = null;
  #lastState = {}; // Per-symbol last known state

  /**
   * Initialize the regime logger
   * @param {Object} config
   */
  init(config = {}) {
    const {
      logPath = process.env.REGIME_LOG_PATH || 'logs/regimes.jsonl',
      bufferSize = 50,
      enabled = process.env.REGIME_LOGGING_ENABLED !== '0'
    } = config;

    this.#logPath = logPath;
    this.#bufferSize = bufferSize;
    this.#enabled = enabled;

    if (this.#enabled) {
      // Ensure directory exists
      const dir = path.dirname(this.#logPath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      // Open append stream
      this.#stream = fs.createWriteStream(this.#logPath, { flags: 'a' });
      console.log(`RegimeLogger initialized: ${this.#logPath}`);
    }
  }

  /**
   * Log regime state
   * @param {Object} params
   */
  logRegimeState(params) {
    if (!this.#enabled) return;

    const {
      timestamp = Date.now(),
      symbol,
      regimes // { volatility: 0|1|2, trend: -1|0|1, spread: 0|1|2 }
    } = params;

    // Detect transitions
    const transitions = this.#detectTransitions(symbol, regimes);

    const entry = {
      ts: new Date(timestamp).toISOString(),
      ts_epoch: timestamp,
      symbol,
      regime_volatility: regimes.volatility,
      regime_trend: regimes.trend,
      regime_spread: regimes.spread,
      transitions: transitions.length > 0 ? transitions : undefined
    };

    // Update last state
    this.#lastState[symbol] = { ...regimes };

    this.#buffer.push(JSON.stringify(entry));

    if (this.#buffer.length >= this.#bufferSize) {
      this.flush();
    }

    return transitions;
  }

  /**
   * Detect regime transitions
   * @param {string} symbol
   * @param {Object} currentRegimes
   * @returns {string[]} Transition descriptions
   */
  #detectTransitions(symbol, currentRegimes) {
    const last = this.#lastState[symbol];
    if (!last) return [];

    const transitions = [];

    for (const [regimeType, currentValue] of Object.entries(currentRegimes)) {
      const lastValue = last[regimeType];
      if (lastValue !== undefined && lastValue !== currentValue) {
        const states = REGIME_STATES[regimeType] || {};
        const fromState = states[lastValue] || lastValue;
        const toState = states[currentValue] || currentValue;
        transitions.push(`${regimeType}:${fromState}->${toState}`);
      }
    }

    return transitions;
  }

  /**
   * Get current regime for a symbol
   * @param {string} symbol
   * @returns {Object|null}
   */
  getCurrentRegime(symbol) {
    return this.#lastState[symbol] || null;
  }

  /**
   * Flush buffer to disk
   */
  flush() {
    if (!this.#enabled || this.#buffer.length === 0) return;

    try {
      const content = this.#buffer.join('\n') + '\n';
      this.#stream.write(content);
      this.#buffer = [];
    } catch (err) {
      console.error('RegimeLogger flush error:', err.message);
    }
  }

  /**
   * Get regime history for a symbol
   * @param {string} symbol
   * @param {number} limit
   * @returns {Object[]}
   */
  getRegimeHistory(symbol, limit = 100) {
    if (!fs.existsSync(this.#logPath)) {
      return [];
    }

    try {
      const content = fs.readFileSync(this.#logPath, 'utf8');
      const lines = content.trim().split('\n').filter(l => l);

      const entries = lines
        .map(line => JSON.parse(line))
        .filter(entry => entry.symbol === symbol);

      return entries.slice(-limit);
    } catch (err) {
      console.error('RegimeLogger read error:', err.message);
      return [];
    }
  }

  /**
   * Get regime transition statistics
   * @param {string} symbol
   * @param {number} limit
   * @returns {Object}
   */
  getTransitionStats(symbol, limit = 1000) {
    const history = this.getRegimeHistory(symbol, limit);

    if (history.length === 0) {
      return { count: 0 };
    }

    const stats = {
      count: history.length,
      transitionCount: 0,
      transitionTypes: {},
      regimeDistribution: {
        volatility: { 0: 0, 1: 0, 2: 0 },
        trend: { '-1': 0, 0: 0, 1: 0 },
        spread: { 0: 0, 1: 0, 2: 0 }
      }
    };

    for (const entry of history) {
      // Count regime occurrences
      if (entry.regime_volatility !== undefined) {
        stats.regimeDistribution.volatility[entry.regime_volatility]++;
      }
      if (entry.regime_trend !== undefined) {
        stats.regimeDistribution.trend[entry.regime_trend]++;
      }
      if (entry.regime_spread !== undefined) {
        stats.regimeDistribution.spread[entry.regime_spread]++;
      }

      // Count transitions
      if (entry.transitions && entry.transitions.length > 0) {
        stats.transitionCount += entry.transitions.length;
        for (const t of entry.transitions) {
          stats.transitionTypes[t] = (stats.transitionTypes[t] || 0) + 1;
        }
      }
    }

    return stats;
  }

  /**
   * Get regime duration statistics
   * @param {string} symbol
   * @returns {Object}
   */
  getRegimeDurations(symbol) {
    const history = this.getRegimeHistory(symbol);

    if (history.length < 2) {
      return { insufficient_data: true };
    }

    const durations = {
      volatility: {},
      trend: {},
      spread: {}
    };

    let lastRegimes = {};
    let lastTs = null;

    for (const entry of history) {
      const ts = entry.ts_epoch;

      if (lastTs !== null) {
        const durationMs = ts - lastTs;

        for (const regimeType of ['volatility', 'trend', 'spread']) {
          const regimeValue = lastRegimes[`regime_${regimeType}`];
          if (regimeValue !== undefined) {
            const key = String(regimeValue);
            if (!durations[regimeType][key]) {
              durations[regimeType][key] = [];
            }
            durations[regimeType][key].push(durationMs);
          }
        }
      }

      lastRegimes = entry;
      lastTs = ts;
    }

    // Calculate average durations
    const avgDurations = {};
    for (const [regimeType, values] of Object.entries(durations)) {
      avgDurations[regimeType] = {};
      for (const [regimeValue, durationList] of Object.entries(values)) {
        const stateName = REGIME_STATES[regimeType]?.[regimeValue] || regimeValue;
        avgDurations[regimeType][stateName] = {
          avgDurationMs: durationList.reduce((a, b) => a + b, 0) / durationList.length,
          count: durationList.length
        };
      }
    }

    return avgDurations;
  }

  /**
   * Close the logger
   */
  close() {
    this.flush();
    if (this.#stream) {
      this.#stream.end();
      this.#stream = null;
    }
  }

  /**
   * Check if logging is enabled
   * @returns {boolean}
   */
  isEnabled() {
    return this.#enabled;
  }
}

// Singleton instance
const RegimeLogger = new RegimeLoggerClass();

export { RegimeLogger };
export default RegimeLogger;
