/**
 * QuantLab Baseline Strategy v1 — Configuration
 * 
 * Default parameters for the baseline rule-based strategy.
 * Designed to be overridable at runtime.
 */

/**
 * @typedef {Object} BaselineConfig
 * @property {string} symbol - Target trading symbol
 * @property {number} orderQty - Order quantity per trade
 * @property {number} cooldownEvents - Events to wait after a trade
 * @property {number} momentumThreshold - Min return_1 to trigger signal (abs value)
 * @property {number} spreadMaxBps - Max spread in basis points to consider trading
 */

/** @type {BaselineConfig} */
export const DEFAULT_CONFIG = {
  symbol: 'btcusdt',
  orderQty: 0.01,
  cooldownEvents: 50,
  momentumThreshold: 0.0001,  // 0.01% price move
  spreadMaxBps: 10            // 10 bps = 0.1%
};

/**
 * Merge user config with defaults
 * @param {Partial<BaselineConfig>} [userConfig={}]
 * @returns {BaselineConfig}
 */
export function createConfig(userConfig = {}) {
  return {
    ...DEFAULT_CONFIG,
    ...userConfig
  };
}
