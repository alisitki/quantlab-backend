/**
 * QuantLab Risk Management v1 — Configuration
 * 
 * Default parameters for risk rules.
 * Deterministic — no randomness, no wall-clock.
 */

/**
 * @typedef {Object} RiskConfig
 * @property {number} maxPositions - Max concurrent positions (1 = one at a time)
 * @property {number} cooldownEvents - Events to skip after a fill
 * @property {number} maxDailyLossPct - Max daily loss as % of initial capital (0.02 = 2%)
 * @property {number} stopLossPct - Stop loss as % of entry price (0.005 = 0.5%)
 * @property {number} takeProfitPct - Take profit as % of entry price (0.01 = 1%)
 * @property {boolean} enabled - Master switch for risk management
 */

/** @type {RiskConfig} */
export const DEFAULT_RISK_CONFIG = {
  maxPositions: 1,
  cooldownEvents: 50,
  maxDailyLossPct: 0.02,    // 2% max daily loss
  stopLossPct: 0.005,        // 0.5% stop loss
  takeProfitPct: 0.01,       // 1% take profit
  enabled: true
};

/**
 * Merge user config with defaults
 * @param {Partial<RiskConfig>} [userConfig={}]
 * @returns {RiskConfig}
 */
export function createRiskConfig(userConfig = {}) {
  return {
    ...DEFAULT_RISK_CONFIG,
    ...userConfig
  };
}
