/**
 * QuantLab Baseline Strategy v1 — Decision Engine
 * 
 * Isolated, rule-based decision logic.
 * Designed to be replaced by ML model later.
 * 
 * Rules:
 * 1. If return_1 > threshold AND no position → LONG
 * 2. If return_1 < -threshold AND no position → SHORT
 * 3. If position open AND opposite signal → EXIT
 * 4. Otherwise → FLAT (no action)
 */

/**
 * @typedef {'LONG' | 'SHORT' | 'FLAT' | 'EXIT_LONG' | 'EXIT_SHORT'} Action
 */

/**
 * @typedef {Object} Decision
 * @property {Action} action - Recommended action
 * @property {number} [confidence] - Optional confidence score (0-1)
 * @property {string} [reason] - Optional reason for decision
 */

/**
 * @typedef {Object} DecisionInput
 * @property {number} mid_price - Current mid price
 * @property {number} spread - Current bid-ask spread
 * @property {number} return_1 - One-tick return
 * @property {'LONG' | 'SHORT' | 'FLAT'} position - Current position state
 * @property {import('./config.js').BaselineConfig} config - Strategy config
 */

/**
 * Make trading decision based on features and current position
 * 
 * @param {DecisionInput} input
 * @returns {Decision}
 */
export function decide(input) {
  const { mid_price, spread, return_1, position, config } = input;
  
  // Spread filter: skip if spread too wide
  const spreadBps = (spread / mid_price) * 10000;
  if (spreadBps > config.spreadMaxBps) {
    return {
      action: 'FLAT',
      reason: `spread_too_wide (${spreadBps.toFixed(1)} bps > ${config.spreadMaxBps})`
    };
  }
  
  const threshold = config.momentumThreshold;
  
  // Position management first
  if (position === 'LONG') {
    // Check for exit signal (opposite momentum)
    if (return_1 < -threshold) {
      return {
        action: 'EXIT_LONG',
        confidence: Math.min(1, Math.abs(return_1) / threshold),
        reason: 'momentum_reversal'
      };
    }
    // Hold current position
    return {
      action: 'FLAT',
      reason: 'holding_long'
    };
  }
  
  if (position === 'SHORT') {
    // Check for exit signal (opposite momentum)
    if (return_1 > threshold) {
      return {
        action: 'EXIT_SHORT',
        confidence: Math.min(1, Math.abs(return_1) / threshold),
        reason: 'momentum_reversal'
      };
    }
    // Hold current position
    return {
      action: 'FLAT',
      reason: 'holding_short'
    };
  }
  
  // No position — look for entry signals
  if (return_1 > threshold) {
    return {
      action: 'LONG',
      confidence: Math.min(1, return_1 / threshold),
      reason: 'positive_momentum'
    };
  }
  
  if (return_1 < -threshold) {
    return {
      action: 'SHORT',
      confidence: Math.min(1, Math.abs(return_1) / threshold),
      reason: 'negative_momentum'
    };
  }
  
  // No signal
  return {
    action: 'FLAT',
    reason: 'no_signal'
  };
}
