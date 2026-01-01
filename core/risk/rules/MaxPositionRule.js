/**
 * QuantLab Risk Management v1 — Max Position Rule
 * 
 * Ensures only one position is open at a time.
 * Rejects new entries if position already exists.
 * Always allows exits.
 */

/**
 * @typedef {Object} RuleResult
 * @property {boolean} allowed - Whether the signal is allowed
 * @property {string} [reason] - Reason for rejection
 */

/**
 * Max Position Rule
 */
export class MaxPositionRule {
  /**
   * @param {number} [maxPositions=1]
   */
  constructor(maxPositions = 1) {
    this.maxPositions = maxPositions;
  }

  /**
   * Check if signal is allowed
   * @param {Object} signal - Trading signal
   * @param {string} signal.action - 'LONG', 'SHORT', 'EXIT_LONG', 'EXIT_SHORT', 'FLAT'
   * @param {Object} ctx - Runner context with execution state
   * @returns {RuleResult}
   */
  check(signal, ctx) {
    // Always allow exits and flat
    if (signal.action === 'FLAT' || 
        signal.action === 'EXIT_LONG' || 
        signal.action === 'EXIT_SHORT') {
      return { allowed: true };
    }

    // Check current position count
    if (!ctx.execution) {
      return { allowed: true }; // No execution engine = allow
    }

    const state = ctx.execution.snapshot();
    let openPositions = 0;

    for (const [symbol, pos] of Object.entries(state.positions)) {
      if (pos.size !== 0) {
        openPositions++;
      }
    }

    if (openPositions >= this.maxPositions) {
      return {
        allowed: false,
        reason: `max_position_reached (${openPositions}/${this.maxPositions})`
      };
    }

    return { allowed: true };
  }
}
