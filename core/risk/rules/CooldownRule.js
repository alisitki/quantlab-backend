/**
 * QuantLab Risk Management v1 — Cooldown Rule
 * 
 * Blocks new entries for N events after a fill.
 * Event-based (not time-based) for determinism.
 */

/**
 * Cooldown Rule
 */
export class CooldownRule {
  /** @type {number} */
  #cooldownEvents;
  
  /** @type {number} */
  #eventsSinceFill = Infinity; // Start with no cooldown
  
  /** @type {number} */
  #lastFillCount = 0;

  /**
   * @param {number} [cooldownEvents=50]
   */
  constructor(cooldownEvents = 50) {
    this.#cooldownEvents = cooldownEvents;
  }

  /**
   * Update cooldown state on each event
   * Call this BEFORE check() on each event
   * @param {Object} ctx - Runner context
   */
  onEvent(ctx) {
    if (!ctx.execution) return;

    const state = ctx.execution.snapshot();
    const currentFillCount = state.fills.length;

    // Detect new fill
    if (currentFillCount > this.#lastFillCount) {
      this.#eventsSinceFill = 0;
      this.#lastFillCount = currentFillCount;
    } else {
      this.#eventsSinceFill++;
    }
  }

  /**
   * Check if signal is allowed
   * @param {Object} signal - Trading signal
   * @param {Object} ctx - Runner context
   * @returns {import('./MaxPositionRule.js').RuleResult}
   */
  check(signal, ctx) {
    // Always allow exits and flat
    if (signal.action === 'FLAT' || 
        signal.action === 'EXIT_LONG' || 
        signal.action === 'EXIT_SHORT') {
      return { allowed: true };
    }

    if (this.#eventsSinceFill < this.#cooldownEvents) {
      return {
        allowed: false,
        reason: `cooldown_active (${this.#eventsSinceFill}/${this.#cooldownEvents} events)`
      };
    }

    return { allowed: true };
  }

  /**
   * Reset state (for new runs)
   */
  reset() {
    this.#eventsSinceFill = Infinity;
    this.#lastFillCount = 0;
  }
}
