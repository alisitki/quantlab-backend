/**
 * QuantLab Risk Management v1 — Max Daily Loss Rule
 * 
 * Blocks ALL entries if daily realized loss exceeds threshold.
 * Day boundary based on ts_event UTC day.
 * Deterministic — no wall-clock usage.
 */

/**
 * Max Daily Loss Rule
 */
export class MaxDailyLossRule {
  /** @type {number} */
  #maxDailyLossPct;
  
  /** @type {number} */
  #initialCapital;
  
  /** @type {string|null} */
  #currentDay = null;
  
  /** @type {number} */
  #dayStartPnl = 0;
  
  /** @type {boolean} */
  #locked = false;

  /**
   * @param {number} [maxDailyLossPct=0.02] - Max loss as fraction (0.02 = 2%)
   * @param {number} [initialCapital=10000]
   */
  constructor(maxDailyLossPct = 0.02, initialCapital = 10000) {
    this.#maxDailyLossPct = maxDailyLossPct;
    this.#initialCapital = initialCapital;
  }

  /**
   * Extract UTC day from ts_event (nanoseconds)
   * @param {bigint} ts_event
   * @returns {string} YYYY-MM-DD
   */
  #getUtcDay(ts_event) {
    // ts_event is in nanoseconds
    const ms = Number(ts_event / 1000000n);
    const date = new Date(ms);
    return date.toISOString().split('T')[0];
  }

  /**
   * Update state on each event
   * @param {Object} event
   * @param {Object} ctx
   */
  onEvent(event, ctx) {
    if (!ctx.execution || !event.ts_event) return;

    const day = this.#getUtcDay(event.ts_event);
    const state = ctx.execution.snapshot();
    const currentPnl = state.totalRealizedPnl;

    // New day — reset
    if (day !== this.#currentDay) {
      this.#currentDay = day;
      this.#dayStartPnl = currentPnl;
      this.#locked = false;
    }

    // Check daily loss
    const dayPnl = currentPnl - this.#dayStartPnl;
    const maxLoss = -this.#maxDailyLossPct * this.#initialCapital;

    if (dayPnl < maxLoss) {
      this.#locked = true;
    }
  }

  /**
   * Check if signal is allowed
   * @param {Object} signal
   * @param {Object} ctx
   * @returns {import('./MaxPositionRule.js').RuleResult}
   */
  check(signal, ctx) {
    // Always allow exits
    if (signal.action === 'FLAT' || 
        signal.action === 'EXIT_LONG' || 
        signal.action === 'EXIT_SHORT') {
      return { allowed: true };
    }

    if (this.#locked) {
      return {
        allowed: false,
        reason: `max_daily_loss_exceeded (locked for ${this.#currentDay})`
      };
    }

    return { allowed: true };
  }

  /**
   * Check if currently locked
   * @returns {boolean}
   */
  isLocked() {
    return this.#locked;
  }

  /**
   * Reset state
   */
  reset() {
    this.#currentDay = null;
    this.#dayStartPnl = 0;
    this.#locked = false;
  }
}
