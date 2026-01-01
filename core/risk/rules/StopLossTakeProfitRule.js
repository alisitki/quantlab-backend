/**
 * QuantLab Risk Management v1 — Stop Loss / Take Profit Rule
 * 
 * Monitors positions and triggers forced exits when SL/TP hit.
 * Evaluated on every event.
 * Deterministic — same price = same decision.
 */

/**
 * @typedef {Object} ForceExitOrder
 * @property {string} symbol
 * @property {string} side - 'BUY' or 'SELL'
 * @property {number} qty
 * @property {bigint} ts_event
 * @property {string} reason - 'stop_loss' or 'take_profit'
 */

/**
 * Stop Loss / Take Profit Rule
 */
export class StopLossTakeProfitRule {
  /** @type {number} */
  #stopLossPct;
  
  /** @type {number} */
  #takeProfitPct;

  /**
   * @param {number} [stopLossPct=0.005] - SL as fraction (0.005 = 0.5%)
   * @param {number} [takeProfitPct=0.01] - TP as fraction (0.01 = 1%)
   */
  constructor(stopLossPct = 0.005, takeProfitPct = 0.01) {
    this.#stopLossPct = stopLossPct;
    this.#takeProfitPct = takeProfitPct;
  }

  /**
   * Check positions for SL/TP triggers
   * Returns a forced exit order if any position hits SL/TP
   * @param {Object} event
   * @param {Object} ctx
   * @returns {ForceExitOrder|null}
   */
  checkForExit(event, ctx) {
    if (!ctx.execution) return null;

    const state = ctx.execution.snapshot();
    const ts_event = event.ts_event;

    for (const [symbol, pos] of Object.entries(state.positions)) {
      if (pos.size === 0 || pos.avgEntryPrice === 0) continue;

      const entryPrice = pos.avgEntryPrice;
      const currentPrice = pos.currentPrice;
      
      if (currentPrice === 0) continue;

      // Calculate return vs entry
      let returnPct;
      if (pos.size > 0) {
        // Long: profit if price went up
        returnPct = (currentPrice - entryPrice) / entryPrice;
      } else {
        // Short: profit if price went down
        returnPct = (entryPrice - currentPrice) / entryPrice;
      }

      // Check take profit
      if (returnPct >= this.#takeProfitPct) {
        return {
          symbol,
          side: pos.size > 0 ? 'SELL' : 'BUY',
          qty: Math.abs(pos.size),
          ts_event,
          reason: `take_profit (${(returnPct * 100).toFixed(2)}% >= ${(this.#takeProfitPct * 100).toFixed(2)}%)`
        };
      }

      // Check stop loss
      if (returnPct <= -this.#stopLossPct) {
        return {
          symbol,
          side: pos.size > 0 ? 'SELL' : 'BUY',
          qty: Math.abs(pos.size),
          ts_event,
          reason: `stop_loss (${(returnPct * 100).toFixed(2)}% <= -${(this.#stopLossPct * 100).toFixed(2)}%)`
        };
      }
    }

    return null;
  }

  /**
   * This rule doesn't block entries, it forces exits
   * Always returns allowed for check()
   */
  check(signal, ctx) {
    return { allowed: true };
  }
}
