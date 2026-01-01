/**
 * ResearchExecution is a simplified execution layer for research mode.
 * Rules:
 * - Market price = mid_price (or last price if mid not available)
 * - Single position only (can flip)
 * - No partial fills, no spreads, no SL/TP, no risk rules.
 */
export class ResearchExecution {
  #position = 0; // Current position: 1 (long), -1 (short), 0 (flat)
  #entryPrice = 0;
  #totalRealizedPnl = 0;
  #trades = [];
  #lastPrice = 0;

  /**
   * Processes an order intent from the strategy.
   * research execution allow position flips.
   * @param {Object} intent - { side: 'LONG'|'SHORT'|'FLAT', price: number }
   */
  onOrder(intent) {
    const side = intent.side.toUpperCase();
    const price = intent.price || this.#lastPrice;

    if (side === 'LONG') {
      this.#execute(1, price);
    } else if (side === 'SHORT') {
      this.#execute(-1, price);
    } else if (side === 'FLAT') {
      this.#execute(0, price);
    }
  }

  /**
   * Update the engine with a new market event.
   * @param {Object} event
   */
  onEvent(event) {
    // We use mid_price if available, otherwise last_price
    this.#lastPrice = event.mid_price || event.last_price || event.price || 0;
  }

  #execute(targetPosition, price) {
    if (this.#position === targetPosition) return;

    // If we are closing or flipping, realize PnL
    if (this.#position !== 0) {
      const pnl = (price - this.#entryPrice) * this.#position;
      this.#totalRealizedPnl += pnl;

      this.#trades.push({
        exitTs: Date.now(), // Rough approx
        side: this.#position === 1 ? 'LONG' : 'SHORT',
        entryPrice: this.#entryPrice,
        exitPrice: price,
        pnl
      });
    }

    // Update position
    this.#position = targetPosition;
    this.#entryPrice = price;
  }

  snapshot() {
    const unrealizedPnl = this.#position !== 0 ? (this.#lastPrice - this.#entryPrice) * this.#position : 0;
    return {
      position: this.#position,
      totalRealizedPnl: this.#totalRealizedPnl,
      totalPnl: this.#totalRealizedPnl + unrealizedPnl,
      trades: this.#trades,
      tradeCount: this.#trades.length,
      lastPrice: this.#lastPrice
    };
  }
}
