/**
 * ImbalanceEMAFeature: Smoothed order book imbalance
 *
 * Formula: imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)
 * Smoothed with EMA to filter spikes
 */
export class ImbalanceEMAFeature {
  #period;
  #alpha;
  #prevEma = null;
  #count = 0;

  constructor(config = {}) {
    this.#period = config.period || 20;
    this.#alpha = 2 / (this.#period + 1);
  }

  onEvent(event) {
    const bidQty = Number(event.bid_qty ?? event.bid_size ?? 0);
    const askQty = Number(event.ask_qty ?? event.ask_size ?? 0);

    const totalQty = bidQty + askQty;
    if (totalQty === 0) return null;

    // Raw imbalance: -1 to +1
    const imbalance = (bidQty - askQty) / totalQty;
    this.#count++;

    if (this.#prevEma === null) {
      this.#prevEma = imbalance;
      return null;
    }

    const ema = this.#alpha * imbalance + (1 - this.#alpha) * this.#prevEma;
    this.#prevEma = ema;

    // Warm-up: need period events
    if (this.#count < this.#period) return null;

    return ema;
  }

  reset() {
    this.#prevEma = null;
    this.#count = 0;
  }
}
