/**
 * ATRFeature: Average True Range (BBO adapted)
 *
 * BBO Adaptation: tick_tr = max(spread, |mid - prev_mid|)
 * ATR = SMA(tick_tr, period)
 */
export class ATRFeature {
  #period;
  #prevMid = null;
  #trValues = [];

  constructor(config = {}) {
    this.#period = config.period || 14;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;
    const spread = ask - bid;

    if (this.#prevMid !== null) {
      const midChange = Math.abs(mid - this.#prevMid);
      // Tick True Range: max of spread or price change
      const tickTR = Math.max(spread, midChange);

      this.#trValues.push(tickTR);
      if (this.#trValues.length > this.#period) {
        this.#trValues.shift();
      }
    }

    this.#prevMid = mid;

    // Warm-up: need period values
    if (this.#trValues.length < this.#period) return null;

    // Return average true range
    return this.#trValues.reduce((a, b) => a + b, 0) / this.#period;
  }

  reset() {
    this.#prevMid = null;
    this.#trValues = [];
  }
}
