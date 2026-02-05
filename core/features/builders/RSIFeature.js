/**
 * RSIFeature: Relative Strength Index (BBO adapted)
 *
 * Uses mid_price changes instead of OHLC close.
 * Formula: RSI = 100 - (100 / (1 + RS))
 * where RS = avg(gains) / avg(losses) over period
 */
export class RSIFeature {
  #period;
  #prevMid = null;
  #gains = [];
  #losses = [];

  constructor(config = {}) {
    this.#period = config.period || 14;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;

    if (this.#prevMid !== null) {
      const change = mid - this.#prevMid;
      const gain = change > 0 ? change : 0;
      const loss = change < 0 ? -change : 0;

      this.#gains.push(gain);
      this.#losses.push(loss);

      if (this.#gains.length > this.#period) {
        this.#gains.shift();
        this.#losses.shift();
      }
    }

    this.#prevMid = mid;

    // Warm-up: need period values
    if (this.#gains.length < this.#period) return null;

    const avgGain = this.#gains.reduce((a, b) => a + b, 0) / this.#period;
    const avgLoss = this.#losses.reduce((a, b) => a + b, 0) / this.#period;

    // Edge cases
    if (avgLoss === 0) return 100; // All gains, no losses
    if (avgGain === 0) return 0;   // All losses, no gains

    const rs = avgGain / avgLoss;
    return 100 - (100 / (1 + rs));
  }

  reset() {
    this.#prevMid = null;
    this.#gains = [];
    this.#losses = [];
  }
}
