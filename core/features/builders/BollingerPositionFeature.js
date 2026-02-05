/**
 * BollingerPositionFeature: Position within Bollinger Bands
 *
 * Formula: position = (mid - lower) / (upper - lower)
 * where: upper = SMA + k*std, lower = SMA - k*std
 * Output: 0-1 normalized (0.5 = middle, 0 = lower band, 1 = upper band)
 */
export class BollingerPositionFeature {
  #period;
  #k;
  #midBuffer = [];

  constructor(config = {}) {
    this.#period = config.period || 20;
    this.#k = config.k || 2;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;

    this.#midBuffer.push(mid);
    if (this.#midBuffer.length > this.#period) {
      this.#midBuffer.shift();
    }

    // Warm-up: need full period
    if (this.#midBuffer.length < this.#period) return null;

    // Calculate SMA
    const sma = this.#midBuffer.reduce((a, b) => a + b, 0) / this.#period;

    // Calculate standard deviation
    const sqDiffs = this.#midBuffer.map(v => Math.pow(v - sma, 2));
    const std = Math.sqrt(sqDiffs.reduce((a, b) => a + b, 0) / this.#period);

    // Bollinger bands
    const upper = sma + this.#k * std;
    const lower = sma - this.#k * std;
    const bandwidth = upper - lower;

    if (bandwidth === 0) return 0.5; // At middle if no volatility

    // Normalized position (0-1)
    const position = (mid - lower) / bandwidth;

    // Clamp to 0-1 range (can exceed bands)
    return Math.max(0, Math.min(1, position));
  }

  reset() {
    this.#midBuffer = [];
  }
}
