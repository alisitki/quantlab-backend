/**
 * ROCFeature: Rate of Change
 *
 * Formula: ROC = ((mid - mid[n]) / mid[n]) * 100
 * Shows percentage change over n periods
 */
export class ROCFeature {
  #period;
  #midBuffer = [];

  constructor(config = {}) {
    this.#period = config.period || 10;
  }

  onEvent(event) {
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);

    if (isNaN(bid) || isNaN(ask)) return null;

    const mid = (bid + ask) / 2;

    this.#midBuffer.push(mid);

    if (this.#midBuffer.length > this.#period + 1) {
      this.#midBuffer.shift();
    }

    // Warm-up: need period + 1 values (current + n periods back)
    if (this.#midBuffer.length <= this.#period) return null;

    const oldMid = this.#midBuffer[0];
    if (oldMid === 0) return null; // Avoid division by zero

    return ((mid - oldMid) / oldMid) * 100;
  }

  reset() {
    this.#midBuffer = [];
  }
}
