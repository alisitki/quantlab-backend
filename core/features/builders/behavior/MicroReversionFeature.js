/**
 * MicroReversionFeature: Measure mean-reversion tendency at micro level
 *
 * Tracks how often price returns reverse direction vs continue.
 * High reversion = mean-reverting market (oscillating)
 * Low reversion = trending market (directional persistence)
 *
 * Hypothesis: High reversion score indicates mean-reversion edge is active.
 *
 * Algorithm:
 * 1. Track (return_t, return_t+1) pairs
 * 2. Count reversals (sign flips) vs continuations
 * 3. reversion_score = reversals / total over rolling window
 *
 * Range: [0, 1]
 *   0.5 = random (50% reversals, no edge)
 *   >0.5 = mean-reverting (edges work)
 *   <0.5 = trending (momentum edges work)
 */
export class MicroReversionFeature {
  #window;
  #returns = [];
  #reversions = []; // 1 = reversion, 0 = continuation
  #prevMid = null;
  #prevReturn = null;

  constructor(config = {}) {
    this.#window = config.window || 50;
  }

  onEvent(event) {
    const mid = (Number(event.bid_price) + Number(event.ask_price)) / 2;

    // Need previous mid to calculate return
    if (this.#prevMid === null) {
      this.#prevMid = mid;
      return null; // Warmup
    }

    // Calculate return
    const ret = (mid - this.#prevMid) / this.#prevMid;
    this.#prevMid = mid;

    // Need previous return to detect reversion
    if (this.#prevReturn === null) {
      this.#prevReturn = ret;
      return null; // Warmup
    }

    // Detect reversion (sign flip) or continuation (same sign)
    // Also count zero returns as neutral (0.5 reversion score)
    let reversionValue;
    if (ret === 0 || this.#prevReturn === 0) {
      reversionValue = 0.5; // Neutral - no clear direction
    } else {
      reversionValue = (ret * this.#prevReturn) < 0 ? 1 : 0; // 1=reversion, 0=continuation
    }

    this.#reversions.push(reversionValue);

    // Trim to window
    if (this.#reversions.length > this.#window) {
      this.#reversions.shift();
    }

    this.#prevReturn = ret;

    // Warmup: need full window
    if (this.#reversions.length < this.#window) return null;

    // Calculate reversion score
    const totalReversions = this.#reversions.reduce((sum, val) => sum + val, 0);
    return totalReversions / this.#reversions.length;
  }

  reset() {
    this.#returns = [];
    this.#reversions = [];
    this.#prevMid = null;
    this.#prevReturn = null;
  }
}
