/**
 * ReturnMomentumFeature: Measure directional consistency in returns
 *
 * Tracks whether returns are persistently positive (uptrend) or negative (downtrend).
 * High momentum = consistent directional movement
 * Low momentum = choppy/mean-reverting movement
 *
 * Hypothesis: High momentum predicts trend continuation.
 *
 * Algorithm:
 * 1. Track last N returns
 * 2. Count positive vs negative returns
 * 3. Weight by magnitude
 * 4. Momentum = (positive_weight - negative_weight) / total_weight
 *
 * Range: [-1, +1]
 *   +1 = all returns positive (strong upward momentum)
 *   -1 = all returns negative (strong downward momentum)
 *    0 = balanced (no momentum)
 */
export class ReturnMomentumFeature {
  #window;
  #returns = [];
  #prevMid = null;
  #weightByMagnitude;

  constructor(config = {}) {
    this.#window = config.window || 20;
    this.#weightByMagnitude = config.weightByMagnitude !== false; // Default true
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

    // Add to sliding window
    this.#returns.push(ret);
    if (this.#returns.length > this.#window) {
      this.#returns.shift();
    }

    // Warmup: need full window
    if (this.#returns.length < this.#window) return null;

    // Calculate momentum
    return this.#calculateMomentum();
  }

  #calculateMomentum() {
    if (this.#weightByMagnitude) {
      // Weighted momentum: larger moves have more weight
      let positiveWeight = 0;
      let negativeWeight = 0;

      for (const ret of this.#returns) {
        const magnitude = Math.abs(ret);
        if (ret > 0) {
          positiveWeight += magnitude;
        } else if (ret < 0) {
          negativeWeight += magnitude;
        }
      }

      const totalWeight = positiveWeight + negativeWeight;
      if (totalWeight === 0) return 0;

      return (positiveWeight - negativeWeight) / totalWeight;
    } else {
      // Simple count: just count direction
      let positiveCount = 0;
      let negativeCount = 0;

      for (const ret of this.#returns) {
        if (ret > 0) positiveCount++;
        else if (ret < 0) negativeCount++;
      }

      const totalCount = positiveCount + negativeCount;
      if (totalCount === 0) return 0;

      return (positiveCount - negativeCount) / totalCount;
    }
  }

  reset() {
    this.#returns = [];
    this.#prevMid = null;
  }
}
