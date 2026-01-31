import { promisify } from 'util';
const sleep = promisify(setTimeout);

/**
 * QuantLab Replay Engine — Scaled Clock
 * 
 * Replays events with scaled delays (faster or slower than realtime).
 * speed > 1: faster replay
 * speed < 1: slower replay
 * speed = 1: realtime
 * 
 * @implements {Clock}
 */
class ScaledClock {
  /** @type {number} */
  #speed;
  /** @type {bigint|number|null} */
  #prevTs = null;

  /**
   * @param {Object} options
   * @param {number} options.speed - Speed multiplier (default: 1.0)
   */
  constructor({ speed = 1.0 } = {}) {
    this.#speed = speed;
  }

  /**
   * Initialize with first event timestamp.
   * @param {bigint|number} firstTs
   */
  init(firstTs) {
    this.#prevTs = BigInt(firstTs);
  }

  /**
   * Wait based on scaled timestamp difference.
   * @param {bigint|number} ts_event - Current event timestamp (nanoseconds)
   * @returns {Promise<void>}
   */
  async wait(ts_event) {
    const currentTs = BigInt(ts_event);
    
    if (this.#prevTs !== null) {
      // Convert nanoseconds to milliseconds, apply speed factor
      const deltaNs = currentTs - this.#prevTs;
      const deltaMs = Number(deltaNs / 1_000_000n) / this.#speed;
      
      if (deltaMs > 0) {
        await sleep(deltaMs);
      }
    }
    
    this.#prevTs = currentTs;
  }

  /**
   * Called when replay ends.
   */
  onEnd() {
    this.#prevTs = null;
  }
}

export default ScaledClock;
