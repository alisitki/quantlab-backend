import { promisify } from 'util';
const sleep = promisify(setTimeout);

/**
 * QuantLab Replay Engine — Realtime Clock
 * 
 * Replays events with real-time delays based on ts_event differences.
 * Use for live simulation and real-time visualization.
 * 
 * Note: ts_event is in nanoseconds, delays are in milliseconds.
 * 
 * @implements {Clock}
 */
class RealtimeClock {
  /** @type {bigint|number|null} */
  #prevTs = null;
  /** @type {boolean} */
  #requireBigInt = true;

  constructor() {
    this.#prevTs = null;
    this.#requireBigInt = true;
  }

  /**
   * Initialize with first event timestamp.
   * @param {bigint|number} firstTs
   */
  init(firstTs) {
    if (this.#requireBigInt && typeof firstTs !== 'bigint') {
      throw new Error('CLOCK_TS_EVENT_NOT_BIGINT: RealtimeClock requires BigInt ts_event');
    }
    this.#prevTs = BigInt(firstTs);
  }

  /**
   * Wait based on timestamp difference from previous event.
   * @param {bigint|number} ts_event - Current event timestamp (nanoseconds)
   * @returns {Promise<void>}
   */
  async wait(ts_event) {
    if (this.#requireBigInt && typeof ts_event !== 'bigint') {
      throw new Error('CLOCK_TS_EVENT_NOT_BIGINT: RealtimeClock requires BigInt ts_event');
    }
    const currentTs = BigInt(ts_event);
    
    if (this.#prevTs !== null) {
      // Convert nanoseconds to milliseconds
      const deltaNs = currentTs - this.#prevTs;
      const deltaMs = Number(deltaNs / 1_000_000n);
      
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

export default RealtimeClock;
