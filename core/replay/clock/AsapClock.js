/**
 * QuantLab Replay Engine — ASAP Clock
 * 
 * Replays events as fast as possible with no delays.
 * Use for backtesting, feature generation, and batch processing.
 * 
 * @implements {Clock}
 */
class AsapClock {
  /** @type {bigint|number|null} */
  #firstTs = null;

  /**
   * Initialize with first event timestamp.
   * @param {bigint|number} firstTs
   */
  init(firstTs) {
    this.#firstTs = firstTs;
  }

  /**
   * Wait before next event (no-op for ASAP).
   * @param {bigint|number} ts_event
   * @returns {Promise<void>}
   */
  async wait(ts_event) {
    // Resolve immediately - no delay
    return Promise.resolve();
  }

  /**
   * Called when replay ends.
   */
  onEnd() {
    // No cleanup needed
  }
}

export default AsapClock;
