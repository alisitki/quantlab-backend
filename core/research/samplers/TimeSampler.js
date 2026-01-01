/**
 * TimeSampler filters events based on a time interval.
 * Only forward events if: ts_event - last_emitted_ts >= intervalMs
 */
export class TimeSampler {
  #intervalMs;
  #lastEmittedTs = -1;

  /**
   * @param {Object} config
   * @param {number} config.intervalMs - Minimum milliseconds between events
   */
  constructor({ intervalMs }) {
    this.#intervalMs = intervalMs;
  }

  /**
   * @param {Object} event
   * @returns {boolean} True if event should be forwarded
   */
  shouldProcess(event) {
    if (this.#lastEmittedTs === -1 || event.ts_event - this.#lastEmittedTs >= this.#intervalMs) {
      this.#lastEmittedTs = event.ts_event;
      return true;
    }
    return false;
  }

  reset() {
    this.#lastEmittedTs = -1;
  }
}
