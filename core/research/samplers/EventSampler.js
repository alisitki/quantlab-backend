/**
 * EventSampler filters every N-th event.
 */
export class EventSampler {
  #n;
  #count = 0;

  /**
   * @param {Object} config
   * @param {number} config.n - Forward every n-th event
   */
  constructor({ n }) {
    this.#n = n;
  }

  /**
   * @param {Object} event
   * @returns {boolean} True if event should be forwarded
   */
  shouldProcess(event) {
    this.#count++;
    if (this.#count % this.#n === 0) {
      return true;
    }
    return false;
  }

  reset() {
    this.#count = 0;
  }
}
