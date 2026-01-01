/**
 * @implements {Clock}
 */
class AsapClock {
  /**
   * @param {Object} prevEvent
   * @param {Object} nextEvent
   * @returns {Promise<void>}
   */
  async beforeNext(prevEvent, nextEvent) {
    // Resolve immediately
    return Promise.resolve();
  }
}

export default AsapClock;
