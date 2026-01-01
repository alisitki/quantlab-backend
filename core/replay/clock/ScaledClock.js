import { promisify } from 'util';
const sleep = promisify(setTimeout);

/**
 * @implements {Clock}
 */
class ScaledClock {
  /**
   * @param {Object} options
   * @param {number} options.speed
   */
  constructor({ speed }) {
    this.speed = speed || 1.0;
  }

  /**
   * @param {Object} prevEvent
   * @param {Object} nextEvent
   * @returns {Promise<void>}
   */
  async beforeNext(prevEvent, nextEvent) {
    const delta = Number(nextEvent.ts_event - prevEvent.ts_event) / this.speed;
    if (delta > 0) {
      await sleep(delta);
    }
  }
}

export default ScaledClock;
