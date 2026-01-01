import { promisify } from 'util';
const sleep = promisify(setTimeout);

/**
 * @implements {Clock}
 */
class RealtimeClock {
  /**
   * @param {Object} prevEvent
   * @param {Object} nextEvent
   * @returns {Promise<void>}
   */
  async beforeNext(prevEvent, nextEvent) {
    const delta = Number(nextEvent.ts_event - prevEvent.ts_event);
    if (delta > 0) {
      await sleep(delta);
    }
  }
}

export default RealtimeClock;
