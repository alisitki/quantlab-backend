/**
 * @typedef {import('../types.js').RunnerContext} RunnerContext
 */

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export class SlowStrategy {
  /**
   * @param {Object} event
   * @param {RunnerContext} ctx
   */
  async onEvent(event, ctx) {
    if (ctx.stats.processed >= 10000) {
      throw new Error('STOP');
    }
    await sleep(5);
  }
}
