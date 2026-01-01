/**
 * @typedef {import('../types.js').RunnerContext} RunnerContext
 */

export class FaultyStrategy {
  /**
   * @param {Object} event
   * @param {RunnerContext} ctx
   */
  async onEvent(event, ctx) {
    if (ctx.stats.processed === 10000) {
      throw new Error("INTENTIONAL_STRATEGY_FAILURE");
    }
  }
}
