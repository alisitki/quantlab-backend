/**
 * @typedef {import('../types.js').RunnerContext} RunnerContext
 */

export class PrintHeadTailStrategy {
  constructor() {
    this.head = [];
    this.tail = [];
    this.maxSamples = 5;
  }

  /**
   * @param {RunnerContext} ctx
   */
  async onStart(ctx) {
    ctx.logger.info('--- STRATEGY onStart ---');
    ctx.logger.info(`Starting strategy for ${ctx.dataset.parquet}`);
  }

  /**
   * @param {Object} event
   * @param {RunnerContext} ctx
   */
  async onEvent(event, ctx) {
    // Keep first 5
    if (this.head.length < this.maxSamples) {
      this.head.push({ ts_event: event.ts_event, seq: event.seq });
    }

    // Keep last 5 (ring buffer style)
    this.tail.push({ ts_event: event.ts_event, seq: event.seq });
    if (this.tail.length > this.maxSamples) {
      this.tail.shift();
    }
  }

  /**
   * @param {RunnerContext} ctx
   */
  async onEnd(ctx) {
    ctx.logger.info('\n--- STRATEGY RESULT ---');
    ctx.logger.info(`total_processed: ${ctx.stats.processed}`);
    
    ctx.logger.info('\nHEAD 5:');
    this.head.forEach(e => ctx.logger.info(`(${e.ts_event}, ${e.seq})`));

    ctx.logger.info('\nTAIL 5:');
    this.tail.forEach(e => ctx.logger.info(`(${e.ts_event}, ${e.seq})`));
  }
}
