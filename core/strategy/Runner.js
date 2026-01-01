/**
 * @typedef {import('./types.js').RunnerContext} RunnerContext
 * @typedef {import('./types.js').Strategy} Strategy
 * @typedef {import('../replay/index.js').ReplayEngine} ReplayEngine
 */

/**
 * Runs a ReplayEngine with a given Strategy.
 * 
 * @param {Object} params
 * @param {ReplayEngine} params.replayEngine - The engine instance
 * @param {Strategy} params.strategy - The strategy instance
 * @param {Object} [params.options] - Replay options (batchSize, startTs, endTs)
 * @returns {Promise<RunnerContext>}
 */
export async function runReplayWithStrategy({ replayEngine, strategy, options = {} }) {
  const meta = await replayEngine.getMeta();
  
  // Optional execution engine
  const executionEngine = options.executionEngine || null;

  /** @type {RunnerContext} */
  const ctx = {
    runId: `run_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    dataset: {
      parquet: options.parquetPath || 'unknown',
      meta: options.metaPath || 'unknown'
    },
    stats: {
      processed: 0
    },
    logger: {
      info: (...args) => console.log(`[INFO]`, ...args),
      error: (...args) => console.error(`[ERROR]`, ...args),
      warn: (...args) => console.warn(`[WARN]`, ...args)
    },
    execution: executionEngine,
    placeOrder: executionEngine 
      ? (orderIntent) => executionEngine.onOrder(orderIntent)
      : null
  };

  // Re-fetch paths from engine if private (though in v1.1 they are private)
  // Let's assume we can get them or pass them in. 
  // Looking at ReplayEngine.js, they are private. I'll need to pass them or the runner should have them.
  // Actually, the runner can take them from the engine if we exposed them, or just use what's passed.
  // I will check ReplayEngine.js again.
  
  if (strategy.onStart) {
    await strategy.onStart(ctx);
  }

  const ClockClass = options.clock || (await import('../replay/clock/AsapClock.js')).default;
  const clock = typeof ClockClass === 'function' ? new ClockClass() : ClockClass;
  
  if (clock.init) {
    // We'll pass the first event if we find it, but let's just create the generator
  }

  const replayGenerator = replayEngine.replay(options);
  let prevEvent = null;

  for await (const event of replayGenerator) {
    if (clock.beforeNext && prevEvent) {
      await clock.beforeNext(prevEvent, event);
    }

    // Update execution engine with current event (for price/MTM)
    if (executionEngine) {
      executionEngine.onEvent(event);
    }

    await strategy.onEvent(event, ctx);
    
    ctx.stats.processed++;
    prevEvent = event;

    if (ctx.stats.processed % 5000 === 0) {
      ctx.logger.info(`processed=${ctx.stats.processed} last=(ts_event=${event.ts_event}, seq=${event.seq})`);
    }
  }

  if (clock.onEnd) {
    await clock.onEnd();
  }

  if (strategy.onEnd) {
    await strategy.onEnd(ctx);
  }

  // Log execution summary if engine was used
  if (executionEngine) {
    const state = executionEngine.snapshot();
    ctx.logger.info(`Execution: fills=${state.fills.length} equity=${state.equity.toFixed(2)} pnl=${state.totalRealizedPnl.toFixed(2)}`);
  }

  ctx.logger.info(`Final stats: processed=${ctx.stats.processed}`);
  return ctx;
}
