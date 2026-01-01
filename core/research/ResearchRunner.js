import { ResearchExecution } from './ResearchExecution.js';
import { ResearchMetrics } from './ResearchMetrics.js';

/**
 * ResearchRunner drives a strategy with sampled data and lightweight execution.
 */
export class ResearchRunner {
  /**
   * Run strategy in research mode.
   * @param {Object} params
   * @param {import('../replay/ReplayEngine.js').ReplayEngine} params.replayEngine
   * @param {import('../strategy/types.js').Strategy} params.strategy
   * @param {Object} [params.sampler] - Instance of TimeSampler or EventSampler
   * @param {Object} [params.options] - Replay options (startTs, endTs, etc.)
   * @param {import('../features/FeatureBuilder.js').FeatureBuilder} [params.featureBuilder] - Optional feature builder
   */
  static async runResearch({ replayEngine, strategy, sampler, options = {}, featureBuilder = null }) {
    const execution = new ResearchExecution();
    
    // Create Research Context (similar to RunnerContext but simplified)
    const ctx = {
      runId: `research_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      stats: {
        processed: 0,
        sampled: 0
      },
      logger: {
        info: (...args) => {}, // Silent by default for speed
        error: (...args) => console.error(`[RESEARCH ERROR]`, ...args),
        warn: (...args) => console.warn(`[RESEARCH WARN]`, ...args)
      },
      execution: execution,
      placeOrder: (orderIntent) => execution.onOrder(orderIntent)
    };

    if (strategy.onStart) {
      await strategy.onStart(ctx);
    }

    const replayGenerator = replayEngine.replay(options);

    for await (const event of replayGenerator) {
      ctx.stats.processed++;

      // Sampler check
      if (sampler && !sampler.shouldProcess(event)) {
        continue;
      }

      ctx.stats.sampled++;

      // Update execution with current price for MTM
      execution.onEvent(event);

      // Compute features if builder provided
      if (featureBuilder) {
        ctx.currentFeatures = featureBuilder.onEvent(event);
      }

      // Run strategy
      await strategy.onEvent(event, ctx);
    }

    if (strategy.onEnd) {
      await strategy.onEnd(ctx);
    }

    const snapshot = execution.snapshot();
    const metrics = ResearchMetrics.compute(snapshot);

    return {
      runId: ctx.runId,
      stats: ctx.stats,
      snapshot,
      metrics
    };
  }
}
