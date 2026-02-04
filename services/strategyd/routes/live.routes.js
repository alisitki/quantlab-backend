/**
 * Live Strategy Routes for strategyd
 *
 * POST /live/start  - Start a live strategy run
 * POST /live/stop   - Stop a live strategy run
 * GET  /live/status - Get status of all live runs
 * GET  /live/status/:id - Get status of specific run
 */

import { LiveStrategyRunner } from '../../../core/strategy/live/LiveStrategyRunner.js';
import { observerRegistry } from '../../../core/observer/ObserverRegistry.js';

// In-memory runner storage (keyed by live_run_id)
const activeRunners = new Map();

export default async function liveRoutes(fastify, options) {

  /**
   * POST /live/start - Start a new live strategy run
   */
  fastify.post('/live/start', async (request, reply) => {
    const {
      exchange,
      symbols,
      strategyPath,
      strategyConfig,
      seed,
      errorPolicy,
      orderingMode,
      enableMetrics,
      riskConfig,
      executionConfig,
      maxLagMs
    } = request.body || {};

    if (!exchange || !symbols || !strategyPath) {
      return reply.code(400).send({
        error: 'MISSING_REQUIRED_FIELDS',
        required: ['exchange', 'symbols', 'strategyPath']
      });
    }

    try {
      const runner = new LiveStrategyRunner({
        dataset: { parquet: 'live', meta: 'live' },
        exchange,
        symbols,
        strategyPath,
        strategyConfig,
        seed,
        errorPolicy,
        orderingMode,
        enableMetrics: enableMetrics !== false,
        riskConfig,
        executionConfig,
        maxLagMs
      });

      const liveRunId = runner.liveRunId;
      activeRunners.set(liveRunId, runner);

      // Start run in background
      runner.run({ handleSignals: false })
        .then((result) => {
          console.log(`[LIVE] Run completed: ${liveRunId}`, JSON.stringify(result));
        })
        .catch((err) => {
          console.error(`[LIVE] Run error: ${liveRunId} - ${err.message}`);
        })
        .finally(() => {
          activeRunners.delete(liveRunId);
        });

      return reply.code(201).send({
        live_run_id: liveRunId,
        status: 'STARTED',
        message: 'Live strategy run started'
      });
    } catch (err) {
      return reply.code(500).send({
        error: 'START_FAILED',
        message: err.message
      });
    }
  });

  /**
   * POST /live/stop - Stop a live strategy run
   */
  fastify.post('/live/stop', async (request, reply) => {
    const { live_run_id } = request.body || {};

    if (!live_run_id) {
      return reply.code(400).send({
        error: 'MISSING_LIVE_RUN_ID'
      });
    }

    // Try local runner first
    const runner = activeRunners.get(live_run_id);
    if (runner) {
      runner.stop();
      return reply.send({
        live_run_id,
        status: 'STOP_REQUESTED'
      });
    }

    // Fallback to observer registry
    const ok = observerRegistry.stopRun(live_run_id, 'API_STOP');
    if (ok) {
      return reply.send({
        live_run_id,
        status: 'STOP_REQUESTED'
      });
    }

    return reply.code(404).send({
      error: 'RUN_NOT_FOUND',
      live_run_id
    });
  });

  /**
   * GET /live/status - Get status of all live runs
   */
  fastify.get('/live/status', async (request, reply) => {
    const runs = observerRegistry.listRuns();
    const health = observerRegistry.getHealth();

    return {
      health,
      runs,
      active_count: runs.filter(r => r.status === 'RUNNING').length,
      local_runners: activeRunners.size
    };
  });

  /**
   * GET /live/status/:id - Get status of specific run
   */
  fastify.get('/live/status/:id', async (request, reply) => {
    const { id } = request.params;
    const runs = observerRegistry.listRuns();
    const run = runs.find(r => r.live_run_id === id);

    if (!run) {
      return reply.code(404).send({
        error: 'RUN_NOT_FOUND',
        live_run_id: id
      });
    }

    return run;
  });
}
