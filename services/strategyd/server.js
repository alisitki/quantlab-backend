#!/usr/bin/env node
/**
 * Strategyd Server — With Control API
 */

import 'dotenv/config';
import Fastify from 'fastify';
import { SSEStrategyRunner } from './runtime/SSEStrategyRunner.js';
import { RunRetentionCleaner } from './runtime/RunRetentionCleaner.js';
import { RunIndexBuilder } from './runtime/RunIndexBuilder.js';
import { RunHealthEvaluator } from './runtime/RunHealthEvaluator.js';
import { RunSummaryBuilder } from './runtime/RunSummaryBuilder.js';
import stateRoutes from './routes/state.js';
import tradesRoutes from './routes/trades.js';
import controlRoutes from './routes/control.js';
import runsRoutes from './routes/runs.routes.js';
import runArtifactsRoutes from './routes/runArtifacts.routes.js';
import runCompareRoutes from './routes/runCompare.routes.js';
import runTimelineRoutes from './routes/runTimeline.routes.js';
import healthRoutes from './routes/health.routes.js';
import metricsRoutes from './routes/metrics.js';
import liveRoutes from './routes/live.routes.js';
import bridgeRoutes from './routes/bridge.routes.js';
import monitorRoutes from './routes/monitor.routes.js';
import sloRoutes from './routes/slo.routes.js';
import { ReplaySeekHelper } from './runtime/ReplaySeekHelper.js';
import authMiddleware from './middleware/auth.js';


const now = new Date();
const timestamp = now.toISOString().replace(/[:.]/g, '-');
const random = Math.floor(Math.random() * 1000).toString().padStart(3, '0');
const defaultRunId = `ema_cross_${process.env.SYMBOL || 'BTCUSDT'}_${process.env.DATE || '2024-01-15'}_${timestamp}_${random}`;


const runtimeFlag = process.env.STRATEGY_RUNTIME_V2;
const runtimeV2Enabled = runtimeFlag !== '0';
const yieldEvery = Number(process.env.STRATEGYD_YIELD_EVERY || 1000);
const bpHigh = Number(process.env.STRATEGYD_BACKPRESSURE_HIGH || 1500);
const bpLow = Number(process.env.STRATEGYD_BACKPRESSURE_LOW || 500);
const backtestJobId = process.env.BACKTEST_JOB_ID || null;

const config = {
  runId: process.env.RUN_ID || defaultRunId,
  port: Number(process.env.STRATEGYD_PORT) || 3031,

  replaydUrl: process.env.REPLAYD_URL || 'http://localhost:3030',
  replaydToken: process.env.REPLAYD_TOKEN,
  dataset: process.env.DATASET || 'bbo',
  symbol: process.env.SYMBOL || 'BTCUSDT',
  date: process.env.DATE || '2024-01-15',
  speed: process.env.SPEED || 'asap',
  cursor: process.env.CURSOR || null,
  strategyConfig: {
    fastPeriod: Number(process.env.FAST_PERIOD) || 9,
    slowPeriod: Number(process.env.SLOW_PERIOD) || 21,
    positionSize: Number(process.env.POSITION_SIZE) || 0.1
  },
  executionConfig: {
    initialCapital: Number(process.env.INITIAL_CAPITAL) || 10000,
    feeRate: Number(process.env.FEE_RATE) || 0.0004
  },
  strategyRuntimeV2: runtimeV2Enabled
};

const fastify = Fastify({
  logger: {
    transport: {
      target: 'pino-pretty'
    }
  }
});

// Fix BigInt serialization
fastify.setReplySerializer((payload) => {
  return JSON.stringify(payload, (_, v) => 
    typeof v === 'bigint' ? v.toString() : v
  );
});


const runner = new SSEStrategyRunner(config);
const summaryBuilder = new RunSummaryBuilder();
export const replaySeekHelper = new ReplaySeekHelper();
const runsCache = {
  index: null,
  health: new Map()
};
const indexBuilder = new RunIndexBuilder({
  onRebuildSummary: () => summaryBuilder.rebuildAll(),
  onIndexRebuilt: () => {
    runsCache.index = null;
  }
});
const healthEvaluator = new RunHealthEvaluator({
  onRebuildSummary: (runId) => summaryBuilder.rebuildRun(runId)
});
const retentionCleaner = new RunRetentionCleaner({
  onRebuildIndex: () => indexBuilder.rebuild()
});

console.log(`[SERVER] Started. PID: ${process.pid}, RunID: ${config.runId}`);
console.log(
  `[SERVER] Runtime mode=${runtimeV2Enabled ? 'v2' : 'legacy'} yieldEvery=${yieldEvery} backpressureHigh=${bpHigh} backpressureLow=${bpLow}`
);
if (backtestJobId) {
  console.log(`[SERVER] Backtest job_id=${backtestJobId}`);
}

Promise.resolve()
  .then(() => indexBuilder.rebuild())
  .then(() => healthEvaluator.evaluateAll())
  .then(() => summaryBuilder.rebuildAll())
  .catch(() => {});
retentionCleaner.start();


// Auth
fastify.addHook('preHandler', authMiddleware);

// Register Routes
fastify.register(stateRoutes, { runner });
fastify.register(tradesRoutes, { runner });
fastify.register(controlRoutes, { runner });
fastify.register(healthRoutes, { runner });
fastify.register(runsRoutes, { runner, runsCache });
fastify.register(runArtifactsRoutes);
fastify.register(runCompareRoutes);
fastify.register(runTimelineRoutes);
fastify.register(metricsRoutes, { runner });
fastify.register(liveRoutes);
fastify.register(bridgeRoutes);
fastify.register(monitorRoutes);
fastify.register(sloRoutes);

runner.startMonitoring();



// Health check
fastify.get('/health', async () => ({ status: 'ok', service: 'strategyd' }));

/**
 * Graceful Shutdown
 */
const shutdown = async () => {
  fastify.log.info('Shutting down...');
  runner.setEndedReason('interrupted');
  await runner.finalizeManifest();
  runner.stop();
  await fastify.close();
  
  const snapshot = runner.getSnapshot();
  const stats = runner.getStats();
  
  console.log('\n--- FINAL RESULTS ---');
  console.log(`Events:    ${stats.eventCount}`);
  console.log(`Signals:   ${stats.signalCount}`);
  console.log(`Fills:     ${snapshot.fills.length}`);
  console.log(`Equity:    ${snapshot.equity.toFixed(2)}`);
  console.log(`PnL:       ${snapshot.totalRealizedPnl.toFixed(2)}`);
  
  process.exit(0);
};


process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

/**
 * Start Server & Runner
 */
const start = async () => {
  try {
    await fastify.listen({ port: config.port, host: '0.0.0.0' });
    fastify.log.info(`🚀 Strategyd API running on port ${config.port}`);
    
    // Start strategy runner in background (non-blocking for API)
    runner.start()
      .then(() => {
        fastify.log.info('Stream finished normally');
      })
      .catch((err) => {
        fastify.log.error(`Runner failed: ${err.message}`);
      });
      
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
