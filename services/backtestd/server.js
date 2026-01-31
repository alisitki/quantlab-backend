#!/usr/bin/env node
/**
 * Backtestd Server — Orchestrates determinism backtests
 */

import Fastify from 'fastify';
import { loadConfig } from './config.js';
import authMiddleware from './middleware/auth.js';
import { JobStore } from './JobStore.js';
import { BacktestOrchestrator } from './BacktestOrchestrator.js';
import backtestsRoutes from './routes/backtests.routes.js';

const config = loadConfig();

const fastify = Fastify({
  logger: {
    transport: {
      target: 'pino-pretty'
    }
  }
});

fastify.addHook('preHandler', authMiddleware);

const jobStore = new JobStore({ backtestsDir: config.backtestsDir });
await jobStore.init();

const orchestrator = new BacktestOrchestrator({ jobStore, config });

fastify.register(backtestsRoutes, { orchestrator, jobStore });

fastify.get('/health', async () => ({ status: 'ok', service: 'backtestd' }));

fastify.listen({ port: config.port, host: '0.0.0.0' }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  console.log(`[BACKTESTD] Started. PID=${process.pid} port=${config.port}`);
});

const shutdown = async () => {
  fastify.log.info('Shutting down...');
  await fastify.close();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
