#!/usr/bin/env node
/**
 * Labeld Server — deterministic label generation
 */

import Fastify from 'fastify';
import { loadConfig } from './config.js';
import authMiddleware from './middleware/auth.js';
import { LabelJobStore } from './LabelJobStore.js';
import { LabelOrchestrator } from './LabelOrchestrator.js';
import labelRoutes from './routes/labels.routes.js';

const config = loadConfig();

const fastify = Fastify({
  logger: {
    transport: {
      target: 'pino-pretty'
    }
  }
});

fastify.addHook('preHandler', authMiddleware);

const jobStore = new LabelJobStore({ jobsDir: config.jobsDir });
await jobStore.init();

const orchestrator = new LabelOrchestrator({ jobStore, config });

fastify.register(labelRoutes, { orchestrator, jobStore });

fastify.get('/health', async () => ({ status: 'ok', service: 'labeld' }));

fastify.listen({ port: config.port, host: '0.0.0.0' }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  console.log(`[LABELD] Started. PID=${process.pid} port=${config.port}`);
});

const shutdown = async () => {
  fastify.log.info('Shutting down...');
  await fastify.close();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
