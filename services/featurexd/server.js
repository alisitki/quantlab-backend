#!/usr/bin/env node
/**
 * Featurexd Server — deterministic feature extraction
 */

import Fastify from 'fastify';
import { loadConfig } from './config.js';
import authMiddleware from './middleware/auth.js';
import { FeatureJobStore } from './FeatureJobStore.js';
import { FeatureOrchestrator } from './FeatureOrchestrator.js';
import featureRoutes from './routes/features.routes.js';

const config = loadConfig();

const fastify = Fastify({
  logger: {
    transport: {
      target: 'pino-pretty'
    }
  }
});

fastify.addHook('preHandler', authMiddleware);

const jobStore = new FeatureJobStore({ jobsDir: config.jobsDir });
await jobStore.init();

const orchestrator = new FeatureOrchestrator({ jobStore, config });

fastify.register(featureRoutes, { orchestrator, jobStore });

fastify.get('/health', async () => ({ status: 'ok', service: 'featurexd' }));

fastify.listen({ port: config.port, host: '0.0.0.0' }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  console.log(`[FEATUREXD] Started. PID=${process.pid} port=${config.port}`);
});

const shutdown = async () => {
  fastify.log.info('Shutting down...');
  await fastify.close();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
