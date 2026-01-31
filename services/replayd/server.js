#!/usr/bin/env node
/**
 * Replayd Server
 * Fastify-based replay service exposing ReplayEngine via SSE.
 */

import 'dotenv/config';
import Fastify from 'fastify';
import healthRoutes from './routes/health.js';
import streamRoutes from './routes/stream.js';
import metricsRoutes from './routes/metrics.js';
import authMiddleware from './middleware/auth.js';
import { SERVICE_PORT, REPLAY_VERSION } from './config.js';
import { startReplayTelemetry } from './metrics.js';

const fastify = Fastify({
  logger: {
    level: 'info',
    transport: {
      target: 'pino-pretty',
      options: { colorize: true }
    }
  }
});

// Auth
fastify.addHook('preHandler', authMiddleware);

// Register routes
fastify.register(healthRoutes);
fastify.register(streamRoutes);
fastify.register(metricsRoutes);

startReplayTelemetry();


// Graceful shutdown
const shutdown = async (signal) => {
  fastify.log.info(`Received ${signal}, shutting down...`);
  await fastify.close();
  process.exit(0);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Start server
const start = async () => {
  try {
    const host = process.env.REPLAYD_HOST || '0.0.0.0';
    await fastify.listen({ port: SERVICE_PORT, host });
    fastify.log.info(`🚀 replayd v${REPLAY_VERSION} running on ${host}:${SERVICE_PORT}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
