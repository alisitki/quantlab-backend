/**
 * Replayd Metrics Route
 * GET /metrics
 */
import { replayMetrics } from '../metrics.js';

export default async function metricsRoutes(fastify, options) {
  fastify.get('/metrics', async (request, reply) => {
    reply.type('text/plain; version=0.0.4; charset=utf-8');
    return replayMetrics.render();
  });
}
