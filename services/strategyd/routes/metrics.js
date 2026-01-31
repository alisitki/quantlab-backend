/**
 * Strategyd Metrics Route
 * GET /metrics
 */

export default async function metricsRoutes(fastify, options) {
  const { runner } = options;
  
  fastify.get('/metrics', async (request, reply) => {
    reply.type('text/plain; version=0.0.4; charset=utf-8');
    return runner.renderMetrics();
  });
}
