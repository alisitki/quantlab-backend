/**
 * Strategyd Run Compare Routes (read-only)
 * GET /runs/compare/:runA/:runB
 */

import { RunCompareService } from '../runtime/RunCompareService.js';

const RUN_ID_RE = /^[a-zA-Z0-9_-]+$/;

export default async function runCompareRoutes(fastify) {
  const comparer = new RunCompareService();

  fastify.get('/runs/compare/:runA/:runB', async (request, reply) => {
    const { runA, runB } = request.params;
    if (!RUN_ID_RE.test(runA) || !RUN_ID_RE.test(runB)) {
      return reply.code(400).send({ error: 'INVALID_RUN_ID', runA, runB });
    }

    const result = await comparer.compare(runA, runB);
    if (result.status !== 200) {
      return reply.code(result.status).send({ error: result.error, runA, runB });
    }

    return result.data;
  });
}
