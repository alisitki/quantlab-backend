/**
 * Strategyd Run Compare Routes (read-only)
 *
 * GET /runs/compare/:runA/:runB       - Compare two runs
 * GET /runs/compare/:runA/:runB?include_ml=true - Compare with ML metadata
 * GET /ml/compare/:jobA/:jobB         - Compare two ML models directly
 */

import { RunCompareService } from '../runtime/RunCompareService.js';

const RUN_ID_RE = /^[a-zA-Z0-9_-]+$/;
const JOB_ID_RE = /^[a-zA-Z0-9_-]+$/;

export default async function runCompareRoutes(fastify) {
  const comparer = new RunCompareService();

  /**
   * GET /runs/compare/:runA/:runB - Compare two strategy runs
   * Query params:
   *   - include_ml: boolean - Include ML model comparison
   */
  fastify.get('/runs/compare/:runA/:runB', async (request, reply) => {
    const { runA, runB } = request.params;
    const includeMl = request.query.include_ml === 'true' || request.query.include_ml === '1';

    if (!RUN_ID_RE.test(runA) || !RUN_ID_RE.test(runB)) {
      return reply.code(400).send({ error: 'INVALID_RUN_ID', runA, runB });
    }

    const result = await comparer.compare(runA, runB, { includeMl });
    if (result.status !== 200) {
      return reply.code(result.status).send({ error: result.error, runA, runB });
    }

    return result.data;
  });

  /**
   * GET /ml/compare/:jobA/:jobB - Compare two ML models directly
   *
   * Compares ML job artifacts:
   * - Decision configs (threshold, proba source, featureset)
   * - Training metrics (accuracy, F1, directional hit rate)
   * - Generates recommendation based on metrics
   */
  fastify.get('/ml/compare/:jobA/:jobB', async (request, reply) => {
    const { jobA, jobB } = request.params;

    if (!JOB_ID_RE.test(jobA) || !JOB_ID_RE.test(jobB)) {
      return reply.code(400).send({ error: 'INVALID_JOB_ID', jobA, jobB });
    }

    const result = await comparer.compareMlModels(jobA, jobB);
    if (result.status !== 200) {
      return reply.code(result.status).send({ error: result.error, jobA, jobB });
    }

    return result.data;
  });
}
