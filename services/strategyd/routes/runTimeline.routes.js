/**
 * Strategyd Run Timeline Diff Routes (read-only)
 * GET /runs/timeline/:runA/:runB
 */

import { RunTimelineDiff } from '../runtime/RunTimelineDiff.js';

const RUN_ID_RE = /^[a-zA-Z0-9_-]+$/;

export default async function runTimelineRoutes(fastify) {
  const diff = new RunTimelineDiff();

  fastify.get('/runs/timeline/:runA/:runB', async (request, reply) => {
    const { runA, runB } = request.params;
    if (!RUN_ID_RE.test(runA) || !RUN_ID_RE.test(runB)) {
      return reply.code(400).send({ error: 'INVALID_RUN_ID', runA, runB });
    }

    const result = await diff.compare(runA, runB);
    if (result.status !== 200) {
      return reply.code(result.status).send({ error: result.error, runA, runB });
    }

    return result.data;
  });
}
