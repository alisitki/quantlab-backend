/**
 * Strategyd Run Artifacts Routes (read-only)
 * GET /runs/:run_id/manifest
 * GET /runs/:run_id/archive
 */

import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const RUN_ID_RE = /^[a-zA-Z0-9_-]+$/;

export default async function runArtifactsRoutes(fastify) {
  function validateRunId(runId) {
    return RUN_ID_RE.test(runId);
  }

  fastify.get('/runs/:run_id/manifest', async (request, reply) => {
    const runId = request.params.run_id;
    if (!validateRunId(runId)) {
      return reply.code(400).send({ error: 'INVALID_RUN_ID', id: runId });
    }

    const filePath = path.join(RUNS_DIR, `${runId}.json`);
    try {
      const data = await fsPromises.readFile(filePath, 'utf8');
      reply.header('Content-Type', 'application/json');
      return data;
    } catch {
      return reply.code(404).send({ error: 'RUN_NOT_FOUND', id: runId });
    }
  });

  fastify.get('/runs/:run_id/archive', async (request, reply) => {
    const runId = request.params.run_id;
    if (!validateRunId(runId)) {
      return reply.code(400).send({ error: 'INVALID_RUN_ID', id: runId });
    }

    const filePath = path.join(RUNS_DIR, `${runId}.json.gz`);
    try {
      await fsPromises.access(filePath);
      reply.header('Content-Type', 'application/gzip');
      return reply.send(fs.createReadStream(filePath));
    } catch {
      return reply.code(404).send({ error: 'RUN_NOT_FOUND', id: runId });
    }
  });
}
