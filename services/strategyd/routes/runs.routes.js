/**
 * Strategyd Runs Routes (read-only)
 * GET /runs
 * GET /runs/:run_id
 * GET /run/:id (legacy manifest)
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const INDEX_PATH = path.join(RUNS_DIR, 'index.json');
const HEALTH_DIR = path.join(RUNS_DIR, 'health');
const SUMMARY_DIR = path.join(RUNS_DIR, 'summary');

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

export default async function runsRoutes(fastify, options) {
  const { runner, runsCache } = options;
  const manifestManager = runner.getManifestManager();

  async function getIndex() {
    if (runsCache?.index) return runsCache.index;
    try {
      const data = await readJson(INDEX_PATH);
      if (Array.isArray(data)) {
        if (runsCache) runsCache.index = data;
        return data;
      }
    } catch {
      // ignore missing index
    }
    if (runsCache) runsCache.index = [];
    return [];
  }

  async function getHealthClass(runId) {
    if (!runId) return null;
    const cached = runsCache?.health?.get(runId);
    if (cached !== undefined) return cached;
    const filePath = path.join(HEALTH_DIR, `${runId}.json`);
    try {
      const data = await readJson(filePath);
      const value = data?.class || null;
      if (runsCache?.health) runsCache.health.set(runId, value);
      return value;
    } catch {
      if (runsCache?.health) runsCache.health.set(runId, null);
      return null;
    }
  }

  // GET /runs - list runs from index
  fastify.get('/runs', async (request) => {
    const { health, ended_reason, limit, offset } = request.query || {};
    let entries = await getIndex();

    if (ended_reason) {
      entries = entries.filter((entry) => entry?.ended_reason === ended_reason);
    }

    if (health) {
      const filtered = [];
      for (const entry of entries) {
        const runId = entry?.run_id;
        const healthClass = await getHealthClass(runId);
        if (healthClass === health) filtered.push(entry);
      }
      entries = filtered;
    }

    const start = Math.max(0, Number(offset || 0));
    const lim = Math.max(0, Number(limit || entries.length));
    const page = entries.slice(start, start + lim);

    return { count: entries.length, runs: page.map((entry) => entry.run_id) };
  });

  // GET /runs/:run_id - summary
  fastify.get('/runs/:run_id', async (request, reply) => {
    const runId = request.params.run_id;
    const filePath = path.join(SUMMARY_DIR, `${runId}.json`);
    try {
      const summary = await readJson(filePath);
      return summary;
    } catch {
      return reply.code(404).send({ error: 'RUN_NOT_FOUND', id: runId });
    }
  });

  // GET /run/:id - legacy manifest
  fastify.get('/run/:id', async (request, reply) => {
    const { id } = request.params;
    try {
      const manifest = await manifestManager.get(id);
      if (!manifest) {
        return reply.code(404).send({ error: 'RUN_NOT_FOUND', id });
      }
      return manifest;
    } catch {
      return reply.code(404).send({ error: 'RUN_NOT_FOUND', id });
    }
  });
}
