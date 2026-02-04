/**
 * Strategyd Runs Routes (read-only)
 * GET /runs
 * GET /runs/:run_id
 * GET /run/:id (legacy manifest)
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ActiveHealth } from '../runtime/ActiveHealth.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const INDEX_PATH = path.join(RUNS_DIR, 'index.json');
const HEALTH_DIR = path.join(RUNS_DIR, 'health');
const SUMMARY_DIR = path.join(RUNS_DIR, 'summary');
const REPORT_DIR = path.join(RUNS_DIR, 'report');
const ARCHIVE_DIR = path.join(RUNS_DIR, 'archive');

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

function parseArtifactId(filename, prefix) {
  if (!filename.startsWith(prefix) || !filename.endsWith('.json')) return null;
  const base = filename.slice(0, -5);
  const suffix = base.slice(prefix.length);
  const lastUnderscore = suffix.lastIndexOf('_');
  if (lastUnderscore === -1) return { id: base, strategy_id: null, seed: null };
  return {
    id: base,
    strategy_id: suffix.slice(0, lastUnderscore) || null,
    seed: suffix.slice(lastUnderscore + 1) || null
  };
}

async function listArtifacts(prefix, { seed, strategyId }) {
  try {
    const files = await fs.readdir(REPORT_DIR);
    const entries = [];
    for (const file of files) {
      const parsed = parseArtifactId(file, prefix);
      if (!parsed) continue;
      if (seed && parsed.seed !== seed) continue;
      if (strategyId && parsed.strategy_id !== strategyId) continue;
      const stat = await fs.stat(path.join(REPORT_DIR, file));
      entries.push({
        id: parsed.id,
        strategy_id: parsed.strategy_id,
        seed: parsed.seed,
        created_at: stat.mtime.toISOString()
      });
    }
    entries.sort((a, b) => a.id.localeCompare(b.id));
    return entries;
  } catch {
    return [];
  }
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

  // GET /runs/health/active - ACTIVE health snapshot
  fastify.get('/runs/health/active', async (_request, reply) => {
    try {
      const strategyId = runner?.getStrategyId ? runner.getStrategyId() : null;
      const seed = runner?.getStrategySeed ? runner.getStrategySeed() : null;
      const health = new ActiveHealth({ strategyId, seed });
      return reply.code(200).send(health.getSnapshot());
    } catch {
      return reply.code(200).send({
        active_enabled: false,
        strategy_id: null,
        seed: null,
        active_config_present: false,
        limits: { max_weight: null, daily_cap: null },
        guards: { kill_switch_required: true, safety_audit_required: true },
        provenance: {
          active_config_hash: null,
          decision_hash: null,
          triad_report_hash: null
        }
      });
    }
  });

  // GET /runs/report - list triad reports
  fastify.get('/runs/report', async (request) => {
    const { seed, strategy_id } = request.query || {};
    return listArtifacts('triad_', { seed, strategyId: strategy_id });
  });

  // GET /runs/report/:id - fetch report
  fastify.get('/runs/report/:id', async (request, reply) => {
    const id = request.params.id;
    const filePath = path.join(REPORT_DIR, `${id}.json`);
    try {
      const doc = await readJson(filePath);
      return doc;
    } catch {
      return reply.code(404).send({ error: 'REPORT_NOT_FOUND', id });
    }
  });

  // GET /runs/decision - list decisions
  fastify.get('/runs/decision', async (request) => {
    const { seed, strategy_id } = request.query || {};
    return listArtifacts('decision_', { seed, strategyId: strategy_id });
  });

  // GET /runs/decision/:id - fetch decision
  fastify.get('/runs/decision/:id', async (request, reply) => {
    const id = request.params.id;
    const filePath = path.join(REPORT_DIR, `${id}.json`);
    try {
      const doc = await readJson(filePath);
      return doc;
    } catch {
      return reply.code(404).send({ error: 'DECISION_NOT_FOUND', id });
    }
  });

  // GET /runs/archive - list archive directories
  fastify.get('/runs/archive', async () => {
    try {
      const entries = await fs.readdir(ARCHIVE_DIR, { withFileTypes: true });
      const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name);
      dirs.sort();
      return dirs;
    } catch {
      return [];
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
