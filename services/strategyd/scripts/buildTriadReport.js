#!/usr/bin/env node
/**
 * buildTriadReport.js — build deterministic triad report by seed or run_id.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TriadReportBuilder } from '../runtime/TriadReportBuilder.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith('--')) continue;
    const name = key.slice(2);
    const value = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true;
    args[name] = value;
    if (value !== true) i++;
  }
  return args;
}

async function readJson(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function loadManifests() {
  const files = await fs.readdir(RUNS_DIR);
  const manifests = [];
  for (const file of files) {
    if (!file.endsWith('.json')) continue;
    if (file === 'index.json') continue;
    const filePath = path.join(RUNS_DIR, file);
    const manifest = await readJson(filePath);
    if (!manifest?.ended_at || !manifest?.ended_reason) continue;
    const runId = manifest?.run_id || path.basename(file, '.json');
    manifests.push({ runId, manifest });
  }
  return manifests;
}

function pickTriad(manifests, { seed, runId }) {
  let target = null;
  if (runId) {
    target = manifests.find((m) => m.runId === runId);
  } else if (seed) {
    const matches = manifests.filter((m) => (m.manifest?.strategy?.seed || null) === seed);
    matches.sort((a, b) => a.runId.localeCompare(b.runId));
    target = matches[0] || null;
  }
  if (!target) return null;
  const strategyId = target.manifest?.strategy?.id || target.manifest?.strategy_id || null;
  const strategySeed = target.manifest?.strategy?.seed || null;
  if (!strategyId || !strategySeed) return null;

  const candidates = manifests.filter((m) => {
    const sId = m.manifest?.strategy?.id || m.manifest?.strategy_id || null;
    const sSeed = m.manifest?.strategy?.seed || null;
    return sId === strategyId && sSeed === strategySeed;
  });

  const modeToRun = new Map();
  for (const item of candidates) {
    const mode = item.manifest?.extra?.ml?.mode || 'off';
    const existing = modeToRun.get(mode);
    if (!existing || item.runId < existing.runId) {
      modeToRun.set(mode, item);
    }
  }

  const off = modeToRun.get('off');
  const shadow = modeToRun.get('shadow');
  const active = modeToRun.get('active');
  if (!off || !shadow || !active) return null;

  return { offRunId: off.runId, shadowRunId: shadow.runId, activeRunId: active.runId };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.seed && !args.run_id) {
    console.error('Usage: node buildTriadReport.js --seed <seed> | --run_id <run_id>');
    process.exit(1);
  }

  const manifests = await loadManifests();
  const triad = pickTriad(manifests, { seed: args.seed, runId: args.run_id });
  if (!triad) {
    console.error('[TriadReport] action=error error=TRIAD_NOT_FOUND');
    process.exit(1);
  }

  const builder = new TriadReportBuilder();
  const result = await builder.build(triad);
  console.log(`[TriadReport] action=written path=${result.outPath}`);
}

main().catch((err) => {
  console.error(`[TriadReport] action=error error=${err.message}`);
  process.exit(1);
});
