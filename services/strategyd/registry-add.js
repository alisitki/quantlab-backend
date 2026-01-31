#!/usr/bin/env node
/**
 * registry-add.js — Add experiment leaderboard into registry.
 *
 * Usage:
 *   node registry-add.js --exp_id <exp_id>
 */

import { readFile, writeFile, mkdir, appendFile } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

const REGISTRY_DIR = path.resolve('./services/strategyd/registry');
const EXP_DIR = path.resolve('./services/strategyd/experiments');
const INDEX_PATH = path.join(REGISTRY_DIR, 'index.json');
const EXP_JSONL = path.join(REGISTRY_DIR, 'experiments.jsonl');

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

function stableStringify(obj) {
  if (obj === null || typeof obj !== 'object') return JSON.stringify(obj);
  if (Array.isArray(obj)) return '[' + obj.map(stableStringify).join(',') + ']';
  const keys = Object.keys(obj).sort();
  return '{' + keys.map(k => `"${k}":${stableStringify(obj[k])}`).join(',') + '}';
}

function hashRows(rows) {
  return createHash('sha256').update(stableStringify(rows)).digest('hex');
}

async function readJsonIfExists(filePath, fallback) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function gridSummaryFromRows(rows) {
  if (!rows.length) return {};
  const keys = new Set();
  rows.forEach(r => {
    if (!r.params_short) return;
    r.params_short.split(';').forEach(pair => {
      const [k] = pair.split('=');
      if (k) keys.add(k);
    });
  });
  return { params_keys: Array.from(keys).sort(), rows: rows.length };
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.exp_id) {
    console.error('Usage: node registry-add.js --exp_id <exp_id>');
    process.exit(1);
  }

  await mkdir(REGISTRY_DIR, { recursive: true });
  const expId = args.exp_id;
  const leaderboardPath = path.join(EXP_DIR, expId, 'leaderboard.json');

  const leaderboard = JSON.parse(await readFile(leaderboardPath, 'utf8'));
  const leaderboardHash = hashRows(leaderboard.rows || []);

  const record = {
    exp_id: expId,
    created_at: new Date().toISOString(),
    strategy_id: leaderboard.strategy_id,
    dataset: leaderboard.dataset,
    grid_summary: gridSummaryFromRows(leaderboard.rows || []),
    top_k: leaderboard.rows?.length || 0,
    leaderboard_path: leaderboardPath,
    leaderboard_hash: leaderboardHash,
    validated_jobs: leaderboard.validated_jobs || 0
  };

  await appendFile(EXP_JSONL, JSON.stringify(record) + '\n');

  const index = await readJsonIfExists(INDEX_PATH, {});
  index[expId] = {
    leaderboard_path: leaderboardPath,
    leaderboard_hash: leaderboardHash,
    strategy_id: leaderboard.strategy_id,
    dataset: leaderboard.dataset,
    created_at: record.created_at
  };
  await writeFile(INDEX_PATH, JSON.stringify(index, null, 2));

  console.log(`[REGISTRY] exp_id=${expId} leaderboard_hash=${leaderboardHash}`);
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
