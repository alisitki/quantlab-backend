#!/usr/bin/env node
/**
 * strategy-get.js — Resolve strategy artifact path.
 *
 * Usage:
 *   node strategy-get.js --strategy_id ema_cross [--version v1.0.0]
 */

import { readFile } from 'node:fs/promises';
import path from 'node:path';

const REGISTRY_DIR = path.resolve('./services/strategyd/strategy_registry');
const INDEX_PATH = path.join(REGISTRY_DIR, 'index.json');
const JSONL_PATH = path.join(REGISTRY_DIR, 'strategies.jsonl');

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

async function readJsonIfExists(filePath, fallback) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function readJsonl(filePath) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
  } catch {
    return [];
  }
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.strategy_id) {
    console.error('Usage: node strategy-get.js --strategy_id <id> [--version v1.0.0]');
    process.exit(1);
  }
  const strategyId = args.strategy_id;
  const version = args.version;

  if (!version) {
    const index = await readJsonIfExists(INDEX_PATH, {});
    const entry = index[strategyId];
    if (!entry) {
      console.error('NOT_FOUND');
      process.exit(1);
    }
    console.log(entry.artifact_path);
    return;
  }

  const rows = await readJsonl(JSONL_PATH);
  const match = rows.find(r => r.strategy_id === strategyId && r.strategy_version === version);
  if (!match) {
    console.error('NOT_FOUND');
    process.exit(1);
  }
  console.log(match.artifact_path);
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
