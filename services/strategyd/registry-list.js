#!/usr/bin/env node
/**
 * registry-list.js — List experiments and candidates.
 */

import { readFile } from 'node:fs/promises';
import path from 'node:path';

const REGISTRY_DIR = path.resolve('./services/strategyd/registry');
const EXP_JSONL = path.join(REGISTRY_DIR, 'experiments.jsonl');
const CAND_JSONL = path.join(REGISTRY_DIR, 'candidates.jsonl');

async function readJsonl(filePath) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
  } catch {
    return [];
  }
}

async function run() {
  const experiments = await readJsonl(EXP_JSONL);
  const candidates = await readJsonl(CAND_JSONL);

  console.log('--- Experiments ---');
  experiments.forEach(e => {
    console.log(`${e.exp_id} | ${e.strategy_id} | ${e.dataset?.symbol}@${e.dataset?.date} | validated_jobs=${e.validated_jobs}`);
  });

  console.log('\n--- Candidates ---');
  candidates.slice(-10).forEach(c => {
    console.log(`${c.candidate_id} | ${c.strategy_id} | tick_pnl_pct=${c.scores.tick_pnl_pct} | max_dd=${c.scores.max_dd} | trades=${c.scores.trades}`);
  });
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
