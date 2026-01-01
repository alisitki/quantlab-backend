#!/usr/bin/env node
/**
 * Replay Engine v1 — Determinism Test
 * Verifies identical event sequence across multiple runs
 */

import { createHash } from 'node:crypto';
import { ReplayEngine } from './index.js';

const PARQUET_PATH = '/tmp/replay-test/data.parquet';
const META_PATH = '/tmp/replay-test/meta.json';
const BATCH_SIZE = 5000;

// Canonical JSON for hashing (sorted keys, BigInt handled)
function canonicalJson(obj) {
  return JSON.stringify(obj, (_, v) => {
    if (typeof v === 'bigint') return v.toString();
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      return Object.keys(v).sort().reduce((acc, k) => { acc[k] = v[k]; return acc; }, {});
    }
    return v;
  });
}

function sha256(data) {
  return createHash('sha256').update(data).digest('hex');
}

async function runReplay() {
  const engine = new ReplayEngine(PARQUET_PATH, META_PATH);
  await engine.validate();

  const first100 = [];
  const last100 = [];
  let prevTs = null;
  let outOfOrder = 0;

  for await (const row of engine.replay({ batchSize: BATCH_SIZE })) {
    // Collect first 100
    if (first100.length < 100) {
      first100.push(row);
    }

    // Maintain last 100 buffer
    last100.push(row);
    if (last100.length > 100) last100.shift();

    // Check ordering
    const currentTs = BigInt(row.ts_event);
    if (prevTs !== null && currentTs < prevTs) {
      outOfOrder++;
    }
    prevTs = currentTs;
  }

  await engine.close();

  // Hash first 100
  const first100Str = first100.map(r => canonicalJson(r)).join('\n');
  const first100Hash = sha256(first100Str);

  // Hash last 100
  const last100Str = last100.map(r => canonicalJson(r)).join('\n');
  const last100Hash = sha256(last100Str);

  return { first100Hash, last100Hash, outOfOrder };
}

async function main() {
  console.log('=== DETERMINISM TEST ===');
  console.log('');
  console.log('DATASET:');
  console.log(PARQUET_PATH);
  console.log(META_PATH);
  console.log('');
  console.log(`BATCH_SIZE: ${BATCH_SIZE}`);
  console.log('');

  // Run A
  const runA = await runReplay();

  // Run B
  const runB = await runReplay();

  console.log('--- FIRST 100 HASH ---');
  console.log(`RUN A: ${runA.first100Hash}`);
  console.log(`RUN B: ${runB.first100Hash}`);
  console.log(`MATCH: ${runA.first100Hash === runB.first100Hash}`);
  console.log('');

  console.log('--- LAST 100 HASH ---');
  console.log(`RUN A: ${runA.last100Hash}`);
  console.log(`RUN B: ${runB.last100Hash}`);
  console.log(`MATCH: ${runA.last100Hash === runB.last100Hash}`);
  console.log('');

  console.log('--- ORDER CHECK ---');
  console.log(`OUT_OF_ORDER_COUNT: ${runA.outOfOrder + runB.outOfOrder}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
