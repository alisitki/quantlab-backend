#!/usr/bin/env node
/**
 * Replay Engine v1.1 — Cursor-Based Verification
 * Tests cursor-based pagination with ts_event + seq
 */

import { readFile } from 'node:fs/promises';
import { ReplayEngine } from '../index.js';

const PARQUET_PATH = '/tmp/replay-test2/data.parquet';
const META_PATH = '/tmp/replay-test2/meta.json';
const BATCH_SIZE = 5000;

async function main() {
  const metaRaw = await readFile(META_PATH, 'utf-8');
  const meta = JSON.parse(metaRaw);

  console.log('=== CURSOR-BASED REPLAY TEST ===');
  console.log('');
  console.log('DATASET:');
  console.log(`  Parquet: ${PARQUET_PATH}`);
  console.log(`  Meta: ${META_PATH}`);
  console.log(`  meta.rows: ${meta.rows}`);
  console.log(`  BATCH_SIZE: ${BATCH_SIZE}`);
  console.log('');

  const engine = new ReplayEngine(PARQUET_PATH, META_PATH);
  await engine.validate();

  const first5 = [];
  const last5 = [];
  let batchIndex = 0;
  let batchCount = 0;
  let firstTs = null, firstSeq = null;
  let lastTs = null, lastSeq = null;
  let totalRows = 0;

  console.log('=== BATCH LOGS ===');

  for await (const row of engine.replay({ batchSize: BATCH_SIZE })) {
    if (batchCount === 0) {
      firstTs = row.ts_event;
      firstSeq = row.seq;
    }
    lastTs = row.ts_event;
    lastSeq = row.seq;
    batchCount++;
    totalRows++;

    // Collect first 5
    if (first5.length < 5) {
      first5.push({ ts_event: row.ts_event, seq: row.seq });
    }

    // Maintain last 5 buffer
    last5.push({ ts_event: row.ts_event, seq: row.seq });
    if (last5.length > 5) last5.shift();

    if (batchCount === BATCH_SIZE) {
      console.log(`Batch ${batchIndex} | rows=${batchCount} | ${firstTs}, ${firstSeq} → ${lastTs}, ${lastSeq}`);
      batchIndex++;
      batchCount = 0;
      firstTs = null;
      firstSeq = null;
    }
  }

  // Final partial batch
  if (batchCount > 0) {
    console.log(`Batch ${batchIndex} | rows=${batchCount} | ${firstTs}, ${firstSeq} → ${lastTs}, ${lastSeq}`);
  }

  await engine.close();

  console.log('');
  console.log('=== TOTAL ROW COUNT ===');
  console.log(`Replayed: ${totalRows}`);
  console.log(`meta.rows: ${meta.rows}`);
  console.log(`MATCH: ${totalRows === meta.rows}`);

  console.log('');
  console.log('=== FIRST 5 EVENTS (ts_event, seq) ===');
  for (const e of first5) {
    console.log(`  ${e.ts_event}, ${e.seq}`);
  }

  console.log('');
  console.log('=== LAST 5 EVENTS (ts_event, seq) ===');
  for (const e of last5) {
    console.log(`  ${e.ts_event}, ${e.seq}`);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
