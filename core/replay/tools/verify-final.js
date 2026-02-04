#!/usr/bin/env node
/**
 * FINAL REPLAY ENGINE VERIFICATION (v1.1)
 * Validates ordering, determinism, and completeness.
 */

import { ReplayEngine } from '../index.js';
import { createHash } from 'node:crypto';

const DEFAULT_BATCH_SIZE = 5000;

function sha256(data) {
  return createHash('sha256').update(data).digest('hex');
}

async function runOnce(parquetPath, metaPath, batchSize) {
  const engine = new ReplayEngine(parquetPath, metaPath);
  const hash = createHash('sha256');

  try {
    const meta = await engine.getMeta();
    await engine.validate();

    let totalReplayed = 0;
    let batchIndex = 0;
    let batchRows = 0;
    let batchFirst = null;
    let batchLast = null;

    const first5 = [];
    const last5 = [];

    for await (const row of engine.replay({ batchSize })) {
      const ts = BigInt(row.ts_event);
      const seq = BigInt(row.seq);

      if (totalReplayed < 5) first5.push(`(${ts}, ${seq})`);
      last5.push(`(${ts}, ${seq})`);
      if (last5.length > 5) last5.shift();

      hash.update(`${ts}:${seq}\n`);

      if (batchRows === 0) batchFirst = { ts, seq };
      batchLast = { ts, seq };
      batchRows++;

      if (batchRows === batchSize) {
        console.log(`Batch ${batchIndex} | ${batchRows} | first: (${batchFirst.ts}, ${batchFirst.seq}) → last: (${batchLast.ts}, ${batchLast.seq})`);
        batchIndex++;
        batchRows = 0;
      }

      totalReplayed++;
    }

    if (batchRows > 0) {
      console.log(`Batch ${batchIndex} | ${batchRows} | first: (${batchFirst.ts}, ${batchFirst.seq}) → last: (${batchLast.ts}, ${batchLast.seq})`);
    }

    return {
      meta,
      totalReplayed,
      first5,
      last5,
      hash: hash.digest('hex')
    };
  } finally {
    await engine.close();
  }
}

async function verify() {
  const [,, parquetPathArg, metaPathArg, batchSizeArg] = process.argv;
  const parquetPath = parquetPathArg || process.env.REPLAY_PARQUET;
  const metaPath = metaPathArg || process.env.REPLAY_META;
  const batchSize = batchSizeArg ? Number(batchSizeArg) : DEFAULT_BATCH_SIZE;

  if (!parquetPath || !metaPath) {
    console.error('Usage: node tools/verify-final.js <parquet_path> <meta_path> [batch_size]');
    console.error('Or set env: REPLAY_PARQUET, REPLAY_META');
    process.exit(1);
  }

  console.log('=== FINAL REPLAY ENGINE VERIFICATION (v1.1) ===\n');
  console.log('DATASET:');
  console.log(parquetPath);
  console.log(metaPath);
  console.log('');
  
  try {
    console.log('--- RUN A ---');
    const runA = await runOnce(parquetPath, metaPath, batchSize);
    console.log('\n--- RUN B ---');
    const runB = await runOnce(parquetPath, metaPath, batchSize);

    console.log('\n--- SAMPLES (RUN A) ---');
    console.log('FIRST 5:');
    console.log(runA.first5.join('\n'));
    console.log('LAST 5:');
    console.log(runA.last5.join('\n'));

    console.log('\n--- FINAL STATS ---');
    console.log(`runA replayed: ${runA.totalReplayed}`);
    console.log(`runB replayed: ${runB.totalReplayed}`);
    console.log(`meta.rows:     ${runA.meta.rows}`);
    console.log(`count match:   ${runA.totalReplayed === runA.meta.rows && runB.totalReplayed === runA.meta.rows}`);
    console.log(`hash match:    ${runA.hash === runB.hash}`);

    const pass = (runA.totalReplayed === runA.meta.rows) &&
      (runB.totalReplayed === runA.meta.rows) &&
      (runA.hash === runB.hash);
    console.log(`\nRESULT: ${pass ? 'PASS' : 'FAIL'}`);
    if (!pass) process.exit(1);
  } catch (err) {
    console.error('\n[FATAL ERROR]', err.message);
    process.exit(1);
  }
}

verify();
