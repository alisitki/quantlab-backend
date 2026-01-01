#!/usr/bin/env node
/**
 * FINAL REPLAY ENGINE VERIFICATION (v1.1)
 * Validates cursor-based logic, ordering, and completeness via S3.
 */

import { ReplayEngine } from './index.js';

const DATA_S3 = 's3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20251228/data.parquet';
const META_S3 = 's3://quantlab-compact/exchange=binance/stream=bbo/symbol=btcusdt/date=20251228/meta.json';
const BATCH_SIZE = 5000;

async function verify() {
  console.log('=== FINAL REPLAY ENGINE VERIFICATION (v1.1) ===\n');
  console.log('DATASET (S3):');
  console.log(DATA_S3);
  console.log(META_S3);
  console.log('');

  const engine = new ReplayEngine(DATA_S3, META_S3);
  
  try {
    const meta = await engine.getMeta();
    console.log('Reading Parquet from S3 (DuckDB read_parquet)');

    let totalReplayed = 0;
    let outOfOrderCount = 0;
    let gapOrOverlapCount = 0;
    
    let lastTs = -1n;
    let lastSeq = -1n;
    
    let batchIndex = 0;
    let batchRows = 0;
    let batchFirst = null;
    let batchLast = null;

    const first5 = [];
    const last5 = [];

    const stream = engine.replay({ batchSize: BATCH_SIZE });

    for await (const row of stream) {
      const ts = BigInt(row.ts_event);
      const seq = BigInt(row.seq);

      // Collect samples
      if (totalReplayed < 5) {
        first5.push(`(${ts}, ${seq})`);
      }
      last5.push(`(${ts}, ${seq})`);
      if (last5.length > 5) last5.shift();

      // Order Check
      if (ts < lastTs || (ts === lastTs && seq <= lastSeq)) {
        outOfOrderCount++;
      }

      // Batch Tracking
      if (batchRows === 0) {
        batchFirst = { ts, seq };
      }
      batchLast = { ts, seq };
      batchRows++;

      if (batchRows === BATCH_SIZE) {
        console.log(`Batch ${batchIndex} | ${batchRows} | first: (${batchFirst.ts}, ${batchFirst.seq}) → last: (${batchLast.ts}, ${batchLast.seq})`);
        batchIndex++;
        batchRows = 0;
      }

      lastTs = ts;
      lastSeq = seq;
      totalReplayed++;
    }

    // Last batch
    if (batchRows > 0) {
      console.log(`Batch ${batchIndex} | ${batchRows} | first: (${batchFirst.ts}, ${batchFirst.seq}) → last: (${batchLast.ts}, ${batchLast.seq})`);
    }

    console.log('\n--- SAMPLES ---');
    console.log('FIRST 5:');
    console.log(first5.join('\n'));
    console.log('LAST 5:');
    console.log(last5.join('\n'));

    console.log('\n--- FINAL STATS ---');
    console.log(`total replayed: ${totalReplayed}`);
    console.log(`meta.rows:      ${meta.rows}`);
    console.log(`count match:    ${totalReplayed === meta.rows}`);
    console.log(`OUT_OF_ORDER:   ${outOfOrderCount}`);

    const pass = (totalReplayed === meta.rows) && (outOfOrderCount === 0);
    console.log(`\nRESULT: ${pass ? 'PASS' : 'FAIL'}`);

    if (!pass) process.exit(1);

  } catch (err) {
    console.error('\n[FATAL ERROR]', err.message);
    process.exit(1);
  } finally {
    await engine.close();
  }
}

verify();
