#!/usr/bin/env node
/**
 * Replay Engine v1 — Hard Verification Script
 * Generates evidence for all verification requirements
 */

import { readFile } from 'node:fs/promises';
import { ReplayEngine } from './index.js';

const PARQUET_PATH = '/tmp/replay-test/data.parquet';
const META_PATH = '/tmp/replay-test/meta.json';
const BATCH_SIZE = 5000;

// BigInt-safe JSON replacer
const replacer = (_, v) => typeof v === 'bigint' ? v.toString() : v;

async function main() {
  // ==========================================================================
  // A) META CONTENT
  // ==========================================================================
  console.log('=== META CONTENT ===');
  const metaRaw = await readFile(META_PATH, 'utf-8');
  console.log(metaRaw);
  
  const meta = JSON.parse(metaRaw);
  console.log('--- EXTRACTED VALUES ---');
  console.log(`schema_version: ${meta.schema_version}`);
  console.log(`rows: ${meta.rows}`);
  console.log(`ts_event_min: ${meta.ts_event_min}`);
  console.log(`ts_event_max: ${meta.ts_event_max}`);
  console.log('');

  // ==========================================================================
  // B) REPLAY HEAD (FIRST 10 EVENTS)
  // ==========================================================================
  console.log('=== REPLAY HEAD (FIRST 10 EVENTS) ===');
  {
    const engine = new ReplayEngine(PARQUET_PATH, META_PATH);
    await engine.validate();
    let count = 0;
    for await (const row of engine.replay({ batchSize: 100 })) {
      console.log(JSON.stringify(row, replacer));
      count++;
      if (count >= 10) break;
    }
    await engine.close();
  }
  console.log('');

  // ==========================================================================
  // REPLAY TAIL (LAST 10 EVENTS)
  // ==========================================================================
  console.log('=== REPLAY TAIL (LAST 10 EVENTS) ===');
  {
    const engine = new ReplayEngine(PARQUET_PATH, META_PATH);
    await engine.validate();
    const buffer = [];
    for await (const row of engine.replay({ batchSize: 10000 })) {
      buffer.push(row);
      if (buffer.length > 10) buffer.shift();
    }
    for (const row of buffer) {
      console.log(JSON.stringify(row, replacer));
    }
    await engine.close();
  }
  console.log('');

  // ==========================================================================
  // C) BATCH LOGS
  // ==========================================================================
  console.log('=== BATCH LOGS ===');
  {
    const engine = new ReplayEngine(PARQUET_PATH, META_PATH);
    await engine.validate();
    
    let batchIndex = 0;
    let batchCount = 0;
    let firstTs = null;
    let lastTs = null;
    let totalRows = 0;

    for await (const row of engine.replay({ batchSize: BATCH_SIZE })) {
      if (batchCount === 0) firstTs = row.ts_event;
      lastTs = row.ts_event;
      batchCount++;
      totalRows++;

      if (batchCount === BATCH_SIZE) {
        console.log(`Batch ${batchIndex} | rows=${batchCount} | ts_event: ${firstTs} → ${lastTs}`);
        batchIndex++;
        batchCount = 0;
        firstTs = null;
        lastTs = null;
      }
    }
    // Final partial batch
    if (batchCount > 0) {
      console.log(`Batch ${batchIndex} | rows=${batchCount} | ts_event: ${firstTs} → ${lastTs}`);
    }
    
    console.log('');
    console.log('=== TOTAL ROW COUNT ===');
    console.log(`Replayed rows: ${totalRows}`);
    console.log(`meta.rows: ${meta.rows}`);
    console.log(`Match: ${totalRows === meta.rows}`);
    
    await engine.close();
  }
  console.log('');

  // ==========================================================================
  // E) TIME FILTER TEST
  // ==========================================================================
  console.log('=== TIME FILTER TEST ===');
  {
    const delta = Math.floor((meta.ts_event_max - meta.ts_event_min) * 0.1); // 10% margin
    
    // Test 1: startTs filter
    console.log('--- TEST 1: startTs filter ---');
    const startTs = meta.ts_event_min + delta;
    console.log(`Filter: startTs = ${startTs} (min + ${delta})`);
    {
      const engine = new ReplayEngine(PARQUET_PATH, META_PATH);
      await engine.validate();
      let count = 0;
      let first = null;
      let last = null;
      for await (const row of engine.replay({ batchSize: 10000, startTs })) {
        if (!first) first = row.ts_event;
        last = row.ts_event;
        count++;
      }
      console.log(`First ts_event: ${first}`);
      console.log(`Last ts_event: ${last}`);
      console.log(`Total rows: ${count}`);
      console.log(`First >= startTs: ${Number(first) >= startTs}`);
      await engine.close();
    }
    console.log('');

    // Test 2: endTs filter
    console.log('--- TEST 2: endTs filter ---');
    const endTs = meta.ts_event_max - delta;
    console.log(`Filter: endTs = ${endTs} (max - ${delta})`);
    {
      const engine = new ReplayEngine(PARQUET_PATH, META_PATH);
      await engine.validate();
      let count = 0;
      let first = null;
      let last = null;
      for await (const row of engine.replay({ batchSize: 10000, endTs })) {
        if (!first) first = row.ts_event;
        last = row.ts_event;
        count++;
      }
      console.log(`First ts_event: ${first}`);
      console.log(`Last ts_event: ${last}`);
      console.log(`Total rows: ${count}`);
      console.log(`Last <= endTs: ${Number(last) <= endTs}`);
      await engine.close();
    }
  }
  console.log('');

  // ==========================================================================
  // F) HARD FAIL TESTS
  // ==========================================================================
  console.log('=== HARD FAIL TESTS ===');
  
  // Test 1: Wrong schema_version
  console.log('--- TEST 1: schema_version = 2 ---');
  {
    const { writeFile } = await import('node:fs/promises');
    const badMeta = { ...meta, schema_version: 2 };
    const badMetaPath = '/tmp/replay-test/meta-bad-schema.json';
    await writeFile(badMetaPath, JSON.stringify(badMeta, null, 2));
    
    const engine = new ReplayEngine(PARQUET_PATH, badMetaPath);
    try {
      await engine.validate();
      console.log('ERROR: Should have thrown');
    } catch (err) {
      console.log(`Error: ${err.message}`);
    }
    await engine.close();
  }
  console.log('');

  // Test 2: Wrong rows count
  console.log('--- TEST 2: meta.rows = 999 (incorrect) ---');
  {
    const { writeFile } = await import('node:fs/promises');
    const badMeta = { ...meta, rows: 999 };
    const badMetaPath = '/tmp/replay-test/meta-bad-rows.json';
    await writeFile(badMetaPath, JSON.stringify(badMeta, null, 2));
    
    const engine = new ReplayEngine(PARQUET_PATH, badMetaPath);
    try {
      await engine.validate();
      console.log('ERROR: Should have thrown');
    } catch (err) {
      console.log(`Error: ${err.message}`);
    }
    await engine.close();
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
