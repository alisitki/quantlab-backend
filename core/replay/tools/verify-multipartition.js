#!/usr/bin/env node
/**
 * QuantLab Replay Engine — Multi-Partition Determinism Test
 * 
 * Verifies that replaying a multi-partition dataset (multiple parquet files)
 * produces IDENTICAL results to replaying the same data as a single file.
 */

import { createHash } from 'node:crypto';
import { writeFile, mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import duckdb from 'duckdb';
import { ReplayEngine } from '../index.js';

const TEST_DIR = '/tmp/replay-multi-test';
const SINGLE_PARQUET = join(TEST_DIR, 'single.parquet');
const SINGLE_META = join(TEST_DIR, 'single.json');

const PART1_PARQUET = join(TEST_DIR, 'part1.parquet');
const PART2_PARQUET = join(TEST_DIR, 'part2.parquet');
const PART3_PARQUET = join(TEST_DIR, 'part3.parquet');

const PART1_META = join(TEST_DIR, 'part1.json');
const PART2_META = join(TEST_DIR, 'part2.json');
const PART3_META = join(TEST_DIR, 'part3.json');

/**
 * Generate synthetic test data and split into partitions
 */
async function setupTestData() {
  await rm(TEST_DIR, { recursive: true, force: true });
  await mkdir(TEST_DIR, { recursive: true });

  const db = new duckdb.Database(':memory:');
  const conn = db.connect();

  const runQuery = (sql) => new Promise((resolve, reject) => {
    conn.run(sql, (err) => err ? reject(err) : resolve());
  });

  console.log('Generating synthetic data (15,000 rows)...');
  
  // Create table with 15k rows
  await runQuery(`
    CREATE TABLE source AS 
    SELECT 
      (1700000000000000000 + i * 1000000)::BIGINT as ts_event,
      (i % 100)::INTEGER as seq,
      'BTCUSDT' as symbol,
      random() as price,
      random() * 10 as qty
    FROM range(0, 15000) t(i)
  `);

  // Export single file
  console.log('Exporting single.parquet...');
  await runQuery(`COPY source TO '${SINGLE_PARQUET}' (FORMAT PARQUET)`);
  
  // Export parts
  console.log('Exporting partitions (3 x 5000 rows)...');
  const baseTs = 1700000000000000000n;
  const part1Boundary = (baseTs + 5000n * 1000000n).toString();
  const part2Boundary = (baseTs + 10000n * 1000000n).toString();

  await runQuery(`COPY (SELECT * FROM source WHERE ts_event < ${part1Boundary}) TO '${PART1_PARQUET}' (FORMAT PARQUET)`);
  await runQuery(`COPY (SELECT * FROM source WHERE ts_event >= ${part1Boundary} AND ts_event < ${part2Boundary}) TO '${PART2_PARQUET}' (FORMAT PARQUET)`);
  await runQuery(`COPY (SELECT * FROM source WHERE ts_event >= ${part2Boundary}) TO '${PART3_PARQUET}' (FORMAT PARQUET)`);



  // Meta generation helper
  const writeMeta = async (path, rows, min, max) => {
    await writeFile(path, JSON.stringify({
      schema_version: 1,
      stream_type: 'test_bbo',
      ordering_columns: ['ts_event', 'seq'],
      rows,
      ts_event_min: min,
      ts_event_max: max
    }));
  };

  await writeMeta(SINGLE_META, 15000, 1700000000000000000, 1700000000000000000 + 14999 * 1000000);
  await writeMeta(PART1_META, 5000, 1700000000000000000, 1700000000000000000 + 4999 * 1000000);
  await writeMeta(PART2_META, 5000, 1700000000000000000 + 5000 * 1000000, 1700000000000000000 + 9999 * 1000000);
  await writeMeta(PART3_META, 5000, 1700000000000000000 + 10000 * 1000000, 1700000000000000000 + 14999 * 1000000);

  db.close();
}

function sha256(data) {
  return createHash('sha256').update(data).digest('hex');
}

async function runReplay(parquet, meta) {
  const engine = new ReplayEngine(parquet, meta);
  const hashes = [];
  
  try {
    await engine.validate();
    for await (const row of engine.replay({ batchSize: 2000 })) {
      // canonical string for row
      const str = JSON.stringify(row, (_, v) => typeof v === 'bigint' ? v.toString() : v);
      hashes.push(sha256(str));
    }
  } finally {
    await engine.close();
  }
  
  return sha256(hashes.join(''));
}

async function main() {
  console.log('=== MULTI-PARTITION DETERMINISM TEST ===');
  await setupTestData();

  console.log('\nRunning Single-File Replay...');
  const singleHash = await runReplay(SINGLE_PARQUET, SINGLE_META);
  console.log(`SINGLE_HASH: ${singleHash}`);

  console.log('\nRunning Multi-Partition Replay (3 files)...');
  const multiHash = await runReplay(
    [PART1_PARQUET, PART2_PARQUET, PART3_PARQUET],
    [PART1_META, PART2_META, PART3_META]
  );
  console.log(`MULTI_HASH:  ${multiHash}`);

  console.log('\n--- RESULT ---');
  const match = singleHash === multiHash;
  console.log(`HASH_MATCH: ${match ? 'true ✓' : 'false ✗'}`);

  if (!match) {
    console.error('FAIL: Multi-partition replay is NOT deterministic vs single-file.');
    process.exit(1);
  } else {
    console.log('PASS: Replay is deterministic regardless of partitioning.');
  }

  // Final cleanup
  await rm(TEST_DIR, { recursive: true, force: true });
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
