#!/usr/bin/env node
/**
 * QuantLab Replay Engine v1 — Example Usage
 * Usage: node replay/example.js <parquet_path> <meta_path>
 */

import { ReplayEngine } from './index.js';

async function main() {
  const [,, parquetPath, metaPath] = process.argv;

  if (!parquetPath || !metaPath) {
    console.error('Usage: node replay/example.js <parquet_path> <meta_path>');
    process.exit(1);
  }

  console.log('='.repeat(60));
  console.log('QuantLab Replay Engine v1');
  console.log('='.repeat(60));
  console.log(`Parquet: ${parquetPath}`);
  console.log(`Meta:    ${metaPath}`);
  console.log('');

  const engine = new ReplayEngine(parquetPath, metaPath);

  try {
    // Validate first
    const meta = await engine.validate();
    console.log('[META] Validated successfully');
    console.log(`  schema_version: ${meta.schema_version}`);
    console.log(`  rows: ${meta.rows.toLocaleString()}`);
    console.log(`  ts_event_min: ${meta.ts_event_min}`);
    console.log(`  ts_event_max: ${meta.ts_event_max}`);
    console.log('');

    // Replay with limit for demo (first 100 rows)
    console.log('[REPLAY] Starting replay (showing first 5 rows)...');
    let count = 0;
    const sampleRows = [];

    for await (const row of engine.replay({ batchSize: 1000 })) {
      count++;
      if (sampleRows.length < 5) {
        sampleRows.push(row);
      }
      // Stop after 100 for demo
      if (count >= 100) break;
    }

    console.log('');
    console.log('[SAMPLE] First 5 rows:');
    // BigInt replacer for JSON.stringify
    const replacer = (_, v) => typeof v === 'bigint' ? v.toString() : v;
    for (const row of sampleRows) {
      const preview = JSON.stringify(row, replacer).slice(0, 120);
      console.log(`  ts_event=${row.ts_event} | ${preview}...`);
    }

    console.log('');
    console.log(`[DONE] Replayed ${count} rows (limited to 100 for demo)`);

  } catch (err) {
    console.error('[ERROR]', err.message);
    process.exit(1);
  } finally {
    await engine.close();
  }
}

main();
