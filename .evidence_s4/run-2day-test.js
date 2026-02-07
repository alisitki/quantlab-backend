#!/usr/bin/env node
/**
 * 2-Day Smoke Test for Sprint-4 Validation
 */

import { spawn } from 'node:child_process';
import v8 from 'node:v8';
import { EdgeDiscoveryPipeline } from '../core/edge/discovery/EdgeDiscoveryPipeline.js';
import { EdgeRegistry } from '../core/edge/EdgeRegistry.js';
import { writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';

// Heap check
const REQUIRED_HEAP_MB = 6144;
const heapStats = v8.getHeapStatistics();
const currentHeapLimitMB = Math.floor(heapStats.heap_size_limit / 1024 / 1024);

console.log(`[Heap Check] Current heap limit: ${currentHeapLimitMB} MB`);

if (currentHeapLimitMB < REQUIRED_HEAP_MB) {
  console.log(`[Heap Check] ⚠️  Heap limit too low (${currentHeapLimitMB} MB < ${REQUIRED_HEAP_MB} MB required)`);
  console.log(`[Heap Check] 🔄 Auto re-exec with --max-old-space-size=${REQUIRED_HEAP_MB}...`);

  const child = spawn(
    process.execPath,
    ['--expose-gc', `--max-old-space-size=${REQUIRED_HEAP_MB}`, ...process.argv.slice(1)],
    {
      stdio: 'inherit',
      env: { ...process.env, NODE_OPTIONS: `--max-old-space-size=${REQUIRED_HEAP_MB}` }
    }
  );

  child.on('exit', (code) => {
    process.exit(code || 0);
  });
} else {
  console.log(`[Heap Check] ✅ Heap limit adequate (${currentHeapLimitMB} MB >= ${REQUIRED_HEAP_MB} MB)`);
  console.log('');
}

// 2-DAY TEST
const FILES = [
  '20260108',   // Day 1: 3.2M rows
  '20260109'    // Day 2: 3.0M rows (approx)
].map(date => ({
  parquetPath: `data/sprint2/adausdt_${date}.parquet`,
  metaPath: `data/sprint2/adausdt_${date}_meta.json`
}));

const SYMBOL = 'ADA/USDT';
const OUTPUT_DIR = 'runs/sprint4-2day-smoke';

async function main() {
  console.log('=== Sprint-4 2-Day Smoke Test ===\n');
  console.log(`Goal: Multi-day iterator stability validation`);
  console.log(`Symbol: ${SYMBOL}`);
  console.log(`Days: ${FILES.length}`);
  console.log(`Output: ${OUTPUT_DIR}\n`);

  // Verify files
  console.log('Verifying files...');
  for (const file of FILES) {
    if (!existsSync(file.parquetPath)) {
      console.error(`✗ Missing: ${file.parquetPath}`);
      process.exit(1);
    }
    if (!existsSync(file.metaPath)) {
      console.error(`✗ Missing: ${file.metaPath}`);
      process.exit(1);
    }
    console.log(`  ✓ ${file.parquetPath.split('/').pop()}`);
  }
  console.log('');

  await mkdir(OUTPUT_DIR, { recursive: true });

  const registry = new EdgeRegistry();
  const pipeline = new EdgeDiscoveryPipeline({ registry });

  console.log('Running 2-day discovery (STREAMING)...');
  console.log('');
  const startTime = Date.now();

  let result;
  try {
    result = await pipeline.runMultiDayStreaming(FILES, SYMBOL);
  } catch (err) {
    console.error('\n❌ Discovery failed:', err.message);
    console.error(err.stack);
    process.exit(1);
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n✓ Discovery complete (${durationSec}s)\n`);

  // Summary
  console.log('=== 2-DAY SMOKE TEST SUMMARY ===\n');
  console.log(`Patterns Scanned:          ${result.totalPatternsScanned || 0}`);
  console.log(`Edge Candidates Generated: ${result.edgeCandidates?.length || 0}`);
  console.log(`Data Rows Processed:       ${result.totalRowsProcessed?.toLocaleString() || 'N/A'}`);
  console.log(`Files Loaded:              ${FILES.length}`);
  console.log(`Duration:                  ${durationSec}s`);

  const reportPath = `${OUTPUT_DIR}/smoke-report-${new Date().toISOString().split('.')[0].replace(/:/g, '-')}.json`;
  await writeFile(reportPath, JSON.stringify({
    test: '2-day-smoke',
    files: FILES.map(f => f.parquetPath),
    summary: result,
    durationSec: parseFloat(durationSec)
  }, null, 2));

  console.log(`\nReport saved: ${reportPath}`);
  console.log('\n=== Sprint-4 2-Day Smoke Test Complete ===\n');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
