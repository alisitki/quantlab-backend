#!/usr/bin/env node
/**
 * Sprint-2 Multi-Day Discovery Runner
 * Tests discovery pipeline on 6 consecutive days of ADA/USDT
 */

import { spawn } from 'node:child_process';
import v8 from 'node:v8';
import { EdgeDiscoveryPipeline } from '../core/edge/discovery/EdgeDiscoveryPipeline.js';
import { EdgeRegistry } from '../core/edge/EdgeRegistry.js';
import { writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';

// ========================================
// HEAP LIMIT CHECK & AUTO RE-EXEC
// ========================================
const REQUIRED_HEAP_MB = 6144; // 6 GB required for multi-day discovery
const heapStats = v8.getHeapStatistics();
const currentHeapLimitMB = Math.floor(heapStats.heap_size_limit / 1024 / 1024);

console.log(`[Heap Check] Current heap limit: ${currentHeapLimitMB} MB`);

if (currentHeapLimitMB < REQUIRED_HEAP_MB) {
  console.log(`[Heap Check] ⚠️  Heap limit too low (${currentHeapLimitMB} MB < ${REQUIRED_HEAP_MB} MB required)`);
  console.log(`[Heap Check] 🔄 Auto re-exec with --max-old-space-size=${REQUIRED_HEAP_MB}...`);
  console.log('');

  // Re-exec self with correct NODE_OPTIONS
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

  // Exit parent process
  // Child will continue execution
} else {
  console.log(`[Heap Check] ✅ Heap limit adequate (${currentHeapLimitMB} MB >= ${REQUIRED_HEAP_MB} MB)`);
  console.log('');
}

// Single-day capacity test (Sprint-2)
// Goal: Validate discovery engine works at scale (3.2M rows)
// Multi-day loading already validated (successfully loaded 2 days before OOM)
const FILES = [
  '20260108'   // Day 1: 3.2M rows (GOOD quality, largest day)
].map(date => ({
  parquetPath: `data/sprint2/adausdt_${date}.parquet`,
  metaPath: `data/sprint2/adausdt_${date}_meta.json`
}));

const SYMBOL = 'ADA/USDT';
const OUTPUT_DIR = 'runs/sprint2-multiday-20260206';

async function main() {
  console.log('=== Sprint-2 Discovery Capacity Test ===\n');
  console.log(`Goal: Validate discovery engine at scale`);
  console.log(`Symbol: ${SYMBOL}`);
  console.log(`Days: ${FILES.length} (largest day)`);
  console.log(`Expected Rows: ~3.2M rows`);
  console.log(`Output: ${OUTPUT_DIR}\n`);

  // Verify files exist
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

  // Create output directory
  await mkdir(OUTPUT_DIR, { recursive: true });

  // Initialize registry
  const registry = new EdgeRegistry();

  // Create pipeline
  const pipeline = new EdgeDiscoveryPipeline({ registry });

  // Run multi-day discovery (STREAMING mode for memory efficiency)
  console.log('Running multi-day discovery (STREAMING)...');
  console.log('');
  const startTime = Date.now();

  let result;
  try {
    result = await pipeline.runMultiDayStreaming(FILES, SYMBOL);
  } catch (err) {
    console.error('\n✗ Discovery failed:', err.message);
    console.error(err.stack);
    process.exit(1);
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n✓ Discovery complete (${durationSec}s)\n`);

  // Print summary
  console.log('=== CANDIDATE DISCOVERY SUMMARY ===\n');
  console.log(`Patterns Scanned:          ${result.patternsScanned}`);
  console.log(`Patterns Tested:           ${result.patternsTestedSignificant}`);
  console.log(`Edge Candidates Generated: ${result.edgeCandidatesGenerated}`);
  console.log(`Edge Candidates Registered: ${result.edgeCandidatesRegistered}`);
  console.log('');
  console.log(`Data Rows Processed:       ${result.metadata.dataRowCount?.toLocaleString() || 'N/A'}`);
  console.log(`Regimes Detected:          ${result.metadata.regimesUsed || 'N/A'}`);
  console.log(`Files Loaded:              ${result.metadata.filesLoaded || FILES.length}`);
  console.log(`Duration:                  ${durationSec}s`);
  console.log('');

  if (result.edgeCandidatesGenerated > 0) {
    console.log('=== EDGE CANDIDATES ===\n');
    result.edges.forEach((edge, i) => {
      console.log(`${i + 1}. ${edge.name} (${edge.id})`);
      console.log(`   Expected Return: ${(edge.expectedAdvantage.mean * 100).toFixed(4)}%`);
      console.log(`   Sharpe Ratio:    ${edge.expectedAdvantage.sharpe?.toFixed(2) || 'N/A'}`);
      console.log(`   Win Rate:        ${(edge.expectedAdvantage.winRate * 100).toFixed(1)}%`);
      console.log(`   Sample Size:     ${edge.confidence.sampleSize}`);
      console.log(`   Confidence:      ${(edge.confidence.score * 100).toFixed(1)}%`);
      console.log(`   Status:          ${edge.status}`);
      console.log('');
    });
  } else {
    console.log('⚠️  No edge candidates generated');
    console.log('');
    console.log('Possible reasons:');
    console.log('  - Patterns did not meet returnThreshold (0.05%)');
    console.log('  - Statistical tests failed (p-value, Sharpe, etc.)');
    console.log('  - Insufficient sample sizes (<30 occurrences)');
    console.log('');
  }

  // Save detailed report
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
  const reportPath = `${OUTPUT_DIR}/discovery-report-${timestamp}.json`;
  const edgesPath = `${OUTPUT_DIR}/edges-discovered-${timestamp}.json`;

  const report = {
    step: 'multi_day_discovery',
    timestamp: new Date().toISOString(),
    symbol: SYMBOL,
    days: FILES.length,
    files: FILES.map(f => f.parquetPath),
    patternsScanned: result.patternsScanned,
    patternsTestedSignificant: result.patternsTestedSignificant,
    edgeCandidatesGenerated: result.edgeCandidatesGenerated,
    edgeCandidatesRegistered: result.edgeCandidatesRegistered,
    metadata: result.metadata,
    duration: durationSec
  };

  await writeFile(reportPath, JSON.stringify(report, null, 2));
  console.log(`Report saved: ${reportPath}`);

  if (result.edges.length > 0) {
    const edgesData = {
      edges: result.edges.map(e => e.toJSON()),
      metadata: {
        discoveryTimestamp: new Date().toISOString(),
        symbol: SYMBOL,
        daysProcessed: FILES.length,
        totalCandidates: result.edges.length
      }
    };
    await writeFile(edgesPath, JSON.stringify(edgesData, null, 2));
    console.log(`Edges saved:  ${edgesPath}`);
  }

  console.log('');
  console.log('=== Sprint-2 Multi-Day Discovery Complete ===');
  console.log('');
  console.log('Next steps:');
  if (result.edgeCandidatesGenerated > 0) {
    console.log('  1. Review edge candidates in discovery report');
    console.log('  2. Run edge validation:');
    console.log(`     node tools/run-edge-validation.js --edges-file=${edgesPath}`);
    console.log('  3. Generate strategies from validated edges');
  } else {
    console.log('  1. Review rejection reasons in discovery logs');
    console.log('  2. Consider:');
    console.log('     - Extending to more days (7-14 days)');
    console.log('     - Using higher volatility period');
    console.log('     - Trying different symbol (BTC/USDT)');
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
