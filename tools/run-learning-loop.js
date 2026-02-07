#!/usr/bin/env node

/**
 * Learning Loop CLI Tool
 *
 * Runs closed-loop learning cycles:
 * - Daily: Read outcomes → Update confidence → Detect drift
 * - Weekly: Daily + Re-validate all VALIDATED edges
 *
 * Usage:
 *   node tools/run-learning-loop.js \
 *     --mode=daily|weekly \
 *     --edges-file=/path/to/edges-validated-*.json \
 *     --outcomes-dir=data/learning/outcomes \
 *     [--parquet=/path/to/data.parquet]  # Required for weekly mode
 *     [--symbol=ADA/USDT]                 # Required for weekly mode
 *     [--meta=/path/to/meta.json] \
 *     [--output-dir=data/learning/reports]
 */

import { LearningScheduler } from '../core/learning/LearningScheduler.js';
import { TradeOutcomeCollector } from '../core/learning/TradeOutcomeCollector.js';
import { EdgeConfidenceUpdater } from '../core/learning/EdgeConfidenceUpdater.js';
import { EdgeRevalidationRunner } from '../core/learning/EdgeRevalidationRunner.js';
import { EdgeValidationPipeline } from '../core/edge/validation/EdgeValidationPipeline.js';
import { DiscoveryDataLoader } from '../core/edge/discovery/DiscoveryDataLoader.js';
import { EdgeSerializer } from '../core/edge/EdgeSerializer.js';
import fs from 'node:fs/promises';
import path from 'node:path';

// Parse CLI args
function parseArgs(argv) {
  const args = {};
  for (const arg of argv.slice(2)) {
    if (arg.startsWith('--')) {
      const match = arg.substring(2).match(/^([^=]+)(?:=(.*))?$/);
      if (match) {
        const [, key, value] = match;
        args[key] = value === undefined ? true : value;
      }
    }
  }
  return args;
}

// Atomic JSON write
async function writeJSON(filePath, data) {
  const content = JSON.stringify(data, null, 2);
  const tmpPath = `${filePath}.tmp`;

  const fileHandle = await fs.open(tmpPath, 'w');
  await fileHandle.write(content);
  await fileHandle.sync();
  await fileHandle.close();

  await fs.rename(tmpPath, filePath);
}

async function main() {
  const args = parseArgs(process.argv);
  const startTime = Date.now();

  // Validate required args
  if (!args.mode || !args['edges-file'] || !args['outcomes-dir']) {
    console.error('Usage: node run-learning-loop.js --mode=<daily|weekly> --edges-file=<file> --outcomes-dir=<dir> [options]');
    console.error('');
    console.error('Required:');
    console.error('  --mode          Learning mode (daily or weekly)');
    console.error('  --edges-file    Path to edges-validated-*.json');
    console.error('  --outcomes-dir  Directory containing outcome JSONL files');
    console.error('');
    console.error('Required for weekly:');
    console.error('  --parquet       Path to parquet file (for re-validation dataset)');
    console.error('  --symbol        Symbol (e.g., ADA/USDT)');
    console.error('');
    console.error('Optional:');
    console.error('  --meta          Path to meta.json (default: <parquet>.meta.json)');
    console.error('  --output-dir    Output directory (default: data/learning/reports)');
    process.exit(1);
  }

  const mode = args.mode.toLowerCase();
  if (mode !== 'daily' && mode !== 'weekly') {
    console.error(`Invalid mode: ${args.mode}. Must be 'daily' or 'weekly'.`);
    process.exit(1);
  }

  // Weekly mode requires parquet and symbol
  if (mode === 'weekly' && (!args.parquet || !args.symbol)) {
    console.error('Weekly mode requires --parquet and --symbol');
    process.exit(1);
  }

  // Default values
  const outputDir = args['output-dir'] || 'data/learning/reports';
  const metaPath = args.meta || (args.parquet ? args.parquet.replace('.parquet', '.parquet.meta.json') : null);

  console.log('='.repeat(80));
  console.log(`LEARNING LOOP - ${mode.toUpperCase()} MODE`);
  console.log('='.repeat(80));
  console.log(`Edges File:     ${args['edges-file']}`);
  console.log(`Outcomes Dir:   ${args['outcomes-dir']}`);
  console.log(`Output Dir:     ${outputDir}`);

  if (mode === 'weekly') {
    console.log(`Parquet:        ${args.parquet}`);
    console.log(`Meta:           ${metaPath}`);
    console.log(`Symbol:         ${args.symbol}`);
  }

  console.log('='.repeat(80));

  try {
    // Load edge registry
    console.log('');
    console.log('Loading edge registry...');
    const serializer = new EdgeSerializer();
    const registry = await serializer.loadFromFile(args['edges-file']);

    console.log(`Loaded ${registry.size()} edges`);

    // Initialize components
    const collector = new TradeOutcomeCollector({
      logDir: args['outcomes-dir']
    });

    const updater = new EdgeConfidenceUpdater(registry);

    // Set baselines from current edge stats
    console.log('');
    console.log('Setting confidence baselines...');
    const allEdges = registry.getAll();
    for (const edge of allEdges) {
      const winRate = edge.stats.trades > 0
        ? edge.stats.wins / edge.stats.trades
        : 0.5;

      updater.setBaseline(edge.id, {
        confidence: edge.confidence.score,
        winRate,
        sharpe: edge.expectedAdvantage?.sharpe || 0
      });
    }

    console.log(`Set baselines for ${allEdges.length} edges`);

    let dataset = null;
    let runner = null;

    // Load dataset for weekly mode
    if (mode === 'weekly') {
      console.log('');
      console.log('Loading validation dataset...');

      const loader = new DiscoveryDataLoader();
      dataset = await loader.load({
        parquetPath: args.parquet,
        metaPath,
        symbol: args.symbol
      });

      console.log(`Loaded ${dataset.rows.length} rows`);

      const validationPipeline = new EdgeValidationPipeline({ registry });
      runner = new EdgeRevalidationRunner({
        edgeRegistry: registry,
        validationPipeline
      });
    }

    // Create scheduler
    const scheduler = new LearningScheduler({
      edgeRegistry: registry,
      confidenceUpdater: updater,
      revalidationRunner: runner,
      outcomeCollector: collector
    });

    // Run learning loop
    console.log('');
    console.log(`Running ${mode} learning loop...`);
    console.log('-'.repeat(80));

    let result;

    if (mode === 'daily') {
      result = await scheduler.runDaily();
    } else {
      result = await scheduler.runWeekly(dataset);
    }

    // Close collector
    await collector.close();

    // Save updated registry
    console.log('');
    console.log('Saving updated edge registry...');

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const updatedEdgesFile = path.join(outputDir, `edges-after-learning-${timestamp}.json`);

    await fs.mkdir(outputDir, { recursive: true });
    await serializer.saveToFile(updatedEdgesFile, registry);

    console.log(`Saved to ${updatedEdgesFile}`);

    // Save learning report
    const reportFile = path.join(outputDir, `learning-report-${timestamp}.json`);
    const report = {
      mode,
      timestamp: new Date().toISOString(),
      edgesFile: args['edges-file'],
      outcomesDir: args['outcomes-dir'],
      result,
      durationMs: Date.now() - startTime,
      outputFiles: {
        updatedEdges: updatedEdgesFile,
        report: reportFile
      }
    };

    await writeJSON(reportFile, report);

    console.log(`Report saved to ${reportFile}`);

    // Console summary
    console.log('');
    console.log('='.repeat(80));
    console.log('LEARNING LOOP COMPLETE');
    console.log('='.repeat(80));

    if (mode === 'daily') {
      console.log(JSON.stringify({
        event: 'learning_daily_complete',
        outcomesProcessed: result.outcomesProcessed,
        alertsGenerated: result.alertsGenerated,
        edgesAffected: result.edgesAffected || 0,
        revalidationFlags: result.revalidationFlags || 0,
        durationMs: Date.now() - startTime
      }, null, 2));
    } else {
      console.log(JSON.stringify({
        event: 'learning_weekly_complete',
        daily: {
          outcomesProcessed: result.daily.outcomesProcessed,
          alertsGenerated: result.daily.alertsGenerated
        },
        revalidation: {
          edgesRevalidated: result.revalidation.edgesRevalidated,
          statusChanges: result.revalidation.statusChanges,
          validated: result.revalidation.validated,
          rejected: result.revalidation.rejected
        },
        durationMs: Date.now() - startTime
      }, null, 2));
    }

    console.log('='.repeat(80));

  } catch (error) {
    console.error('');
    console.error('='.repeat(80));
    console.error(JSON.stringify({
      event: 'learning_loop_error',
      error: error.message,
      stack: error.stack
    }, null, 2));
    console.error('='.repeat(80));
    process.exit(1);
  }
}

main();
