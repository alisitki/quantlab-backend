#!/usr/bin/env node

/**
 * Strategy Factory CLI Tool
 *
 * Generates strategies from validated edges.
 * Backtests and deploys strategies to lifecycle manager.
 *
 * Usage:
 *   node tools/run-strategy-factory.js \
 *     --edges-file=/path/to/edges-validated-*.json \
 *     --parquet=/path/to/data.parquet \
 *     --symbol=ADA/USDT \
 *     [--meta=/path/to/meta.json] \
 *     [--lifecycle-dir=data/lifecycle] \
 *     [--output-dir=data/pipeline-output]
 */

import { StrategyFactory } from '../core/strategy/factory/StrategyFactory.js';
import { StrategyLifecycleManager } from '../core/strategy/lifecycle/StrategyLifecycleManager.js';
import { EdgeSerializer } from '../core/edge/EdgeSerializer.js';
import fs from 'node:fs/promises';
import path from 'node:path';

// Parse CLI args
function parseArgs(argv) {
  const args = {};
  for (const arg of argv.slice(2)) {
    if (arg.startsWith('--')) {
      const [key, value] = arg.substring(2).split('=');
      args[key] = value;
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

  // Validate required args
  if (!args['edges-file'] || !args.parquet || !args.symbol) {
    console.error('Usage: node run-strategy-factory.js --edges-file=<file> --parquet=<path> --symbol=<SYMBOL> [options]');
    console.error('');
    console.error('Required:');
    console.error('  --edges-file   Path to edges-validated-*.json');
    console.error('  --parquet      Path to parquet file (for backtest)');
    console.error('  --symbol       Symbol (e.g., ADA/USDT)');
    console.error('');
    console.error('Optional:');
    console.error('  --meta         Path to meta.json (default: <parquet>.meta.json)');
    console.error('  --lifecycle-dir Lifecycle state directory (default: data/lifecycle)');
    console.error('  --output-dir   Output directory (default: data/pipeline-output)');
    process.exit(1);
  }

  // Default values
  const metaPath = args.meta || args.parquet.replace('.parquet', '.parquet.meta.json');
  const lifecycleDir = args['lifecycle-dir'] || 'data/lifecycle';
  const outputDir = args['output-dir'] || 'data/pipeline-output';

  console.log('='.repeat(80));
  console.log('STRATEGY FACTORY PIPELINE');
  console.log('='.repeat(80));
  console.log(`Edges File:     ${args['edges-file']}`);
  console.log(`Parquet:        ${args.parquet}`);
  console.log(`Meta:           ${metaPath}`);
  console.log(`Symbol:         ${args.symbol}`);
  console.log(`Lifecycle Dir:  ${lifecycleDir}`);
  console.log(`Output Dir:     ${outputDir}`);
  console.log('='.repeat(80));

  try {
    // Load validated edges
    const serializer = new EdgeSerializer();
    const registry = await serializer.loadFromFile(args['edges-file']);

    const validatedEdges = registry.getByStatus('VALIDATED');

    if (validatedEdges.length === 0) {
      console.log('');
      console.log('='.repeat(80));
      console.log(JSON.stringify({
        event: 'factory_skip',
        reason: 'no_validated_edges',
        totalEdges: registry.size()
      }, null, 2));
      console.log('='.repeat(80));
      process.exit(0);
    }

    console.log(`Found ${validatedEdges.length} VALIDATED edges`);
    console.log('');

    // Initialize lifecycle manager
    const lifecycleManager = new StrategyLifecycleManager(lifecycleDir);

    // Try to restore existing lifecycle state
    console.log('Restoring lifecycle state...');
    try {
      await lifecycleManager.restore();
      const summary = lifecycleManager.getSummary();
      console.log(`Restored ${summary.totalStrategies} existing strategies`);
    } catch (err) {
      console.log('No existing lifecycle state (first run)');
    }

    // Connect EdgeRegistry for health monitoring
    lifecycleManager.connectEdgeRegistry(registry);

    console.log('');

    // Create factory with lifecycle integration
    const factory = new StrategyFactory({
      registry,
      dataConfig: {
        parquetPath: args.parquet,
        metaPath,
        symbol: args.symbol
      }
    });

    // Wire deployer to lifecycle manager
    factory.deployer.lifecycleManager = lifecycleManager;

    // Produce strategies for all validated edges
    const startTime = Date.now();
    const results = await factory.produceAll();
    const duration = Date.now() - startTime;

    // Persist lifecycle state
    console.log('');
    console.log('Persisting lifecycle state...');
    await lifecycleManager.persist();

    // Save results
    await fs.mkdir(outputDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

    const reportFile = path.join(outputDir, `factory-report-${timestamp}.json`);
    const report = {
      step: 'factory',
      timestamp: new Date().toISOString(),
      symbol: args.symbol,
      parquetPath: args.parquet,
      duration,
      results: results.map(r => ({
        strategyId: r.strategyId,
        edgeId: r.edgeId,
        templateType: r.templateType,
        status: r.status,
        backtestResult: r.backtestResult ? {
          trades: r.backtestResult.trades,
          returnPct: r.backtestResult.returnPct,
          sharpe: r.backtestResult.sharpe,
          maxDrawdownPct: r.backtestResult.maxDrawdownPct,
          winRate: r.backtestResult.winRate,
          passed: r.backtestResult.passed
        } : null,
        error: r.error
      })),
      lifecycleSummary: lifecycleManager.getSummary()
    };

    await writeJSON(reportFile, report);

    // Console summary
    const deployed = results.filter(r => r.status === 'DEPLOYED').length;
    const failed = results.filter(r => r.status === 'BACKTEST_FAILED').length;
    const errors = results.filter(r => r.status === 'ERROR').length;

    console.log('');
    console.log('='.repeat(80));
    console.log(JSON.stringify({
      event: 'factory_complete',
      deployed,
      backtestFailed: failed,
      errors,
      durationMs: duration,
      lifecycleSummary: lifecycleManager.getSummary(),
      outputFiles: {
        report: reportFile,
        lifecycleState: path.join(lifecycleDir, 'lifecycle-state.json')
      }
    }, null, 2));
    console.log('='.repeat(80));

  } catch (error) {
    console.error('');
    console.error('='.repeat(80));
    console.error(JSON.stringify({
      event: 'factory_error',
      error: error.message,
      stack: error.stack
    }, null, 2));
    console.error('='.repeat(80));
    process.exit(1);
  }
}

main();
