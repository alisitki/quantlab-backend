#!/usr/bin/env node

/**
 * Full Edge Discovery Pipeline
 *
 * Runs all pipeline steps end-to-end:
 * 1. Discovery - Find edge candidates
 * 2. Validation - Statistically validate edges
 * 3. Factory - Generate strategies from validated edges
 * 4. Persist - Save all state to disk
 *
 * Usage:
 *   node tools/run-full-pipeline.js \
 *     --parquet=/path/to/data.parquet \
 *     --symbol=ADA/USDT \
 *     [--meta=/path/to/meta.json] \
 *     [--output-dir=data/pipeline-output] \
 *     [--lifecycle-dir=data/lifecycle] \
 *     [--max-edges=20] \
 *     [--seed=42] \
 *     [--dry-run]
 *
 * Flags:
 *   --dry-run  Run discovery + validation only (skip factory/lifecycle)
 */

import { EdgeDiscoveryPipeline } from '../core/edge/discovery/EdgeDiscoveryPipeline.js';
import { EdgeValidationPipeline } from '../core/edge/validation/EdgeValidationPipeline.js';
import { DiscoveryDataLoader } from '../core/edge/discovery/DiscoveryDataLoader.js';
import { StrategyFactory } from '../core/strategy/factory/StrategyFactory.js';
import { StrategyLifecycleManager } from '../core/strategy/lifecycle/StrategyLifecycleManager.js';
import { EdgeRegistry } from '../core/edge/EdgeRegistry.js';
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
  if (!args.parquet || !args.symbol) {
    console.error('Usage: node run-full-pipeline.js --parquet=<path> --symbol=<SYMBOL> [options]');
    console.error('');
    console.error('Required:');
    console.error('  --parquet    Path to parquet file');
    console.error('  --symbol     Symbol (e.g., ADA/USDT)');
    console.error('');
    console.error('Optional:');
    console.error('  --meta         Path to meta.json (default: <parquet>.meta.json)');
    console.error('  --output-dir   Output directory (default: data/pipeline-output)');
    console.error('  --lifecycle-dir Lifecycle directory (default: data/lifecycle)');
    console.error('  --max-edges    Max edges per run (default: 20)');
    console.error('  --seed         Random seed (default: 42)');
    console.error('  --dry-run      Discovery + validation only (skip factory)');
    process.exit(1);
  }

  // Default values
  const metaPath = args.meta || args.parquet.replace('.parquet', '.parquet.meta.json');
  const outputDir = args['output-dir'] || 'data/pipeline-output';
  const lifecycleDir = args['lifecycle-dir'] || 'data/lifecycle';
  const maxEdges = parseInt(args['max-edges'] || '20');
  const seed = parseInt(args.seed || '42');
  const dryRun = args['dry-run'] === true;

  console.log('='.repeat(80));
  console.log('FULL EDGE DISCOVERY PIPELINE');
  console.log('='.repeat(80));
  console.log(`Parquet:        ${args.parquet}`);
  console.log(`Meta:           ${metaPath}`);
  console.log(`Symbol:         ${args.symbol}`);
  console.log(`Max Edges:      ${maxEdges}`);
  console.log(`Seed:           ${seed}`);
  console.log(`Output Dir:     ${outputDir}`);
  console.log(`Lifecycle Dir:  ${lifecycleDir}`);
  console.log(`Dry Run:        ${dryRun ? 'YES' : 'NO'}`);
  console.log('='.repeat(80));

  let discoveryResult, validationResults, factoryResults, lifecycleSummary;

  try {
    // ============================================================================
    // STEP 1: DISCOVERY
    // ============================================================================
    console.log('');
    console.log('[1/4] EDGE DISCOVERY');
    console.log('-'.repeat(80));

    const registry = new EdgeRegistry();
    const discoveryPipeline = new EdgeDiscoveryPipeline({
      registry,
      seed,
      maxEdgesPerRun: maxEdges
    });

    discoveryResult = await discoveryPipeline.run({
      parquetPath: args.parquet,
      metaPath,
      symbol: args.symbol
    });

    if (discoveryResult.edges.length === 0) {
      console.log('No edges discovered. Pipeline stopped.');
      await savePipelineReport(outputDir, args.symbol, { discovery: discoveryResult }, startTime);
      process.exit(0);
    }

    // ============================================================================
    // STEP 2: VALIDATION
    // ============================================================================
    console.log('');
    console.log('[2/4] EDGE VALIDATION');
    console.log('-'.repeat(80));

    // Reuse dataset from discovery (in-memory)
    const loader = new DiscoveryDataLoader();
    const dataset = await loader.load({
      parquetPath: args.parquet,
      metaPath,
      symbol: args.symbol
    });

    const validationPipeline = new EdgeValidationPipeline({ registry });
    validationResults = await validationPipeline.validateAll(dataset);

    const validatedEdges = registry.getByStatus('VALIDATED');

    if (validatedEdges.length === 0) {
      console.log('No edges passed validation. Pipeline stopped.');
      await savePipelineReport(outputDir, args.symbol, {
        discovery: discoveryResult,
        validation: validationResults
      }, startTime);
      process.exit(0);
    }

    // ============================================================================
    // STEP 3: STRATEGY FACTORY (unless --dry-run)
    // ============================================================================
    if (dryRun) {
      console.log('');
      console.log('[3/4] STRATEGY FACTORY - SKIPPED (--dry-run)');
      console.log('[4/4] PERSIST - SKIPPED (--dry-run)');

      await savePipelineReport(outputDir, args.symbol, {
        discovery: discoveryResult,
        validation: validationResults
      }, startTime);

      console.log('');
      console.log('='.repeat(80));
      console.log('DRY RUN COMPLETE');
      console.log(`Discovered: ${discoveryResult.edgeCandidatesGenerated} edges`);
      console.log(`Validated:  ${validatedEdges.length} edges`);
      console.log('='.repeat(80));

      process.exit(0);
    }

    console.log('');
    console.log('[3/4] STRATEGY FACTORY');
    console.log('-'.repeat(80));

    const lifecycleManager = new StrategyLifecycleManager(lifecycleDir);

    // Try to restore existing state
    try {
      await lifecycleManager.restore();
      console.log(`Restored ${lifecycleManager.getSummary().totalStrategies} existing strategies`);
    } catch (err) {
      console.log('No existing lifecycle state (first run)');
    }

    // Connect EdgeRegistry
    lifecycleManager.connectEdgeRegistry(registry);

    const factory = new StrategyFactory({
      registry,
      dataConfig: {
        parquetPath: args.parquet,
        metaPath,
        symbol: args.symbol
      }
    });

    factory.deployer.lifecycleManager = lifecycleManager;

    factoryResults = await factory.produceAll();

    // ============================================================================
    // STEP 4: PERSIST
    // ============================================================================
    console.log('');
    console.log('[4/4] PERSISTING STATE');
    console.log('-'.repeat(80));

    await fs.mkdir(outputDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const serializer = new EdgeSerializer();

    // Save edges
    const edgesFile = path.join(outputDir, `pipeline-edges-${timestamp}.json`);
    await serializer.saveToFile(edgesFile, registry);

    // Save lifecycle state
    await lifecycleManager.persist();

    lifecycleSummary = lifecycleManager.getSummary();

    // Save full pipeline report
    await savePipelineReport(outputDir, args.symbol, {
      discovery: discoveryResult,
      validation: validationResults,
      factory: factoryResults,
      lifecycle: lifecycleSummary
    }, startTime);

    // ============================================================================
    // FINAL SUMMARY
    // ============================================================================
    const deployed = factoryResults.filter(r => r.status === 'DEPLOYED').length;

    console.log('');
    console.log('='.repeat(80));
    console.log('PIPELINE COMPLETE');
    console.log('='.repeat(80));
    console.log(JSON.stringify({
      event: 'pipeline_complete',
      discovery: {
        patternsScanned: discoveryResult.patternsScanned,
        edgesCandidates: discoveryResult.edgeCandidatesGenerated
      },
      validation: {
        validated: validatedEdges.length,
        rejected: validationResults.filter(r => r.newStatus === 'REJECTED').length
      },
      factory: {
        deployed,
        failed: factoryResults.filter(r => r.status !== 'DEPLOYED').length
      },
      lifecycle: lifecycleSummary,
      durationMs: Date.now() - startTime
    }, null, 2));
    console.log('='.repeat(80));

  } catch (error) {
    console.error('');
    console.error('='.repeat(80));
    console.error(JSON.stringify({
      event: 'pipeline_error',
      error: error.message,
      stack: error.stack
    }, null, 2));
    console.error('='.repeat(80));
    process.exit(1);
  }
}

async function savePipelineReport(outputDir, symbol, results, startTime) {
  await fs.mkdir(outputDir, { recursive: true });

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const reportFile = path.join(outputDir, `pipeline-report-${timestamp}.json`);

  const report = {
    pipeline: 'full',
    timestamp: new Date().toISOString(),
    symbol,
    durationMs: Date.now() - startTime,
    ...results
  };

  await writeJSON(reportFile, report);

  console.log(`Pipeline report saved: ${reportFile}`);
}

main();
