#!/usr/bin/env node

/**
 * Edge Validation CLI Tool
 *
 * Validates discovered edge candidates using statistical tests.
 * Updates edge status to VALIDATED or REJECTED.
 *
 * Usage:
 *   node tools/run-edge-validation.js \
 *     --edges-file=/path/to/edges-discovered-*.json \
 *     --parquet=/path/to/data.parquet \
 *     --symbol=ADA/USDT \
 *     [--meta=/path/to/meta.json] \
 *     [--output-dir=data/pipeline-output]
 */

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
    console.error('Usage: node run-edge-validation.js --edges-file=<file> --parquet=<path> --symbol=<SYMBOL> [options]');
    console.error('');
    console.error('Required:');
    console.error('  --edges-file Path to edges-discovered-*.json');
    console.error('  --parquet    Path to parquet file (for validation data)');
    console.error('  --symbol     Symbol (e.g., ADA/USDT)');
    console.error('');
    console.error('Optional:');
    console.error('  --meta       Path to meta.json (default: <parquet>.meta.json)');
    console.error('  --output-dir Output directory (default: data/pipeline-output)');
    process.exit(1);
  }

  // Default values
  const metaPath = args.meta || args.parquet.replace('.parquet', '.parquet.meta.json');
  const outputDir = args['output-dir'] || 'data/pipeline-output';

  console.log('='.repeat(80));
  console.log('EDGE VALIDATION PIPELINE');
  console.log('='.repeat(80));
  console.log(`Edges File: ${args['edges-file']}`);
  console.log(`Parquet:    ${args.parquet}`);
  console.log(`Meta:       ${metaPath}`);
  console.log(`Symbol:     ${args.symbol}`);
  console.log(`Output Dir: ${outputDir}`);
  console.log('='.repeat(80));

  try {
    // Load edges from discovery output
    const serializer = new EdgeSerializer();
    const registry = await serializer.loadFromFile(args['edges-file']);

    const candidateCount = registry.getByStatus('CANDIDATE').length;

    if (candidateCount === 0) {
      console.log('');
      console.log('='.repeat(80));
      console.log(JSON.stringify({
        event: 'validation_skip',
        reason: 'no_candidate_edges',
        totalEdges: registry.size()
      }, null, 2));
      console.log('='.repeat(80));
      process.exit(0);
    }

    console.log(`Found ${candidateCount} CANDIDATE edges to validate`);
    console.log('');

    // Load validation dataset
    console.log('Loading validation dataset...');
    const loader = new DiscoveryDataLoader();
    const dataset = await loader.load({
      parquetPath: args.parquet,
      metaPath,
      symbol: args.symbol
    });

    console.log(`Loaded ${dataset.rows.length} rows for validation`);
    console.log('');

    // Create validation pipeline
    const validationPipeline = new EdgeValidationPipeline({ registry });

    // Validate all CANDIDATE edges
    const startTime = Date.now();
    const results = await validationPipeline.validateAll(dataset);
    const duration = Date.now() - startTime;

    // Save validated edges
    await fs.mkdir(outputDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

    const edgesFile = path.join(outputDir, `edges-validated-${timestamp}.json`);
    await serializer.saveToFile(edgesFile, registry);

    // Save validation report
    const reportFile = path.join(outputDir, `validation-report-${timestamp}.json`);
    const report = {
      step: 'validation',
      timestamp: new Date().toISOString(),
      symbol: args.symbol,
      parquetPath: args.parquet,
      duration,
      results: results.map(r => ({
        edgeId: r.edgeId,
        newStatus: r.newStatus,
        score: r.score,
        validatedAt: r.validatedAt
      }))
    };

    await writeJSON(reportFile, report);

    // Console summary
    const validated = results.filter(r => r.newStatus === 'VALIDATED').length;
    const rejected = results.filter(r => r.newStatus === 'REJECTED').length;

    console.log('');
    console.log('='.repeat(80));
    console.log(JSON.stringify({
      event: 'validation_complete',
      total: results.length,
      validated,
      rejected,
      durationMs: duration,
      outputFiles: {
        edges: edgesFile,
        report: reportFile
      }
    }, null, 2));
    console.log('='.repeat(80));

  } catch (error) {
    console.error('');
    console.error('='.repeat(80));
    console.error(JSON.stringify({
      event: 'validation_error',
      error: error.message,
      stack: error.stack
    }, null, 2));
    console.error('='.repeat(80));
    process.exit(1);
  }
}

main();
