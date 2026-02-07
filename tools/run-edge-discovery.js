#!/usr/bin/env node

/**
 * Edge Discovery CLI Tool
 *
 * Runs edge discovery pipeline on historical parquet data.
 * Outputs edge candidates to JSON file.
 *
 * Usage:
 *   node tools/run-edge-discovery.js \
 *     --parquet=/path/to/data.parquet \
 *     --symbol=ADA/USDT \
 *     [--meta=/path/to/meta.json] \
 *     [--output-dir=data/pipeline-output] \
 *     [--max-edges=20] \
 *     [--seed=42]
 */

import { EdgeDiscoveryPipeline } from '../core/edge/discovery/EdgeDiscoveryPipeline.js';
import { EdgeRegistry } from '../core/edge/EdgeRegistry.js';
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
  if (!args.parquet || !args.symbol) {
    console.error('Usage: node run-edge-discovery.js --parquet=<path> --symbol=<SYMBOL> [options]');
    console.error('');
    console.error('Required:');
    console.error('  --parquet    Path to parquet file');
    console.error('  --symbol     Symbol (e.g., ADA/USDT)');
    console.error('');
    console.error('Optional:');
    console.error('  --meta       Path to meta.json (default: <parquet>.meta.json)');
    console.error('  --output-dir Output directory (default: data/pipeline-output)');
    console.error('  --max-edges  Max edges per run (default: 20)');
    console.error('  --seed       Random seed (default: 42)');
    process.exit(1);
  }

  // Default values
  const metaPath = args.meta || args.parquet.replace('.parquet', '.parquet.meta.json');
  const outputDir = args['output-dir'] || 'data/pipeline-output';
  const maxEdges = parseInt(args['max-edges'] || '20');
  const seed = parseInt(args.seed || '42');

  console.log('='.repeat(80));
  console.log('EDGE DISCOVERY PIPELINE');
  console.log('='.repeat(80));
  console.log(`Parquet:    ${args.parquet}`);
  console.log(`Meta:       ${metaPath}`);
  console.log(`Symbol:     ${args.symbol}`);
  console.log(`Max Edges:  ${maxEdges}`);
  console.log(`Seed:       ${seed}`);
  console.log(`Output Dir: ${outputDir}`);
  console.log('='.repeat(80));

  try {
    // Create registry and pipeline
    const registry = new EdgeRegistry();
    const pipeline = new EdgeDiscoveryPipeline({
      registry,
      seed,
      maxEdgesPerRun: maxEdges
    });

    // Run discovery
    const startTime = Date.now();
    const result = await pipeline.run({
      parquetPath: args.parquet,
      metaPath,
      symbol: args.symbol
    });

    const duration = Date.now() - startTime;

    // Save results
    await fs.mkdir(outputDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const serializer = new EdgeSerializer();

    // Save edges with definitions
    const edgesFile = path.join(outputDir, `edges-discovered-${timestamp}.json`);
    await serializer.saveToFile(edgesFile, registry);

    // Save summary report
    const reportFile = path.join(outputDir, `discovery-report-${timestamp}.json`);
    const report = {
      step: 'discovery',
      timestamp: new Date().toISOString(),
      symbol: args.symbol,
      parquetPath: args.parquet,
      patternsScanned: result.patternsScanned,
      patternsTestedSignificant: result.patternsTestedSignificant,
      edgeCandidatesGenerated: result.edgeCandidatesGenerated,
      edgeCandidatesRegistered: result.edgeCandidatesRegistered,
      duration,
      metadata: result.metadata,
      edges: result.edges.map(e => e.toJSON())
    };

    await writeJSON(reportFile, report);

    // Console summary (JSON for programmatic consumption)
    console.log('');
    console.log('='.repeat(80));
    console.log(JSON.stringify({
      event: 'discovery_complete',
      patternsScanned: result.patternsScanned,
      patternsSignificant: result.patternsTestedSignificant,
      edgesCandidates: result.edgeCandidatesGenerated,
      edgesRegistered: result.edgeCandidatesRegistered,
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
      event: 'discovery_error',
      error: error.message,
      stack: error.stack
    }, null, 2));
    console.error('='.repeat(80));
    process.exit(1);
  }
}

main();
