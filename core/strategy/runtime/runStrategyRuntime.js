#!/usr/bin/env node
/**
 * QuantLab Strategy Runtime v2 — CLI Entrypoint
 * 
 * PHASE 7: Engine Integration
 * 
 * New entrypoint that wires StrategyRuntime v2 to real ReplayEngine + ExecutionEngine.
 * Does NOT modify existing Runner.js behavior.
 * 
 * Usage:
 *   node runStrategyRuntime.js \
 *     --parquet /path/to/data.parquet \
 *     --meta /path/to/meta.json \
 *     --strategy /path/to/MyStrategy.js \
 *     --stream bbo \
 *     [--config '{"param": 1}'] \
 *     [--seed test] \
 *     [--start-cursor <base64>] \
 *     [--end-cursor <base64>] \
 *     [--checkpoint-dir /path/to/checkpoints] \
 *     [--checkpoint-interval 50000] \
 *     [--error-policy FAIL_FAST|SKIP_AND_LOG|QUARANTINE] \
 *     [--ordering-mode STRICT|WARN] \
 *     [--no-execution] \
 *     [--output-dir /path/to/runs]
 * 
 * Output:
 *   Writes manifest.json to: <output-dir>/<run_id>/manifest.json
 * 
 * @module core/strategy/runtime/runStrategyRuntime
 */

import dotenv from 'dotenv';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { existsSync } from 'node:fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment variables from core/.env
const envPath = resolve(__dirname, '../../.env');
if (existsSync(envPath)) {
  console.log(`[runStrategyRuntime] Loading environment from ${envPath}`);
  dotenv.config({ path: envPath });
} else {
  console.warn(`[runStrategyRuntime] WARNING: core/.env not found, using system environment variables`);
}

import { parseArgs } from 'node:util';
import { mkdir, writeFile } from 'node:fs/promises';

// Strategy Runtime v2 imports
import { StrategyRuntime } from './StrategyRuntime.js';
import { StrategyLoader } from '../interface/StrategyLoader.js';
import { OrderingGuard } from '../safety/OrderingGuard.js';
import { ErrorContainment } from '../safety/ErrorContainment.js';
import { MetricsRegistry } from '../metrics/MetricsRegistry.js';
import { CheckpointManager } from '../state/CheckpointManager.js';
import { ErrorPolicy, OrderingMode } from '../interface/types.js';

// Core engine imports
import { ReplayEngine } from '../../replay/ReplayEngine.js';
import { ExecutionEngine } from '../../execution/engine.js';
import { encodeCursor, decodeCursor } from '../../replay/CursorCodec.js';

// Default output directory for run manifests
const DEFAULT_OUTPUT_DIR = join(__dirname, '_runs');

/**
 * Parse command line arguments.
 */
function parseCliArgs() {
  const { values } = parseArgs({
    options: {
      parquet: { type: 'string' },
      meta: { type: 'string' },
      stream: { type: 'string', default: 'bbo' },
      strategy: { type: 'string' },
      config: { type: 'string', default: '{}' },
      seed: { type: 'string', default: '' },
      'start-cursor': { type: 'string' },
      'end-cursor': { type: 'string' },
      'checkpoint-dir': { type: 'string' },
      'checkpoint-interval': { type: 'string', default: '100000' },
      'error-policy': { type: 'string', default: 'FAIL_FAST' },
      'ordering-mode': { type: 'string', default: 'STRICT' },
      'no-execution': { type: 'boolean', default: false },
      'output-dir': { type: 'string', default: DEFAULT_OUTPUT_DIR },
      help: { type: 'boolean', default: false }
    },
    allowPositionals: false
  });

  return values;
}

/**
 * Print usage and exit.
 */
function printUsage() {
  console.log(`
Usage: node runStrategyRuntime.js [options]

Required:
  --parquet <path>          Path to parquet file(s)
  --meta <path>             Path to meta.json file(s)
  --strategy <path>         Path to strategy file (.js)

Optional:
  --stream <name>           Stream type (default: bbo)
  --config <json>           Strategy config as JSON (default: {})
  --seed <string>           Seed for deterministic run_id
  --start-cursor <base64>   Resume from cursor
  --end-cursor <base64>     Stop at cursor (not implemented yet)
  --checkpoint-dir <path>   Enable checkpointing to this directory
  --checkpoint-interval <n> Events between checkpoints (default: 100000)
  --error-policy <policy>   FAIL_FAST | SKIP_AND_LOG | QUARANTINE
  --ordering-mode <mode>    STRICT | WARN
  --no-execution            Run without ExecutionEngine (dry run)
  --output-dir <path>       Dir for manifest output (default: _runs/)
  --help                    Show this help
`);
}

/**
 * Validate required arguments.
 */
function validateArgs(args) {
  const errors = [];
  
  if (!args.parquet) errors.push('--parquet is required');
  if (!args.meta) errors.push('--meta is required');
  if (!args.strategy) errors.push('--strategy is required');
  
  if (errors.length > 0) {
    console.error('ERROR:', errors.join(', '));
    printUsage();
    process.exit(1);
  }
}

/**
 * Main entry point.
 */
async function main() {
  const args = parseCliArgs();
  
  if (args.help) {
    printUsage();
    process.exit(0);
  }
  
  validateArgs(args);
  
  console.log('[runStrategyRuntime] Starting...');
  console.log(`  parquet: ${args.parquet}`);
  console.log(`  meta: ${args.meta}`);
  console.log(`  strategy: ${args.strategy}`);
  console.log(`  stream: ${args.stream}`);
  console.log(`  seed: ${args.seed || '(none)'}`);
  console.log(`  error-policy: ${args['error-policy']}`);
  console.log(`  ordering-mode: ${args['ordering-mode']}`);
  
  try {
    // 1. Parse strategy config
    let strategyConfig;
    try {
      strategyConfig = JSON.parse(args.config);
    } catch (e) {
      throw new Error(`Invalid --config JSON: ${e.message}`);
    }
    
    // 2. Load strategy (auto-adapts v1 → v2)
    console.log('[runStrategyRuntime] Loading strategy...');
    const strategyPath = resolve(process.cwd(), args.strategy);
    const strategy = await StrategyLoader.loadFromFile(strategyPath, {
      config: strategyConfig,
      autoAdapt: true
    });
    console.log(`  Loaded: ${strategy.id || 'unknown'} v${strategy.version || '?'}`);
    console.log(`  Type: ${StrategyLoader.detectVersion(strategy)}`);
    
    // 3. Create ReplayEngine
    console.log('[runStrategyRuntime] Creating ReplayEngine...');
    const replayEngine = new ReplayEngine(
      { parquet: args.parquet, meta: args.meta },
      { stream: args.stream }
    );
    await replayEngine.validate();
    
    // 4. Create ExecutionEngine (optional)
    let executionEngine = null;
    if (!args['no-execution']) {
      console.log('[runStrategyRuntime] Creating ExecutionEngine...');
      executionEngine = new ExecutionEngine({
        initialCapital: 10000,
        recordEquityCurve: true,
        requiresBbo: args.stream === 'bbo'
      });
    }
    
    // 5. Create StrategyRuntime
    console.log('[runStrategyRuntime] Creating StrategyRuntime...');
    const runtime = new StrategyRuntime({
      dataset: {
        parquet: args.parquet,
        meta: args.meta,
        stream: args.stream
      },
      strategy,
      strategyConfig,
      seed: args.seed,
      errorPolicy: ErrorPolicy[args['error-policy']] || ErrorPolicy.FAIL_FAST,
      orderingMode: OrderingMode[args['ordering-mode']] || OrderingMode.STRICT,
      enableCheckpoints: !!args['checkpoint-dir'],
      checkpointInterval: parseInt(args['checkpoint-interval'], 10)
    });
    
    console.log(`  run_id: ${runtime.runId}`);
    
    // 6. Attach components
    
    // OrderingGuard
    const orderingGuard = new OrderingGuard({
      mode: OrderingMode[args['ordering-mode']] || OrderingMode.STRICT
    });
    runtime.attachOrderingGuard(orderingGuard);
    
    // ErrorContainment
    const errorContainment = new ErrorContainment({
      policy: ErrorPolicy[args['error-policy']] || ErrorPolicy.FAIL_FAST,
      maxErrors: 100
    });
    runtime.attachErrorContainment(errorContainment);
    
    // MetricsRegistry
    const metrics = new MetricsRegistry({ runId: runtime.runId });
    runtime.attachMetrics(metrics);
    
    // CheckpointManager (if enabled)
    if (args['checkpoint-dir']) {
      const checkpointManager = new CheckpointManager({
        baseDir: resolve(process.cwd(), args['checkpoint-dir']),
        runId: runtime.runId
      });
      runtime.attachCheckpointManager(checkpointManager);
      console.log(`  checkpoints: ${args['checkpoint-dir']}`);
    }
    
    // ExecutionEngine (if enabled)
    if (executionEngine) {
      runtime.attachExecutionEngine(executionEngine);
    }
    
    // 7. Initialize runtime
    console.log('[runStrategyRuntime] Initializing...');
    await runtime.init();
    
    // 8. Create event stream from ReplayEngine
    console.log('[runStrategyRuntime] Starting replay...');
    const startTime = Date.now();
    
    // Create an async generator that wraps ReplayEngine
    async function* createEventStream() {
      const replayOpts = {
        batchSize: 10000
      };
      
      // Handle start cursor for resume
      if (args['start-cursor']) {
        replayOpts.cursor = args['start-cursor'];
        console.log('  Resuming from cursor...');
      }
      
      const replayGenerator = replayEngine.replay(replayOpts);
      
      for await (const event of replayGenerator) {
        // Feed event to ExecutionEngine first (for price updates)
        if (executionEngine) {
          executionEngine.onEvent(event);
        }
        
        // Yield event to runtime
        yield event;
      }
    }
    
    // Process the stream
    const manifest = await runtime.processStream(createEventStream());
    
    const elapsedMs = Date.now() - startTime;
    
    // 9. Create output directory
    const outputDir = resolve(process.cwd(), args['output-dir'], runtime.runId);
    await mkdir(outputDir, { recursive: true });
    
    // 10. Enhance manifest with additional info
    const fullManifest = {
      ...manifest,
      config: {
        parquet: args.parquet,
        meta: args.meta,
        stream: args.stream,
        strategy: args.strategy,
        strategyConfig,
        seed: args.seed,
        errorPolicy: args['error-policy'],
        orderingMode: args['ordering-mode']
      },
      timing: {
        elapsed_ms: elapsedMs,
        events_per_sec: manifest.output.event_count / (elapsedMs / 1000)
      },
      metrics: metrics.snapshot(),
      safety: {
        ordering_violations: orderingGuard.getStats().violationCount,
        errors_contained: errorContainment.errorCount,
        events_skipped: errorContainment.skippedCount
      }
    };
    
    // 11. Write manifest
    const manifestPath = join(outputDir, 'manifest.json');
    await writeFile(manifestPath, JSON.stringify(fullManifest, null, 2));
    
    console.log('');
    console.log('[runStrategyRuntime] ==================== COMPLETE ====================');
    console.log(`  run_id:       ${manifest.run_id}`);
    console.log(`  events:       ${manifest.output.event_count}`);
    console.log(`  fills:        ${manifest.output.fills_count}`);
    console.log(`  state_hash:   ${manifest.output.state_hash.substring(0, 16)}...`);
    console.log(`  fills_hash:   ${manifest.output.fills_hash.substring(0, 16)}...`);
    console.log(`  elapsed:      ${elapsedMs}ms`);
    console.log(`  events/sec:   ${(manifest.output.event_count / (elapsedMs / 1000)).toFixed(0)}`);
    console.log(`  manifest:     ${manifestPath}`);
    console.log('================================================================');
    
    // Clean up
    await replayEngine.close();
    
    process.exit(0);
    
  } catch (error) {
    console.error('');
    console.error('[runStrategyRuntime] FATAL ERROR:', error.message);
    if (process.env.DEBUG) {
      console.error(error.stack);
    }
    process.exit(1);
  }
}

// Run main
main();
