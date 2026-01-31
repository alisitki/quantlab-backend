#!/usr/bin/env node
/**
 * QuantLab Strategy Runtime — Determinism Verification Test
 * 
 * PHASE 8: Real Engine Wiring Test
 * 
 * Runs StrategyRuntime + ReplayEngine + ExecutionEngine together
 * and outputs determinism-critical hashes for twin-run comparison.
 * 
 * Usage:
 *   node test-real-replay.js \
 *     --parquet <path> \
 *     --meta <path> \
 *     --strategy <path> \
 *     --stream bbo \
 *     --seed test
 * 
 * Output (JSON):
 *   { run_id, state_hash, fills_hash, event_count }
 */

import dotenv from 'dotenv';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

import { existsSync } from 'node:fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment variables from core/.env
const envPath = resolve(__dirname, '../../.env');
if (existsSync(envPath)) {
  console.log(`[test-real-replay] Loading environment from ${envPath}`);
  dotenv.config({ path: envPath });
} else {
  console.warn(`[test-real-replay] WARNING: core/.env not found, using system environment variables`);
}

// Strategy Runtime v2 imports
import { StrategyRuntime } from './StrategyRuntime.js';
import { StrategyLoader } from '../interface/StrategyLoader.js';
import { OrderingGuard } from '../safety/OrderingGuard.js';
import { ErrorContainment } from '../safety/ErrorContainment.js';
import { MetricsRegistry } from '../metrics/MetricsRegistry.js';
import { ErrorPolicy, OrderingMode } from '../interface/types.js';

// Core engine imports
import { ReplayEngine } from '../../replay/ReplayEngine.js';
import { ExecutionEngine } from '../../execution/engine.js';

/**
 * Parse CLI arguments
 */
function parseCliArgs() {
  const { values } = parseArgs({
    options: {
      parquet: { type: 'string' },
      meta: { type: 'string' },
      strategy: { type: 'string' },
      stream: { type: 'string', default: 'bbo' },
      config: { type: 'string', default: '{}' },
      seed: { type: 'string', default: 'determinism-test' },
      limit: { type: 'string' }
    },
    allowPositionals: false
  });
  return values;
}

/**
 * Main entry point
 */
async function main() {
  const args = parseCliArgs();
  
  if (!args.parquet || !args.meta || !args.strategy) {
    console.error('Usage: node test-real-replay.js --parquet <path> --meta <path> --strategy <path> [--stream bbo] [--seed test]');
    process.exit(1);
  }
  
  try {
    // 1. Parse strategy config
    const strategyConfig = JSON.parse(args.config);
    
    // 2. Load strategy (auto-adapts v1 → v2)
    const strategyPath = resolve(process.cwd(), args.strategy);
    const strategy = await StrategyLoader.loadFromFile(strategyPath, {
      config: strategyConfig,
      autoAdapt: true
    });
    
    // 3. Create ReplayEngine
    const replayEngine = new ReplayEngine(
      { parquet: args.parquet, meta: args.meta },
      { stream: args.stream }
    );
    await replayEngine.validate();
    
    // 4. Create ExecutionEngine
    const executionEngine = new ExecutionEngine({
      initialCapital: 10000,
      recordEquityCurve: true,
      requiresBbo: args.stream === 'bbo'
    });
    
    // 5. Create StrategyRuntime
    const runtime = new StrategyRuntime({
      dataset: {
        parquet: args.parquet,
        meta: args.meta,
        stream: args.stream
      },
      strategy,
      strategyConfig,
      seed: args.seed,
      errorPolicy: ErrorPolicy.FAIL_FAST,
      orderingMode: OrderingMode.STRICT,
      enableCheckpoints: false
    });
    
    // 6. Attach components
    const orderingGuard = new OrderingGuard({ mode: OrderingMode.STRICT });
    runtime.attachOrderingGuard(orderingGuard);
    
    const errorContainment = new ErrorContainment({
      policy: ErrorPolicy.FAIL_FAST,
      maxErrors: 10
    });
    runtime.attachErrorContainment(errorContainment);
    
    const metrics = new MetricsRegistry({ runId: runtime.runId });
    runtime.attachMetrics(metrics);
    
    runtime.attachExecutionEngine(executionEngine);
    
    // 7. Initialize runtime
    await runtime.init();
    
    // 8. Create event stream from ReplayEngine
    console.log('[test-real-replay] Starting event stream...');
    async function* createEventStream() {
      const replayOpts = { batchSize: 10000 };
      const replayGenerator = replayEngine.replay(replayOpts);
      const limit = args.limit ? parseInt(args.limit, 10) : Infinity;
      
      let count = 0;
      for await (const event of replayGenerator) {
        count++;
        if (count % 10000 === 0) console.log(`[test-real-replay] Processed ${count} events...`);
        
        // Feed event to ExecutionEngine first (for price updates)
        executionEngine.onEvent(event);
        yield event;
        
        if (count >= limit) break;
      }
      console.log(`[test-real-replay] Stream finished. Total events: ${count}`);
    }
    
    // 9. Process the stream
    const manifest = await runtime.processStream(createEventStream());
    
    // 10. Clean up
    await replayEngine.close();
    
    // 11. Output determinism-critical values ONLY
    const result = {
      run_id: manifest.run_id,
      state_hash: manifest.output.state_hash,
      fills_hash: manifest.output.fills_hash,
      event_count: manifest.output.event_count
    };
    
    console.log(JSON.stringify(result, null, 2));
    process.exit(0);
    
  } catch (error) {
    console.error('ERROR:', error.message);
    if (process.env.DEBUG) {
      console.error(error.stack);
    }
    process.exit(1);
  }
}

main();
