#!/usr/bin/env node
/**
 * Replay → Strategy Runtime determinism verification
 * Runs replay twice and compares decision hash + count.
 */

import dotenv from 'dotenv';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { existsSync } from 'node:fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const envPath = resolve(__dirname, '../../.env');
if (existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

import { StrategyRuntime } from '../../strategy/runtime/StrategyRuntime.js';
import { StrategyLoader } from '../../strategy/interface/StrategyLoader.js';
import { OrderingGuard } from '../../strategy/safety/OrderingGuard.js';
import { ErrorContainment } from '../../strategy/safety/ErrorContainment.js';
import { MetricsRegistry } from '../../strategy/metrics/MetricsRegistry.js';
import { ErrorPolicy, OrderingMode } from '../../strategy/interface/types.js';
import { ReplayEngine } from '../ReplayEngine.js';

function parseCliArgs() {
  const { values } = parseArgs({
    options: {
      parquet: { type: 'string' },
      meta: { type: 'string' },
      strategy: { type: 'string' },
      stream: { type: 'string', default: 'bbo' },
      config: { type: 'string', default: '{}' },
      seed: { type: 'string', default: 'replay-strategy' },
      batch: { type: 'string' }
    },
    allowPositionals: false
  });
  return values;
}

async function runOnce(args) {
  const strategyConfig = JSON.parse(args.config || '{}');
  const strategyPath = resolve(process.cwd(), args.strategy);
  const strategy = await StrategyLoader.loadFromFile(strategyPath, {
    config: strategyConfig,
    autoAdapt: true
  });

  const replayEngine = new ReplayEngine(
    { parquet: args.parquet, meta: args.meta },
    { stream: args.stream }
  );

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

  const orderingGuard = new OrderingGuard({ mode: OrderingMode.STRICT });
  runtime.attachOrderingGuard(orderingGuard);

  const errorContainment = new ErrorContainment({
    policy: ErrorPolicy.FAIL_FAST,
    maxErrors: 10
  });
  runtime.attachErrorContainment(errorContainment);

  const metrics = new MetricsRegistry({ runId: runtime.runId });
  runtime.attachMetrics(metrics);

  await runtime.init();

  const batchSize = args.batch ? parseInt(args.batch, 10) : undefined;
  const manifest = await runtime.processReplay(replayEngine, batchSize ? { batchSize } : {});
  await replayEngine.close();

  return {
    run_id: manifest.run_id,
    replay_run_id: manifest.replay?.replay_run_id || runtime.replayRunId,
    decision_count: manifest.output.decision_count,
    decision_hash: manifest.output.decision_hash
  };
}

async function main() {
  const args = parseCliArgs();

  if (!args.parquet || !args.meta || !args.strategy) {
    console.error('Usage: node tools/verify-replay-strategy.js --parquet <path> --meta <path> --strategy <path> [--stream bbo] [--seed test]');
    process.exit(1);
  }

  const runA = await runOnce(args);
  const runB = await runOnce(args);

  const countMatch = runA.decision_count === runB.decision_count;
  const hashMatch = runA.decision_hash === runB.decision_hash;

  console.log('--- RUN A ---');
  console.log(JSON.stringify(runA, null, 2));
  console.log('--- RUN B ---');
  console.log(JSON.stringify(runB, null, 2));

  console.log('--- RESULT ---');
  console.log(`decision_count_match: ${countMatch}`);
  console.log(`decision_hash_match: ${hashMatch}`);
  console.log(`PASS: ${countMatch && hashMatch}`);

  if (!countMatch || !hashMatch) process.exit(1);
}

main().catch((err) => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
