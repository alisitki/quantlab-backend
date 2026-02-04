#!/usr/bin/env node
/**
 * Verify replay vs live decision parity
 */

import dotenv from 'dotenv';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { existsSync } from 'node:fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const envPath = resolve(__dirname, '../core/.env');
if (existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

import { StrategyRuntime } from '../core/strategy/runtime/StrategyRuntime.js';
import { StrategyLoader } from '../core/strategy/interface/StrategyLoader.js';
import { OrderingGuard } from '../core/strategy/safety/OrderingGuard.js';
import { ErrorContainment } from '../core/strategy/safety/ErrorContainment.js';
import { MetricsRegistry } from '../core/strategy/metrics/MetricsRegistry.js';
import { ErrorPolicy, OrderingMode } from '../core/strategy/interface/types.js';
import { ReplayEngine } from '../core/replay/ReplayEngine.js';
import { LiveStrategyRunner } from '../core/strategy/live/LiveStrategyRunner.js';

function parseCliArgs() {
  const { values } = parseArgs({
    options: {
      parquet: { type: 'string' },
      meta: { type: 'string' },
      strategy: { type: 'string' },
      stream: { type: 'string', default: 'bbo' },
      config: { type: 'string', default: '{}' },
      seed: { type: 'string', default: 'live-parity' },
      batch: { type: 'string' }
    },
    allowPositionals: false
  });
  return values;
}

async function runReplay(args) {
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
  const meta = await replayEngine.getMeta();

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
    decision_count: manifest.output.decision_count,
    decision_hash: manifest.output.decision_hash
  };
}

async function runLive(args) {
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

  const liveRunner = new LiveStrategyRunner({
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
    enableMetrics: true,
    archiveInfo: {
      parquet_path: args.parquet,
      manifest_id: meta.manifest_id
    }
  });

  const batchSize = args.batch ? parseInt(args.batch, 10) : undefined;
  const stream = replayEngine.replay(batchSize ? { batchSize } : {});
  const result = await liveRunner.run(stream, { handleSignals: false });
  await replayEngine.close();

  return {
    decision_count: result.decision_count,
    decision_hash: result.decision_hash
  };
}

async function main() {
  const args = parseCliArgs();

  if (!args.parquet || !args.meta || !args.strategy) {
    console.error('Usage: node tools/verify-live-parity.js --parquet <path> --meta <path> --strategy <path> [--stream bbo] [--seed test]');
    process.exit(1);
  }

  const replay = await runReplay(args);
  const live = await runLive(args);

  const countMatch = replay.decision_count === live.decision_count;
  const hashMatch = replay.decision_hash === live.decision_hash;

  console.log('--- REPLAY ---');
  console.log(JSON.stringify(replay, null, 2));
  console.log('--- LIVE ---');
  console.log(JSON.stringify(live, null, 2));
  console.log('--- RESULT ---');
  console.log(`decision_count_match: ${countMatch}`);
  console.log(`decision_hash_match: ${hashMatch}`);
  console.log(`PASS: ${countMatch && hashMatch}`);

  if (!countMatch || !hashMatch) process.exit(1);
}

main().catch((err) => {
  console.error('FATAL:', err.message || String(err));
  process.exit(1);
});
