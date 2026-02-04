#!/usr/bin/env node
/**
 * Replay Run → S3 Archive determinism verification
 */

import dotenv from 'dotenv';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

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
      seed: { type: 'string', default: 'replay-archive' },
      batch: { type: 'string' }
    },
    allowPositionals: false
  });
  return values;
}

function envRequired(name) {
  const v = process.env[name];
  if (!v) throw new Error(`ENV_MISSING: ${name}`);
  return v;
}

async function hashS3Object(s3, bucket, key) {
  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const hash = createHash('sha256');
  for await (const chunk of res.Body) {
    hash.update(chunk);
  }
  return hash.digest('hex');
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
  await runtime.processReplay(replayEngine, batchSize ? { batchSize } : {});
  await replayEngine.close();

  const replayRunId = runtime.replayRunId;
  const bucket = envRequired('RUN_ARCHIVE_S3_BUCKET');
  const endpoint = envRequired('RUN_ARCHIVE_S3_ENDPOINT');
  const accessKey = envRequired('RUN_ARCHIVE_S3_ACCESS_KEY');
  const secretKey = envRequired('RUN_ARCHIVE_S3_SECRET_KEY');

  const s3 = new S3Client({
    endpoint,
    region: 'auto',
    credentials: { accessKeyId: accessKey, secretAccessKey: secretKey },
    forcePathStyle: true
  });

  const key = `replay_runs/replay_run_id=${replayRunId}/decisions.jsonl`;
  const hash = await hashS3Object(s3, bucket, key);

  return { replay_run_id: replayRunId, decisions_hash: hash };
}

async function main() {
  const args = parseCliArgs();

  if (!args.parquet || !args.meta || !args.strategy) {
    console.error('Usage: node tools/verify-run-archive.js --parquet <path> --meta <path> --strategy <path> [--stream bbo] [--seed test]');
    process.exit(1);
  }

  const runA = await runOnce(args);
  const runB = await runOnce(args);

  const hashMatch = runA.decisions_hash === runB.decisions_hash;

  console.log('--- RUN A ---');
  console.log(JSON.stringify(runA, null, 2));
  console.log('--- RUN B ---');
  console.log(JSON.stringify(runB, null, 2));

  console.log('--- RESULT ---');
  console.log(`decisions_hash_match: ${hashMatch}`);
  console.log(`PASS: ${hashMatch}`);

  if (!hashMatch) process.exit(1);
}

main().catch((err) => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
