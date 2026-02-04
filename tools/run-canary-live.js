import { LiveStrategyRunner } from '../core/strategy/live/LiveStrategyRunner.js';
import { readdir, readFile, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(new URL('../core/package.json', import.meta.url));
const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const dotenv = require('dotenv');

dotenv.config({ path: new URL('../core/.env', import.meta.url).pathname });

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function pad2(n) {
  return n.toString().padStart(2, '0');
}

function dateKeyFromMs(ms) {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}${pad2(d.getUTCDate())}`;
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith('--')) continue;
    const name = key.replace(/^--/, '');
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[name] = true;
    } else {
      args[name] = next;
      i += 1;
    }
  }
  return args;
}

function usage() {
  console.error('Usage: node tools/run-canary-live.js --exchange binance --symbols BTCUSDT,ETHUSDT --strategy /path/to/strategy.js --dataset-parquet live --dataset-meta live');
}

class LocalArchiveWriter {
  constructor(outDir) {
    this.outDir = outDir;
  }

  async write(run) {
    const base = join(this.outDir, `replay_runs/replay_run_id=${run.replay_run_id}`);
    await mkdir(base, { recursive: true });
    const manifest = {
      replay_run_id: run.replay_run_id,
      seed: run.seed,
      manifest_id: run.manifest_id,
      parquet_path: run.parquet_path,
      started_at: run.first_ts_event ? new Date(Number(run.first_ts_event / 1_000_000n)).toISOString() : null,
      finished_at: run.last_ts_event ? new Date(Number(run.last_ts_event / 1_000_000n)).toISOString() : null,
      stop_reason: run.stop_reason
    };
    const stats = {
      emitted_event_count: run.stats.emitted_event_count,
      decision_count: run.stats.decision_count,
      duration_ms: run.stats.duration_ms
    };
    const lines = run.decisions.map(d => JSON.stringify({
      replay_run_id: d.replay_run_id,
      cursor: d.cursor,
      ts_event: d.ts_event,
      decision: d.decision
    }));
    await writeFile(join(base, 'manifest.json'), JSON.stringify(manifest));
    await writeFile(join(base, 'stats.json'), JSON.stringify(stats));
    await writeFile(join(base, 'decisions.jsonl'), lines.join('\n') + '\n');
  }
}

async function* mockEvents(count = 20) {
  const base = 1700000000000;
  for (let i = 0; i < count; i++) {
    yield {
      ts_event: base + i,
      ts_recv: base + i + 1,
      exchange: 'binance',
      symbol: 'BTCUSDT',
      stream: 'bbo',
      stream_version: 1,
      bid_price: 100 + i,
      bid_qty: 1,
      ask_price: 101 + i,
      ask_qty: 1
    };
  }
}

async function checkArchive(runId) {
  if (!envBool(process.env.RUN_ARCHIVE_ENABLED || '0')) {
    throw new Error('RUN_ARCHIVE_ENABLED=0 (archive check skipped)');
  }

  const bucket = process.env.RUN_ARCHIVE_S3_BUCKET;
  const endpoint = process.env.RUN_ARCHIVE_S3_ENDPOINT;
  const accessKey = process.env.RUN_ARCHIVE_S3_ACCESS_KEY;
  const secretKey = process.env.RUN_ARCHIVE_S3_SECRET_KEY;

  const s3 = new S3Client({
    endpoint,
    region: 'auto',
    credentials: {
      accessKeyId: accessKey,
      secretAccessKey: secretKey
    },
    forcePathStyle: true
  });

  const prefix = `replay_runs/replay_run_id=${runId}`;
  const keys = [
    `${prefix}/manifest.json`,
    `${prefix}/decisions.jsonl`,
    `${prefix}/stats.json`
  ];

  for (const key of keys) {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
  }
}

async function checkAudit(runId, startedAtMs) {
  const spoolDir = process.env.AUDIT_SPOOL_DIR || '/tmp/quantlab-audit';
  const dateKey = dateKeyFromMs(startedAtMs);
  const dir = join(spoolDir, `date=${dateKey}`);

  const files = await readdir(dir);
  let found = false;

  for (const file of files) {
    if (!file.endsWith('.jsonl')) continue;
    const content = await readFile(join(dir, file), 'utf-8');
    const lines = content.split('\n').filter(Boolean);
    for (const line of lines) {
      try {
        const parsed = JSON.parse(line);
        const metadataRunId = parsed?.metadata?.live_run_id;
        if (parsed?.target_id === runId || metadataRunId === runId) {
          found = true;
          break;
        }
      } catch {
        // ignore malformed lines
      }
    }
    if (found) break;
  }

  if (!found) {
    throw new Error('Audit entries not found for live run');
  }
}

async function checkLocalArchive(runId, archiveDir) {
  const base = join(archiveDir, `replay_runs/replay_run_id=${runId}`);
  const files = await readdir(base);
  const required = new Set(['manifest.json', 'decisions.jsonl', 'stats.json']);
  for (const f of required) {
    if (!files.includes(f)) {
      throw new Error(`Local archive missing: ${f}`);
    }
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const exchange = args.exchange;
  const symbols = args.symbols ? args.symbols.split(',').map(s => s.trim()).filter(Boolean) : [];
  const strategyPath = args.strategy;
  const datasetParquet = args['dataset-parquet'];
  const datasetMeta = args['dataset-meta'];
  const seed = args.seed || '';
  const strategyConfigRaw = args['strategy-config'] || process.env.GO_LIVE_STRATEGY_CONFIG || '';
  const orderingMode = args['ordering-mode'] || process.env.GO_LIVE_ORDERING_MODE || null;
  const executionMode = args['execution-mode'] || process.env.GO_LIVE_EXECUTION_MODE || 'mock';
  const durationSeconds = Number(args['duration-seconds'] || process.env.GO_LIVE_CANARY_SECONDS || '30');
  const canaryDurationMs = Number.isFinite(durationSeconds) && durationSeconds > 0 ? durationSeconds * 1000 : 30_000;
  const mockMode = Boolean(args.mock);
  const mockFail = Boolean(args['mock-fail']);

  if (!exchange || symbols.length === 0 || !strategyPath || !datasetParquet || !datasetMeta) {
    usage();
    process.exit(1);
  }

  if (!mockMode && !envBool(process.env.CORE_LIVE_WS_ENABLED || '0')) {
    console.error('CORE_LIVE_WS_ENABLED=0 (live WS disabled)');
    process.exit(1);
  }

  const startedAtMs = Date.now();
  const localArchiveDir = args['archive-dir'] || '/tmp/quantlab-canary-archive';
  let strategyConfig = null;
  if (strategyConfigRaw) {
    try {
      strategyConfig = JSON.parse(strategyConfigRaw);
    } catch (err) {
      console.error(`Invalid strategy config JSON: ${err.message || String(err)}`);
      process.exit(1);
    }
  }

  let executionEngine = null;
  if (executionMode === 'mock') {
    executionEngine = {
      onOrder(intent) {
        return {
          id: `fill_${Date.now()}`,
          side: intent.side,
          fillPrice: 1,
          qty: intent.qty,
          ts_event: intent.ts_event
        };
      },
      snapshot() { return { fills: [] }; }
    };
  }

  const runner = new LiveStrategyRunner({
    dataset: { parquet: datasetParquet, meta: datasetMeta },
    exchange,
    symbols,
    strategyPath,
    seed,
    orderingMode: orderingMode || undefined,
    strategyConfig,
    executionEngine,
    archiveWriter: mockMode ? new LocalArchiveWriter(localArchiveDir) : null,
    budgetConfig: {
      enabled: true,
      maxDurationEnabled: true,
      maxRunSeconds: Math.ceil(canaryDurationMs / 1000),
      maxEventsEnabled: false,
      maxDecisionRateEnabled: false
    }
  });

  const stopTimer = setTimeout(() => {
    runner.stop();
  }, canaryDurationMs);

  try {
    if (mockFail) {
      throw new Error('MOCK_FAIL');
    }
    const result = await runner.run({
      strategyPath,
      eventStream: mockMode ? mockEvents(20) : null
    });
    clearTimeout(stopTimer);

    if (mockMode) {
      await checkLocalArchive(result.live_run_id, localArchiveDir);
    } else {
      await checkArchive(result.live_run_id);
    }
    await checkAudit(result.live_run_id, startedAtMs);

    const payload = {
      event: 'canary_live',
      status: 'PASS',
      live_run_id: result.live_run_id,
      stop_reason: result.stop_reason,
      emitted_event_count: result.emitted_event_count,
      decision_count: result.decision_count,
      archive_dir: mockMode ? localArchiveDir : null,
      audit_dir: process.env.AUDIT_SPOOL_DIR || '/tmp/quantlab-audit'
    };
    await new Promise((resolve) => process.stdout.write(`${JSON.stringify(payload)}\n`, resolve));
    process.exit(0);
  } catch (err) {
    clearTimeout(stopTimer);
    try {
      runner.stop();
    } catch {
      // ignore cleanup errors
    }
    const payload = {
      event: 'canary_live',
      status: 'FAIL',
      error: err.message || String(err)
    };
    await new Promise((resolve) => process.stderr.write(`${JSON.stringify(payload)}\n`, resolve));
    process.exit(1);
  }
}

main();
