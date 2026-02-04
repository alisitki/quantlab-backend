#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(new URL('../core/package.json', import.meta.url));
const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const dotenv = require('dotenv');

dotenv.config({ path: new URL('../core/.env', import.meta.url).pathname });

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
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

async function runCommand(cmd, args, env, { stream = false, prefix = '', timeoutMs = null } = {}) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { env });
    let timeout = null;
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => {
      const text = d.toString();
      stdout += text;
      if (stream) process.stdout.write(prefix ? `[${prefix}] ${text}` : text);
    });
    child.stderr.on('data', (d) => {
      const text = d.toString();
      stderr += text;
      if (stream) process.stderr.write(prefix ? `[${prefix}] ${text}` : text);
    });
    if (timeoutMs && Number.isFinite(timeoutMs) && timeoutMs > 0) {
      timeout = setTimeout(() => {
        try { child.kill('SIGTERM'); } catch {}
        setTimeout(() => {
          try { child.kill('SIGKILL'); } catch {}
        }, 2000);
      }, timeoutMs);
    }
    child.on('close', (code) => {
      if (timeout) clearTimeout(timeout);
      resolve({ code, stdout, stderr });
    });
  });
}

function jsonFail(step, reason, details) {
  const payload = {
    go_live: false,
    failed_step: step,
    reason,
    details
  };
  console.log(JSON.stringify(payload));
  process.exitCode = 1;
}

function jsonSuccess() {
  console.log(JSON.stringify({
    go_live: true,
    checks: {
      config: 'ok',
      self_test: 'ok',
      observer: 'ok',
      canary: 'pass',
      archive: 'ok',
      audit: 'ok'
    }
  }));
  process.exitCode = 0;
}

function pickReason(output, fallback) {
  const text = (output || '').trim();
  if (!text) return fallback;
  const lines = text.split('\n').filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i];
    try {
      const parsed = JSON.parse(line);
      if (parsed?.error) return parsed.error;
      if (parsed?.reason) return parsed.reason;
    } catch {
      // ignore
    }
  }
  return fallback;
}

async function checkObserverHealth(baseUrl, token) {
  const res = await fetch(`${baseUrl}/observer/health`, {
    method: 'GET',
    headers: { Authorization: `Bearer ${token}` }
  });
  if (res.status !== 200) {
    const body = await res.text();
    throw new Error(`observer health status=${res.status} body=${body}`);
  }
}

async function checkArchiveS3(runId) {
  if (!envBool(process.env.RUN_ARCHIVE_ENABLED || '0')) {
    throw new Error('RUN_ARCHIVE_ENABLED=0');
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
    `${prefix}/decisions.jsonl`
  ];

  for (const key of keys) {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
  }
}

async function checkArchiveLocal(runId, archiveDir) {
  const base = join(archiveDir, `replay_runs/replay_run_id=${runId}`);
  const files = await readdir(base);
  const required = new Set(['manifest.json', 'decisions.jsonl']);
  for (const f of required) {
    if (!files.includes(f)) {
      throw new Error(`Local archive missing: ${f}`);
    }
  }
}

async function checkAudit(runId, startedAtMs) {
  const spoolDir = process.env.AUDIT_SPOOL_DIR || '/tmp/quantlab-audit';
  const d = new Date(startedAtMs);
  const dateKey = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`;
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

async function main() {
  const args = parseArgs(process.argv);
  const exchange = args.exchange || process.env.GO_LIVE_EXCHANGE;
  const symbols = args.symbols || process.env.GO_LIVE_SYMBOLS;
  const strategyPath = args.strategy || process.env.GO_LIVE_STRATEGY;
  const datasetParquet = args['dataset-parquet'] || process.env.GO_LIVE_DATASET_PARQUET;
  const datasetMeta = args['dataset-meta'] || process.env.GO_LIVE_DATASET_META;
  const seed = args.seed || process.env.GO_LIVE_SEED || '';
  const strategyConfig = args['strategy-config'] || process.env.GO_LIVE_STRATEGY_CONFIG;
  const orderingMode = args['ordering-mode'] || process.env.GO_LIVE_ORDERING_MODE;
  const executionMode = args['execution-mode'] || process.env.GO_LIVE_EXECUTION_MODE;
  const canarySeconds = args['duration-seconds'] || process.env.GO_LIVE_CANARY_SECONDS || '30';
  const canaryMock = Boolean(args['canary-mock'] || envBool(process.env.GO_LIVE_CANARY_MOCK || '0'));
  const canaryMockFail = Boolean(args['canary-mock-fail'] || envBool(process.env.GO_LIVE_CANARY_MOCK_FAIL || '0'));

  if (!exchange || !symbols || !strategyPath || !datasetParquet || !datasetMeta) {
    jsonFail('config', 'MISSING_CANARY_CONFIG', 'GO_LIVE_EXCHANGE, GO_LIVE_SYMBOLS, GO_LIVE_STRATEGY, GO_LIVE_DATASET_PARQUET, GO_LIVE_DATASET_META required');
    return;
  }

  const observerToken = process.env.OBSERVER_TOKEN || '';
  if (!observerToken) {
    jsonFail('observer', 'MISSING_OBSERVER_TOKEN', 'OBSERVER_TOKEN is required');
    return;
  }

  const observerBase = process.env.OBSERVER_URL
    ? process.env.OBSERVER_URL.replace(/\/$/, '')
    : `http://127.0.0.1:${process.env.OBSERVER_PORT || 9150}`;

  const startedAtMs = Date.now();

  const configStep = await runCommand('node', ['core/release/ConfigCheck.js'], process.env);
  if (configStep.code !== 0) {
    jsonFail('config', pickReason(configStep.stderr || configStep.stdout, 'CONFIG_CHECK_FAILED'), configStep.stderr || configStep.stdout);
    return;
  }

  const selfStep = await runCommand('node', ['core/release/SelfTest.js'], process.env);
  if (selfStep.code !== 0) {
    jsonFail('self_test', pickReason(selfStep.stderr || selfStep.stdout, 'SELF_TEST_FAILED'), selfStep.stderr || selfStep.stdout);
    return;
  }

  try {
    await checkObserverHealth(observerBase, observerToken);
  } catch (err) {
    jsonFail('observer', 'OBSERVER_HEALTH_FAIL', err.message || String(err));
    return;
  }

  const canaryArgs = [
    'tools/run-canary-live.js',
    '--exchange', exchange,
    '--symbols', symbols,
    '--strategy', strategyPath,
    '--dataset-parquet', datasetParquet,
    '--dataset-meta', datasetMeta
  ];
  if (seed) {
    canaryArgs.push('--seed', seed);
  }
  if (strategyConfig) {
    canaryArgs.push('--strategy-config', strategyConfig);
  }
  if (orderingMode) {
    canaryArgs.push('--ordering-mode', orderingMode);
  }
  if (executionMode) {
    canaryArgs.push('--execution-mode', executionMode);
  }
  if (canarySeconds) {
    canaryArgs.push('--duration-seconds', canarySeconds);
  }
  if (canaryMock) {
    canaryArgs.push('--mock');
    if (canaryMockFail) canaryArgs.push('--mock-fail');
  }

  const canaryTimeoutMs = Math.max(5, Number(canarySeconds)) * 1000 + 5000;
  const canaryStep = await runCommand('node', canaryArgs, process.env, {
    stream: true,
    prefix: 'canary',
    timeoutMs: canaryTimeoutMs
  });
  if (canaryStep.code !== 0) {
    const reason = canaryStep.code === 124
      ? 'CANARY_TIMEOUT'
      : pickReason(canaryStep.stderr || canaryStep.stdout, 'CANARY_FAILED');
    jsonFail('canary', reason, canaryStep.stderr || canaryStep.stdout);
    return;
  }

  let canaryPayload = null;
  const canaryLines = (canaryStep.stdout || '').split('\n').filter(Boolean);
  for (let i = canaryLines.length - 1; i >= 0; i -= 1) {
    const line = canaryLines[i];
    try {
      const parsed = JSON.parse(line);
      if (parsed?.event === 'canary_live') {
        canaryPayload = parsed;
        break;
      }
    } catch {
      // ignore
    }
  }

  if (!canaryPayload || canaryPayload.status !== 'PASS') {
    jsonFail('canary', 'CANARY_OUTPUT_INVALID', canaryStep.stdout || 'Missing canary_live payload');
    return;
  }

  const runId = canaryPayload.live_run_id;
  if (!runId) {
    jsonFail('canary', 'CANARY_RUN_ID_MISSING', canaryStep.stdout || 'Missing live_run_id');
    return;
  }

  try {
    if (canaryPayload.archive_dir) {
      await checkArchiveLocal(runId, canaryPayload.archive_dir);
    } else {
      await checkArchiveS3(runId);
    }
  } catch (err) {
    jsonFail('archive', 'ARCHIVE_CHECK_FAILED', err.message || String(err));
    return;
  }

  try {
    await checkAudit(runId, startedAtMs);
  } catch (err) {
    jsonFail('audit', 'AUDIT_CHECK_FAILED', err.message || String(err));
    return;
  }

  jsonSuccess();
}

main();
