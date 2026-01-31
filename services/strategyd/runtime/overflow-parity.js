#!/usr/bin/env node
/**
 * overflow-parity.js
 *
 * Forces queue overflow in legacy and v2 modes and verifies parity.
 */

import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const runsDir = path.resolve(__dirname, '../runs');

const replaydUrl = process.env.REPLAYD_URL || 'http://localhost:3036';
const replaydToken = process.env.REPLAYD_TOKEN || 'test-secret';
const dataset = process.env.DATASET || 'bbo';
const symbol = process.env.SYMBOL || 'ADAUSDT';
const date = process.env.DATE || '2026-01-04';

const MAX_QUEUE = Number(process.env.STRATEGYD_MAX_QUEUE_CAPACITY || 200);
const BP_HIGH = Number(process.env.STRATEGYD_BACKPRESSURE_HIGH || 999999);
const BP_LOW = Number(process.env.STRATEGYD_BACKPRESSURE_LOW || 999999);
const YIELD_EVERY = Number(process.env.STRATEGYD_YIELD_EVERY || 1000);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForFile(filePath, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      await sleep(500);
    }
  }
  return false;
}

async function readManifest(runId) {
  const filePath = path.join(runsDir, `${runId}.json`);
  const raw = await fs.readFile(filePath, 'utf8');
  return { filePath, manifest: JSON.parse(raw) };
}

async function fetchMetrics(port) {
  const res = await fetch(`http://localhost:${port}/metrics`);
  if (!res.ok) throw new Error(`METRICS_HTTP_${res.status}`);
  return res.text();
}

function extractOverflowCount(metricsText) {
  const match = metricsText.match(/strategyd_queue_overflow_disconnects_total\s+(\d+)/);
  return match ? Number(match[1]) : 0;
}

async function runMode(modeLabel, enableV2, port) {
  const runId = `overflow_${modeLabel}_${Date.now()}`;
  const logPath = `/tmp/strategyd_overflow_${modeLabel}.log`;
  const env = {
    ...process.env,
    STRATEGY_RUNTIME_V2: enableV2 ? '1' : '0',
    STRATEGYD_PORT: String(port),
    REPLAYD_URL: replaydUrl,
    REPLAYD_TOKEN: replaydToken,
    DATASET: dataset,
    SYMBOL: symbol,
    DATE: date,
    SPEED: 'asap',
    AUTH_REQUIRED: 'false',
    RUN_ID: runId,
    STRATEGYD_MAX_QUEUE_CAPACITY: String(MAX_QUEUE),
    STRATEGYD_BACKPRESSURE_HIGH: String(BP_HIGH),
    STRATEGYD_BACKPRESSURE_LOW: String(BP_LOW),
    STRATEGYD_YIELD_EVERY: String(YIELD_EVERY)
  };

  const child = spawn('node', ['server.js'], {
    cwd: path.resolve(__dirname, '..'),
    env,
    stdio: ['ignore', 'pipe', 'pipe']
  });

  const logStream = createWriteStream(logPath, { flags: 'w' });
  child.stdout.on('data', (chunk) => logStream.write(chunk));
  child.stderr.on('data', (chunk) => logStream.write(chunk));

  const exitPromise = new Promise((resolve) => {
    child.on('exit', (code) => resolve(code ?? 0));
  });

  const manifestPath = path.join(runsDir, `${runId}.json`);
  const manifestReady = await Promise.race([
    waitForFile(manifestPath, 90000),
    exitPromise
  ]);

  const exists = await waitForFile(manifestPath, 5000);

  if (!manifestReady || !exists) {
    child.kill();
    throw new Error(`${modeLabel}: MANIFEST_TIMEOUT (log: ${logPath})`);
  }

  const { filePath, manifest } = await readManifest(runId);
  let metricsText = '';
  try {
    metricsText = await fetchMetrics(port);
  } catch {
    metricsText = '';
  }
  const overflowCount = extractOverflowCount(metricsText);

  child.kill();
  // leave logStream open; process exit will close it

  return {
    runId,
    filePath,
    manifest,
    overflowCount,
    logPath
  };
}

async function main() {
  try {
    const ports = [3211, 3311, 3411];
    const legacy = await runWithRetries('legacy', false, ports);
    const v2 = await runWithRetries('v2', true, ports.map((p) => p + 1));

    const legacyReason = legacy.manifest?.ended_reason;
    const v2Reason = v2.manifest?.ended_reason;

    const ok =
      legacyReason === 'queue_overflow' &&
      v2Reason === 'queue_overflow' &&
      legacy.overflowCount >= 1 &&
      v2.overflowCount >= 1;

    if (!ok) {
      console.error('[OVERFLOW-PARITY] FAIL');
      console.error({
        legacy: { runId: legacy.runId, ended_reason: legacyReason, overflowCount: legacy.overflowCount, path: legacy.filePath, log: legacy.logPath },
        v2: { runId: v2.runId, ended_reason: v2Reason, overflowCount: v2.overflowCount, path: v2.filePath, log: v2.logPath }
      });
      process.exit(1);
    }

    console.log('[OVERFLOW-PARITY] OK');
    console.log(`legacy run_id=${legacy.runId} ended_reason=${legacyReason} overflow=${legacy.overflowCount} path=${legacy.filePath}`);
    console.log(`v2 run_id=${v2.runId} ended_reason=${v2Reason} overflow=${v2.overflowCount} path=${v2.filePath}`);
  } catch (err) {
    console.error('[OVERFLOW-PARITY] FAIL', err.message);
    process.exit(1);
  }
}

main();

async function runWithRetries(modeLabel, enableV2, ports) {
  let lastError;
  for (const port of ports) {
    try {
      return await runMode(modeLabel, enableV2, port);
    } catch (err) {
      lastError = err;
      const logPath = `/tmp/strategyd_overflow_${modeLabel}.log`;
      try {
        const log = await fs.readFile(logPath, 'utf8');
        if (log.includes('EADDRINUSE')) {
          continue;
        }
      } catch {
        // ignore
      }
      throw err;
    }
  }
  throw lastError;
}
