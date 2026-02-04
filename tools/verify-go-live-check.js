#!/usr/bin/env node
import { writeFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { createObserverApp } from '../core/observer/index.js';

async function runCommand(cmd, args, env) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { env });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => { stdout += d.toString(); });
    child.stderr.on('data', (d) => { stderr += d.toString(); });
    child.on('close', (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

function createMockStrategySource() {
  return `export default {
  id: 'mock-go-live',
  version: '1.0.0',
  async onInit() {},
  async onEvent(_event, ctx) {
    ctx.placeOrder({ symbol: 'BTCUSDT', side: 'BUY', qty: 1 });
  },
  async onFinalize() {},
  getState() { return {}; }
};\n`;
}

async function startObserverServer() {
  const app = createObserverApp();
  const server = await new Promise((resolve) => {
    const srv = app.listen(0, () => resolve(srv));
  });
  const address = server.address();
  const port = typeof address === 'string' ? address : address.port;
  return { server, port };
}

async function main() {
  const tmpDir = '/tmp/quantlab-go-live-test';
  const auditDir = join(tmpDir, 'audit');
  const strategyPath = join(tmpDir, 'mock-strategy.js');
  await mkdir(tmpDir, { recursive: true });
  await mkdir(auditDir, { recursive: true });
  await writeFile(strategyPath, createMockStrategySource());

  const { server, port } = await startObserverServer();

  const baseEnv = {
    ...process.env,
    OBSERVER_TOKEN: 'test-token',
    OBSERVER_URL: `http://127.0.0.1:${port}`,
    RUN_ARCHIVE_ENABLED: '0',
    GO_LIVE_EXCHANGE: 'binance',
    GO_LIVE_SYMBOLS: 'BTCUSDT',
    GO_LIVE_STRATEGY: strategyPath,
    GO_LIVE_DATASET_PARQUET: 'mock',
    GO_LIVE_DATASET_META: 'mock',
    GO_LIVE_CANARY_MOCK: '1',
    AUDIT_SPOOL_DIR: auditDir
  };

  const pass = await runCommand('node', ['tools/go-live-check.js'], baseEnv);
  const passOk = pass.code === 0;

  const failEnv = { ...baseEnv, GO_LIVE_CANARY_MOCK_FAIL: '1' };
  const fail = await runCommand('node', ['tools/go-live-check.js'], failEnv);
  const failOk = fail.code === 1;

  await new Promise((resolve) => server.close(() => resolve()));

  if (!passOk) {
    console.error('FAIL: PASS scenario failed');
    console.error(pass.stdout || pass.stderr);
    process.exit(1);
  }

  if (!failOk) {
    console.error('FAIL: FAIL scenario did not fail');
    console.error(fail.stdout || fail.stderr);
    process.exit(1);
  }

  console.log('PASS');
}

main().catch((err) => {
  console.error('FAIL', err.message || String(err));
  process.exit(1);
});
