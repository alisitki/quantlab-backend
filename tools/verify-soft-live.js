#!/usr/bin/env node
import { readFile, readdir } from 'node:fs/promises';
import { RunArchiveReader } from '../core/run-archive/RunArchiveReader.js';
import { createRequire } from 'node:module';

const require = createRequire(new URL('../core/package.json', import.meta.url));
const dotenv = require('dotenv');

dotenv.config({ path: new URL('../core/.env', import.meta.url).pathname });

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

async function readAudit(runId) {
  const spoolDir = process.env.AUDIT_SPOOL_DIR || '/tmp/quantlab-audit';
  const files = [];
  const stack = [spoolDir];
  while (stack.length > 0) {
    const dir = stack.pop();
    let entries = [];
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const full = `${dir}/${entry.name}`;
      if (entry.isDirectory()) stack.push(full);
      else files.push(full);
    }
  }
  let startFound = false;
  let stopFound = false;
  for (const file of files) {
    if (!file.endsWith('.jsonl')) continue;
    const content = await readFile(file, 'utf-8');
    const lines = content.split('\n').filter(Boolean);
    for (const line of lines) {
      try {
        const parsed = JSON.parse(line);
        if (parsed?.metadata?.live_run_id === runId && parsed?.action === 'RUN_START') startFound = true;
        if (parsed?.metadata?.live_run_id === runId && parsed?.action === 'RUN_STOP') stopFound = true;
      } catch {
        // ignore
      }
    }
  }
  return { startFound, stopFound };
}

async function main() {
  let meta;
  try {
    meta = JSON.parse(await readFile('/tmp/quantlab-soft-live.json', 'utf-8'));
  } catch {
    fail('Missing /tmp/quantlab-soft-live.json (run-soft-live not completed)');
  }

  const runId = meta.live_run_id;
  if (!runId) fail('Missing live_run_id');

  const audit = await readAudit(runId);
  if (!audit.startFound || !audit.stopFound) {
    fail('Audit RUN_START/RUN_STOP not found');
  }

  if (process.env.RUN_ARCHIVE_ENABLED === '1') {
    const reader = RunArchiveReader.fromEnv();
    const stats = await reader.getStats(runId);
    if (!stats || typeof stats.emitted_event_count !== 'number') {
      fail('Archive stats missing emitted_event_count');
    }
  }

  console.log('PASS');
}

main().catch((err) => fail(err.message || String(err)));
