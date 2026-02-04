#!/usr/bin/env node
/**
 * Live run smoke test with mock stream and local archive writer
 */

import { mkdir, writeFile, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { LiveStrategyRunner } from '../core/strategy/live/LiveStrategyRunner.js';

const OUT_DIR = '/tmp/live-run-archive';

class LocalArchiveWriter {
  async write(run) {
    const base = join(OUT_DIR, `replay_runs/replay_run_id=${run.replay_run_id}`);
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

function createMockStrategy() {
  return {
    id: 'mock-live',
    version: '1.0.0',
    async onInit() {},
    async onEvent(_event, ctx) {
      ctx.placeOrder({ symbol: 'BTCUSDT', side: 'BUY', qty: 1 });
    },
    async onFinalize() {},
    getState() { return {}; }
  };
}

class MockExecutionEngine {
  onOrder(intent) {
    return {
      id: 'fill_1',
      side: intent.side,
      fillPrice: 1,
      qty: intent.qty,
      ts_event: intent.ts_event
    };
  }
  snapshot() { return { fills: [] }; }
}

async function main() {
  const runner = new LiveStrategyRunner({
    dataset: { parquet: 'mock', meta: 'mock', stream: 'bbo' },
    exchange: 'binance',
    symbols: ['BTCUSDT'],
    strategy: createMockStrategy(),
    executionEngine: new MockExecutionEngine(),
    archiveWriter: new LocalArchiveWriter()
  });

  const result = await runner.run({ eventStream: mockEvents(20), handleSignals: false });

  const dir = join(OUT_DIR, `replay_runs/replay_run_id=${result.live_run_id}`);
  const decisions = await readFile(join(dir, 'decisions.jsonl'), 'utf-8');
  const lines = decisions.trim().split('\n');

  if (lines.length !== 20) {
    console.error('FAIL: decision lines != 20');
    process.exit(1);
  }

  console.log('PASS');
}

main().catch((err) => {
  console.error('FAIL', err.message);
  process.exit(1);
});
