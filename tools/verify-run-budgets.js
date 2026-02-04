#!/usr/bin/env node
/**
 * Verify run budgets trigger (max_events)
 */

import { LiveStrategyRunner } from '../core/strategy/live/LiveStrategyRunner.js';

class DummyExecutionEngine {
  onEvent() {}
  snapshot() { return { totalRealizedPnl: 0, totalUnrealizedPnl: 0, equity: 0 }; }
}

function createMockStrategy() {
  return {
    id: 'budget-test',
    version: '1.0.0',
    async onInit() {},
    async onEvent() {},
    async onFinalize() {},
    getState() { return {}; }
  };
}

async function* mockEvents(count = 10) {
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

async function main() {
  const runner = new LiveStrategyRunner({
    dataset: { parquet: 'mock', meta: 'mock', stream: 'bbo' },
    exchange: 'binance',
    symbols: ['BTCUSDT'],
    strategy: createMockStrategy(),
    executionEngine: new DummyExecutionEngine(),
    archiveWriter: { write: async () => {} },
    guardConfig: { enabled: false },
    budgetConfig: {
      enabled: true,
      maxEventsEnabled: true,
      maxEvents: 3,
      maxDurationEnabled: false,
      maxDecisionRateEnabled: false
    }
  });

  const result = await runner.run({ eventStream: mockEvents(10), handleSignals: false });

  if (result.stop_reason !== 'BUDGET_EXCEEDED') {
    console.error('FAIL: budget did not stop run');
    process.exit(1);
  }

  console.log('PASS');
}

main().catch((err) => {
  console.error('FAIL', err.message || String(err));
  process.exit(1);
});
