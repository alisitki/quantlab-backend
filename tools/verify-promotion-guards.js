#!/usr/bin/env node
/**
 * Verify promotion guards trigger on loss streak
 */

import { LiveStrategyRunner } from '../core/strategy/live/LiveStrategyRunner.js';

class DummyExecutionEngine {
  #pnl = 0;
  onEvent() {
    this.#pnl -= 1;
  }
  snapshot() {
    return {
      totalRealizedPnl: this.#pnl,
      totalUnrealizedPnl: 0,
      equity: this.#pnl
    };
  }
}

function createMockStrategy() {
  return {
    id: 'guard-test',
    version: '1.0.0',
    async onInit() {},
    async onEvent() {},
    async onFinalize() {},
    getState() { return {}; }
  };
}

async function* mockEvents(count = 5) {
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
    guardConfig: {
      enabled: true,
      replayParityEnabled: false,
      minDecisionEnabled: false,
      maxLossEnabled: false,
      lossStreakEnabled: true,
      lossStreak: 2
    }
  });

  const result = await runner.run({ eventStream: mockEvents(5), handleSignals: false });

  if (result.stop_reason !== 'PROMOTION_GUARD_FAIL') {
    console.error('FAIL: guard did not stop run');
    process.exit(1);
  }

  console.log('PASS');
}

main().catch((err) => {
  console.error('FAIL', err.message || String(err));
  process.exit(1);
});
