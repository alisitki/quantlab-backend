import test from 'node:test';
import assert from 'node:assert/strict';

import { StrategyRuntime } from '../StrategyRuntime.js';
import { RiskManager } from '../../../risk/RiskManager.js';

function createExecutionEngine() {
  const fills = [];
  const positions = {};
  let totalRealizedPnl = 0;

  return {
    onEvent() {},
    onOrder(intent) {
      const symbol = String(intent.symbol || '').trim().toUpperCase();
      const qty = Number(intent.qty);
      const side = String(intent.side || '').trim().toUpperCase();
      const signedQty = side === 'BUY' ? qty : -qty;
      const current = positions[symbol] || {
        size: 0,
        avgEntryPrice: 100,
        realizedPnl: 0,
        unrealizedPnl: 0,
        currentPrice: 100,
      };
      const nextSize = current.size + signedQty;
      if (current.size !== 0 && nextSize === 0) {
        current.realizedPnl += 0;
      }
      current.size = nextSize;
      current.currentPrice = 100;
      positions[symbol] = current;
      const fill = {
        fill_id: `fill_${fills.length + 1}`,
        symbol,
        side,
        qty,
        fillPrice: 100,
        fillValue: 100 * qty,
        fee: 0,
        ts_event: intent.ts_event,
      };
      fills.push(fill);
      return fill;
    },
    snapshot() {
      return {
        positions,
        fills,
        totalRealizedPnl,
        totalUnrealizedPnl: 0,
        equity: 10000 + totalRealizedPnl,
        maxPositionValue: 100,
      };
    },
  };
}

async function runTwoEventRuntime(strategy, riskManager) {
  const runtime = new StrategyRuntime({
    dataset: {
      parquet: '/tmp/fake.parquet',
      meta: '/tmp/fake.meta',
    },
    strategy,
    strategyConfig: {},
    seed: 'test-seed',
    enableMetrics: false,
    enableCheckpoints: false,
  });
  const executionEngine = createExecutionEngine();
  runtime.attachExecutionEngine(executionEngine);
  runtime.attachRiskManager(riskManager);
  await runtime.init();
  runtime.setReplayRunId('runtime_position_effect_test');
  await runtime.processStream(
    (async function* generate() {
      yield {
        ts_event: 1700000000000000000n,
        seq: 1,
        cursor: 'cursor_1',
      };
      yield {
        ts_event: 1700000001000000000n,
        seq: 2,
        cursor: 'cursor_2',
      };
    })(),
  );
  return executionEngine.snapshot();
}

test('StrategyRuntime + RiskManager allow exit intents without false max-position rejects', async () => {
  let step = 0;
  const riskManager = new RiskManager({
    maxPositions: 1,
    cooldownEvents: 50,
    maxDailyLossPct: 0.02,
    stopLossPct: 0.005,
    takeProfitPct: 0.01,
  }, 10000);

  const snapshot = await runTwoEventRuntime({
    async onInit() {},
    async onEvent(_event, context) {
      step += 1;
      if (step === 1) {
        context.placeOrder({
          symbol: 'BTCUSDT',
          side: 'BUY',
          qty: 1,
          action: 'LONG',
          position_effect: 'OPEN',
          signal_action: 'LONG_OPEN',
        });
        return;
      }
      context.placeOrder({
        symbol: 'BTCUSDT',
        side: 'SELL',
        qty: 1,
        action: 'EXIT_LONG',
        position_effect: 'CLOSE',
        signal_action: 'LONG_CLOSE',
      });
    },
    async onFinalize() {},
    getState() {
      return {};
    },
    setState() {},
  }, riskManager);

  assert.equal(snapshot.fills.length, 2);
  assert.equal(snapshot.positions.BTCUSDT.size, 0);
  assert.equal(riskManager.getStats().rejectCount, 0);
});
