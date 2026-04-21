import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import { StrategyLoader } from '../../interface/StrategyLoader.js';


const STRATEGY_PATH = path.resolve('core/strategy/strategies/ReturnReversalV1Strategy.js');


function makeConfig() {
  return {
    binding_mode: 'PAPER_DIRECTIONAL_V1',
    family_id: 'return_reversal_v1',
    source_pack_id: 'pack_demo',
    source_decision_tier: 'PROMOTE_STRONG',
    exchange: 'binance',
    stream: 'trade',
    symbols: ['btcusdt'],
    source_family_report_path: '/tmp/family_return_reversal_report.json',
    orderQty: 1,
    params: {
      delta_ms_list: [1000, 5000],
      h_ms_list: [1000, 5000],
      tolerance_ms: 0,
    },
    selected_cell: {
      exchange: 'binance',
      stream: 'trade',
      symbol: 'btcusdt',
      delta_ms: 5000,
      h_ms: 1000,
      event_count: 1000,
      mean_product: -0.25,
      t_stat: -4.0,
    },
  };
}

function makeContext({ currentSize = 0 } = {}) {
  const orders = [];
  let size = currentSize;
  return {
    logger: { info() {} },
    stats: { processed: 2 },
    getExecutionState() {
      return {
        positions: size === 0 ? {} : {
          BTCUSDT: { size },
        },
      };
    },
    placeOrder(intent) {
      orders.push({ ...intent });
      size += intent.side === 'BUY' ? Number(intent.qty) : -Number(intent.qty);
    },
    orders,
  };
}

test('ReturnReversalV1Strategy opens SHORT on positive past return', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onEvent({
    ts_event: 1_700_000_000_000_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 100,
  }, ctx);
  await strategy.onEvent({
    ts_event: 1_700_000_005_000_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 101,
  }, ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].side, 'SELL');
  assert.equal(strategy.getState().last_signal.signal_direction, 'SHORT');
});

test('ReturnReversalV1Strategy opens LONG on negative past return', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onEvent({
    ts_event: 1_700_000_000_000_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 100,
  }, ctx);
  await strategy.onEvent({
    ts_event: 1_700_000_005_000_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 99,
  }, ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].side, 'BUY');
  assert.equal(strategy.getState().last_signal.signal_direction, 'LONG');
});

test('ReturnReversalV1Strategy validates family id', async () => {
  await assert.rejects(
    StrategyLoader.loadFromFile(STRATEGY_PATH, {
      config: {
        ...makeConfig(),
        family_id: 'momentum_v1',
      },
      autoAdapt: true,
    }),
    /RETURN_REVERSAL_V1_CONFIG_ERROR/
  );
});
