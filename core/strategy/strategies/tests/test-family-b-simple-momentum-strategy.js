import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import { StrategyLoader } from '../../interface/StrategyLoader.js';


const STRATEGY_PATH = path.resolve('core/strategy/strategies/FamilyBSimpleMomentumStrategy.js');


function makeConfig() {
  return {
    binding_mode: 'PAPER_DIRECTIONAL_V1',
    family_id: 'family_b_simple_momentum',
    source_pack_id: 'pack_demo',
    source_decision_tier: 'PROMOTE',
    exchange: 'binance',
    stream: 'trade',
    symbols: ['ethusdt'],
    source_family_report_path: '/tmp/family_B_report.json',
    orderQty: 1,
    params: {
      lookback_minutes: 1,
      forward_minutes: 1,
      signal_quantile: 0.9,
      min_support: 200,
    },
    selected_cell: {
      exchange: 'binance',
      stream: 'trade',
      symbol: 'ethusdt',
      lookback_minutes: 1,
      forward_minutes: 1,
      signal_support: 500,
      lookback_quantile_threshold: 0.004,
      mean_forward_return: 0.001,
      t_stat: 3.5,
    },
  };
}

function makeContext() {
  const orders = [];
  let size = 0;
  return {
    logger: { info() {} },
    stats: { processed: 3 },
    getExecutionState() {
      return {
        positions: size === 0 ? {} : {
          ETHUSDT: { size },
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

test('FamilyBSimpleMomentumStrategy opens LONG on strong lookback continuation', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onEvent({ ts_event: 1_700_000_000_000_000_000n, symbol: 'ETHUSDT', stream: 'trade', price: 100 }, ctx);
  await strategy.onEvent({ ts_event: 1_700_000_060_000_000_000n, symbol: 'ETHUSDT', stream: 'trade', price: 101 }, ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].side, 'BUY');
  assert.equal(strategy.getState().last_signal.signal_direction, 'LONG');
});

test('FamilyBSimpleMomentumStrategy never opens SHORT on weak lookback return', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onEvent({ ts_event: 1_700_000_000_000_000_000n, symbol: 'ETHUSDT', stream: 'trade', price: 100 }, ctx);
  await strategy.onEvent({ ts_event: 1_700_000_060_000_000_000n, symbol: 'ETHUSDT', stream: 'trade', price: 100.1 }, ctx);

  assert.equal(ctx.orders.length, 0);
  assert.equal(strategy.getState().last_action.action, 'STAY_FLAT');
});

test('FamilyBSimpleMomentumStrategy closes long after horizon when signal disappears', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onEvent({ ts_event: 1_700_000_000_000_000_000n, symbol: 'ETHUSDT', stream: 'trade', price: 100 }, ctx);
  await strategy.onEvent({ ts_event: 1_700_000_060_000_000_000n, symbol: 'ETHUSDT', stream: 'trade', price: 101 }, ctx);
  await strategy.onEvent({ ts_event: 1_700_000_180_000_000_000n, symbol: 'ETHUSDT', stream: 'trade', price: 101 }, ctx);

  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].side, 'SELL');
  assert.equal(strategy.getState().last_action.action, 'LONG_CLOSE');
});
