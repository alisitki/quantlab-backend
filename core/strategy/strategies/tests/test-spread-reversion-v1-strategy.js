import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import { StrategyLoader } from '../../interface/StrategyLoader.js';


const STRATEGY_PATH = path.resolve('core/strategy/strategies/SpreadReversionV1Strategy.js');


function makeConfig() {
  return {
    binding_mode: 'OBSERVE_ONLY',
    family_id: 'spread_reversion_v1',
    source_pack_id: 'pack_demo',
    source_decision_tier: 'PROMOTE_STRONG',
    exchange: 'bybit',
    stream: 'bbo',
    symbols: ['bnbusdt'],
    source_family_report_path: '/tmp/family_spread_reversion_report.json',
    window: '20260123..20260123',
    params: {
      delta_ms_list: [1000, 5000],
      h_ms_list: [1000, 5000],
      tolerance_ms: 0,
    },
    selected_cell: {
      exchange: 'bybit',
      stream: 'bbo',
      symbol: 'bnbusdt',
      delta_ms: 5000,
      h_ms: 5000,
      mean_product: -0.001929579253559,
      t_stat: -9.346773529701917,
    },
  };
}


function makeContext() {
  return {
    logger: {
      info() {},
    },
  };
}


test('SpreadReversionV1Strategy loads through StrategyLoader and emits family signal state', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

  await strategy.onEvent({
    ts_event: 1_700_000_000_000_000_000n,
    symbol: 'BNBUSDT',
    bid_price: 100,
    ask_price: 101,
  }, ctx);
  await strategy.onEvent({
    ts_event: 1_700_000_005_000_000_000n,
    symbol: 'BNBUSDT',
    bid_price: 100,
    ask_price: 102,
  }, ctx);

  const state = strategy.getState();
  assert.equal(state.family_id, 'spread_reversion_v1');
  assert.equal(state.matched_bbo_events, 2);
  assert.equal(state.signal_event_count, 1);
  assert.equal(state.last_signal.signal_type, 'SPREAD_WIDENED_EXPECT_REVERSION');
  assert.equal(state.last_signal.delta_ms, 5000);
  assert.equal(state.last_signal.h_ms, 5000);
});


test('SpreadReversionV1Strategy rejects ambiguous config', async () => {
  await assert.rejects(
    async () => StrategyLoader.loadFromFile(STRATEGY_PATH, {
      config: {
        family_id: 'spread_reversion_v1',
        exchange: 'bybit',
        stream: 'bbo',
        symbols: ['bnbusdt', 'ethusdt'],
        params: {
          delta_ms_list: [1000],
          h_ms_list: [1000],
          tolerance_ms: 0,
        },
        selected_cell: {
          exchange: 'bybit',
          stream: 'bbo',
          symbol: 'bnbusdt',
          delta_ms: 1000,
          h_ms: 1000,
        },
      },
      autoAdapt: true,
    }),
    /exactly one symbol required/,
  );
});
