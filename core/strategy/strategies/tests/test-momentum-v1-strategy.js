import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import { StrategyLoader } from '../../interface/StrategyLoader.js';


const STRATEGY_PATH = path.resolve('core/strategy/strategies/MomentumV1Strategy.js');


function makeConfig() {
  return {
    binding_mode: 'PAPER_DIRECTIONAL_V1',
    family_id: 'momentum_v1',
    source_pack_id: 'pack_demo',
    source_decision_tier: 'PROMOTE_STRONG',
    exchange: 'binance',
    stream: 'trade',
    symbols: ['btcusdt'],
    source_family_report_path: '/tmp/family_momentum_report.json',
    window: '20260107..20260107',
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
      mean_product: 0.25,
      t_stat: 4.0,
    },
  };
}

function makeConfigWithOverrides(overrides = {}) {
  const base = makeConfig();
  return {
    ...base,
    ...overrides,
    params: {
      ...base.params,
      ...(overrides.params || {}),
    },
    selected_cell: {
      ...base.selected_cell,
      ...(overrides.selected_cell || {}),
    },
  };
}


function makeContext({ currentSize = 0 } = {}) {
  const orders = [];
  let size = currentSize;
  return {
    logger: {
      info() {},
    },
    stats: {
      processed: 2,
    },
    getExecutionState() {
      return {
        positions: size === 0 ? {} : {
          BTCUSDT: {
            size,
          },
        },
      };
    },
    placeOrder(intent) {
      orders.push({ ...intent });
      if (intent.side === 'BUY') {
        size += Number(intent.qty);
      } else {
        size -= Number(intent.qty);
      }
      return {
        fill_id: `fill_${orders.length}`,
        symbol: intent.symbol,
        side: intent.side,
        qty: intent.qty,
        fillPrice: 100,
        fillValue: 100 * Number(intent.qty),
        fee: 0.04,
        ts_event: 1_700_000_005_000_000_000n,
      };
    },
    setSize(nextSize) {
      size = Number(nextSize);
    },
    getSize() {
      return size;
    },
    orders,
  };
}


test('MomentumV1Strategy loads and opens LONG on positive continuation signal', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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
  assert.deepEqual(ctx.orders[0], {
    symbol: 'BTCUSDT',
    side: 'BUY',
    qty: 1,
    action: 'LONG',
    position_effect: 'OPEN',
    signal_action: 'LONG_OPEN',
  });
  const state = strategy.getState();
  assert.equal(state.last_signal.signal_direction, 'LONG');
  assert.equal(state.last_action.action, 'LONG_OPEN');
  assert.equal(state.order_event_count, 1);
  assert.equal(state.commit_until_ts_event, '1700000006000000000');
});


test('MomentumV1Strategy holds same-side position without pyramiding', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext({ currentSize: 1 });
  await strategy.onInit?.(ctx);

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

  assert.equal(ctx.orders.length, 0);
  const state = strategy.getState();
  assert.equal(state.last_signal.signal_direction, 'LONG');
  assert.equal(state.last_action.action, 'HOLD_LONG');
});

test('MomentumV1Strategy derives deterministic confidence buckets and thresholds from selected_cell inputs', async () => {
  const cases = [
    {
      name: 'LOW',
      config: makeConfigWithOverrides({
        selected_cell: {
          event_count: 300,
          t_stat: 3.0,
        },
      }),
      expectedRatio: 1.5,
      expectedThreshold: 10,
    },
    {
      name: 'MEDIUM',
      config: makeConfigWithOverrides({
        selected_cell: {
          event_count: 2000,
          t_stat: 12.0,
        },
      }),
      expectedRatio: 6,
      expectedThreshold: 7.5,
    },
    {
      name: 'HIGH',
      config: makeConfigWithOverrides({
        selected_cell: {
          event_count: 5000,
          t_stat: 40.0,
        },
      }),
      expectedRatio: 20,
      expectedThreshold: 5,
    },
  ];

  for (const entry of cases) {
    const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
      config: entry.config,
      autoAdapt: true,
    });
    const state = strategy.getState();
    assert.equal(state.confidence_bucket, entry.name);
    assert.equal(state.confidence_ratio, entry.expectedRatio);
    assert.equal(state.required_abs_past_return_bps, entry.expectedThreshold);
  }
});

test('MomentumV1Strategy does not open on weak LOW-confidence signal below threshold', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfigWithOverrides({
      selected_cell: {
        event_count: 300,
        t_stat: 3.0,
      },
    }),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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
    price: 100.05,
  }, ctx);

  assert.equal(ctx.orders.length, 0);
  const state = strategy.getState();
  assert.equal(state.confidence_bucket, 'LOW');
  assert.equal(state.required_abs_past_return_bps, 10);
  assert.equal(state.last_signal.signal_direction, 'LONG');
  assert.equal(state.last_signal.required_abs_past_return_bps, 10);
  assert.equal(state.last_signal.min_edge_passed, false);
  assert.equal(state.last_action.action, 'STAY_FLAT');
  assert.equal(state.last_action.min_edge_passed, false);
});

test('MomentumV1Strategy opens on strong HIGH-confidence signal above reduced threshold', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfigWithOverrides({
      selected_cell: {
        event_count: 5000,
        t_stat: 40.0,
      },
    }),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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
    price: 100.06,
  }, ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].signal_action, 'LONG_OPEN');
  const state = strategy.getState();
  assert.equal(state.confidence_bucket, 'HIGH');
  assert.equal(state.required_abs_past_return_bps, 5);
  assert.equal(state.last_signal.required_abs_past_return_bps, 5);
  assert.equal(state.last_signal.min_edge_passed, true);
  assert.equal(state.last_action.action, 'LONG_OPEN');
});


test('MomentumV1Strategy closes long on neutral signal without active commit horizon', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext({ currentSize: 1 });
  await strategy.onInit?.(ctx);

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
    price: 100,
  }, ctx);

  assert.equal(ctx.orders.length, 1);
  assert.deepEqual(ctx.orders[0], {
    symbol: 'BTCUSDT',
    side: 'SELL',
    qty: 1,
    action: 'EXIT_LONG',
    position_effect: 'CLOSE',
    signal_action: 'LONG_CLOSE',
  });
  const state = strategy.getState();
  assert.equal(state.last_signal.signal_direction, 'FLAT');
  assert.equal(state.last_action.action, 'LONG_CLOSE');
});

test('MomentumV1Strategy blocks weak opposite reversal signals below confidence-adjusted threshold', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfigWithOverrides({
      selected_cell: {
        event_count: 2000,
        t_stat: 12.0,
      },
    }),
    autoAdapt: true,
  });
  const ctx = makeContext({ currentSize: 1 });
  await strategy.onInit?.(ctx);

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
    price: 99.94,
  }, ctx);

  assert.equal(ctx.orders.length, 0);
  const state = strategy.getState();
  assert.equal(state.confidence_bucket, 'MEDIUM');
  assert.equal(state.required_abs_past_return_bps, 7.5);
  assert.equal(state.last_signal.signal_direction, 'SHORT');
  assert.equal(state.last_signal.required_abs_past_return_bps, 7.5);
  assert.equal(state.last_signal.min_edge_passed, false);
  assert.equal(state.last_action.action, 'HOLD_LONG');
});


test('MomentumV1Strategy holds LONG before horizon maturity on flat signal', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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
  await strategy.onEvent({
    ts_event: 1_700_000_005_500_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 100,
  }, ctx);

  assert.equal(ctx.orders.length, 1);
  assert.deepEqual(ctx.orders[0], {
    symbol: 'BTCUSDT',
    side: 'BUY',
    qty: 1,
    action: 'LONG',
    position_effect: 'OPEN',
    signal_action: 'LONG_OPEN',
  });
  const state = strategy.getState();
  assert.equal(state.last_signal.signal_direction, 'FLAT');
  assert.equal(state.last_action.action, 'HOLD_LONG');
  assert.equal(state.last_action.commit_active, true);
});


test('MomentumV1Strategy holds LONG before horizon maturity on opposite signal', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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
  await strategy.onEvent({
    ts_event: 1_700_000_005_500_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 99,
  }, ctx);

  assert.equal(ctx.orders.length, 1);
  const state = strategy.getState();
  assert.equal(state.last_signal.signal_direction, 'SHORT');
  assert.equal(state.last_action.action, 'HOLD_LONG');
  assert.equal(state.last_action.commit_active, true);
});


test('MomentumV1Strategy closes LONG after horizon maturity on flat signal', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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
  await strategy.onEvent({
    ts_event: 1_700_000_006_000_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 100,
  }, ctx);

  assert.equal(ctx.orders.length, 2);
  assert.deepEqual(ctx.orders[1], {
    symbol: 'BTCUSDT',
    side: 'SELL',
    qty: 1,
    action: 'EXIT_LONG',
    position_effect: 'CLOSE',
    signal_action: 'LONG_CLOSE',
  });
  const state = strategy.getState();
  assert.equal(state.last_signal.signal_direction, 'FLAT');
  assert.equal(state.last_action.action, 'LONG_CLOSE');
  assert.equal(state.commit_until_ts_event, null);
});


test('MomentumV1Strategy reverses after horizon maturity and resets commit horizon', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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
  await strategy.onEvent({
    ts_event: 1_700_000_006_000_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 99,
  }, ctx);
  await strategy.onEvent({
    ts_event: 1_700_000_006_500_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 101,
  }, ctx);

  assert.equal(ctx.orders.length, 2);
  assert.deepEqual(ctx.orders[1], {
    symbol: 'BTCUSDT',
    side: 'SELL',
    qty: 2,
    action: 'EXIT_LONG',
    position_effect: 'REVERSE',
    signal_action: 'LONG_TO_SHORT_REVERSAL',
  });
  const state = strategy.getState();
  assert.equal(state.last_action.action, 'HOLD_SHORT');
  assert.equal(state.last_action.commit_active, true);
  assert.equal(state.commit_until_ts_event, '1700000007000000000');
});


test('MomentumV1Strategy clears stale commit horizon after external flatten', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

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

  ctx.setSize(0);

  await strategy.onEvent({
    ts_event: 1_700_000_005_500_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price: 99,
  }, ctx);

  assert.equal(ctx.orders.length, 2);
  assert.deepEqual(ctx.orders[1], {
    symbol: 'BTCUSDT',
    side: 'SELL',
    qty: 1,
    action: 'SHORT',
    position_effect: 'OPEN',
    signal_action: 'SHORT_OPEN',
  });
  const state = strategy.getState();
  assert.equal(state.last_action.action, 'SHORT_OPEN');
  assert.equal(state.commit_until_ts_event, '1700000006500000000');
});


test('MomentumV1Strategy rejects non-directional or unsupported config', async () => {
  await assert.rejects(
    async () => StrategyLoader.loadFromFile(STRATEGY_PATH, {
      config: {
        family_id: 'momentum_v1',
        binding_mode: 'OBSERVE_ONLY',
        exchange: 'binance',
        stream: 'trade',
        symbols: ['btcusdt'],
        orderQty: 1,
        params: {
          delta_ms_list: [1000],
          h_ms_list: [1000],
          tolerance_ms: 0,
        },
        selected_cell: {
          exchange: 'binance',
          stream: 'trade',
          symbol: 'btcusdt',
          delta_ms: 1000,
          h_ms: 1000,
          event_count: 1000,
          mean_product: 0.2,
          t_stat: 3.0,
        },
      },
      autoAdapt: true,
    }),
    /binding_mode must be PAPER_DIRECTIONAL_V1/,
  );
});
