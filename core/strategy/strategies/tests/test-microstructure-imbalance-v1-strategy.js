import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import { StrategyLoader } from '../../interface/StrategyLoader.js';


const STRATEGY_PATH = path.resolve('core/strategy/strategies/MicrostructureImbalanceV1Strategy.js');
const TS0 = 1_700_000_000_000_000_000n;


function makeConfig(overrides = {}) {
  const base = {
    binding_mode: 'PAPER_DIRECTIONAL_V1',
    family_id: 'microstructure_imbalance_v1',
    source_pack_id: 'pack_demo',
    source_decision_tier: 'PROMOTE_STRONG',
    exchange: 'bybit',
    stream: 'trade',
    symbols: ['btcusdt'],
    source_family_report_path: '/tmp/family_microstructure_imbalance_report.json',
    window: '20260324..20260328',
    orderQty: 1,
    params: {
      delta_ms_list: [100, 250, 500],
      h_ms_list: [250, 500],
      tolerance_ms: 0,
    },
    selected_cell: {
      exchange: 'bybit',
      stream: 'trade',
      symbol: 'btcusdt',
      delta_ms: 250,
      h_ms: 500,
      pressure_threshold: 0.2,
      event_count: 100000,
      mean_signed_fwd_return_bps: 0.5,
      t_stat: 20,
    },
  };
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
    getSize() {
      return size;
    },
    orders,
  };
}

async function loadStrategy(config = makeConfig()) {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config,
    autoAdapt: true,
  });
  return strategy;
}

function trade({ offsetMs, side, qty = 1, price = 100 }) {
  return {
    ts_event: TS0 + BigInt(offsetMs) * 1_000_000n,
    symbol: 'BTCUSDT',
    stream: 'trade',
    price,
    qty,
    side,
  };
}

test('MicrostructureImbalanceV1Strategy opens LONG on positive pressure', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();
  await strategy.onInit?.(ctx);

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3 }), ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].side, 'BUY');
  assert.equal(ctx.orders[0].signal_action, 'LONG_OPEN');
  assert.equal(ctx.orders[0].trade_context.schema_version, 'microstructure_trade_context_v0');
  assert.equal(ctx.orders[0].trade_context.opening_trade.trade_sequence_id, 1);
  assert.equal(ctx.orders[0].trade_context.opening_trade.entry_side, 'LONG');
  assert.equal(ctx.orders[0].trade_context.opening_trade.entry_signal_reason, 'LONG_ENTRY');
  assert.equal(ctx.orders[0].trade_context.opening_trade.entry_pressure, 1);
  assert.equal(strategy.getState().last_signal.signal_direction, 'LONG');
  assert.equal(strategy.getState().order_event_count, 1);
});

test('MicrostructureImbalanceV1Strategy opens SHORT on negative pressure', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: -1, qty: 3 }), ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].side, 'SELL');
  assert.equal(ctx.orders[0].signal_action, 'SHORT_OPEN');
  assert.equal(strategy.getState().last_signal.signal_direction, 'SHORT');
});

test('MicrostructureImbalanceV1Strategy stays flat below threshold', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 1 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 1, side: -1, qty: 1 }), ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(strategy.getState().last_signal.signal_direction, 'FLAT');
  assert.equal(strategy.getState().last_action.action, 'HOLD_LONG');
});

test('MicrostructureImbalanceV1Strategy does not reverse before h_ms', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 100, side: -1, qty: 10 }), ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].side, 'BUY');
  assert.equal(strategy.getState().last_action.action, 'HOLD_LONG');
  assert.equal(strategy.getState().last_action.commit_active, true);
});

test('MicrostructureImbalanceV1Strategy uses selected h_ms without extra hold floor', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 600, side: -1, qty: 10 }), ctx);

  assert.equal(strategy.getState().min_hold_ms, 500);
  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].side, 'SELL');
  assert.equal(ctx.orders[1].signal_action, 'LONG_TO_SHORT_REVERSAL');
});

test('MicrostructureImbalanceV1Strategy can reverse after selected h_ms', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 600, side: -1, qty: 10 }), ctx);

  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].side, 'SELL');
  assert.equal(ctx.orders[1].signal_action, 'LONG_TO_SHORT_REVERSAL');
  assert.equal(ctx.orders[1].qty, 2);
  assert.equal(ctx.orders[1].trade_context.closing_trade.exit_reason, 'REVERSAL_EXIT');
  assert.equal(ctx.orders[1].trade_context.closing_trade.trade_sequence_id, 1);
  assert.equal(ctx.orders[1].trade_context.opening_trade.trade_sequence_id, 2);
  assert.equal(ctx.orders[1].trade_context.opening_trade.entry_side, 'SHORT');
  assert.equal(ctx.orders[1].trade_context.opening_trade.entry_signal_reason, 'REVERSAL_ENTRY');
});

test('MicrostructureImbalanceV1Strategy closes LONG inside hysteresis exit band', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: { exit_pressure_threshold: 0.1 },
    selected_cell: { h_ms: 100 },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 120, side: -1, qty: 2.8 }), ctx);

  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].side, 'SELL');
  assert.equal(ctx.orders[1].signal_action, 'LONG_CLOSE');
});

test('MicrostructureImbalanceV1Strategy holds LONG on moderate opposite pressure', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: { exit_pressure_threshold: 0.1 },
    selected_cell: { h_ms: 100 },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 120, side: -1, qty: 4 }), ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(strategy.getState().last_action.action, 'HOLD_LONG');
});

test('MicrostructureImbalanceV1Strategy closes SHORT inside hysteresis exit band', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: { exit_pressure_threshold: 0.1 },
    selected_cell: { h_ms: 100 },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: -1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 120, side: 1, qty: 2.8 }), ctx);

  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].side, 'BUY');
  assert.equal(ctx.orders[1].signal_action, 'SHORT_CLOSE');
});

test('MicrostructureImbalanceV1Strategy skips low-edge flat entry when fee-aware gate blocks it', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: {
      exit_pressure_threshold: 0.1,
      fee_aware_expected_edge_scale: 0.006095348835,
      fee_aware_fee_rate: 0.0004,
    },
    selected_cell: { h_ms: 100 },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3, price: 100 }), ctx);

  assert.equal(ctx.orders.length, 0);
  assert.equal(strategy.getState().last_action.action, 'SKIP_LONG_LOW_EDGE');
});

test('MicrostructureImbalanceV1Strategy allows high-edge flat entry when fee-aware gate passes', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: {
      exit_pressure_threshold: 0.1,
      fee_aware_expected_edge_scale: 0.006095348835,
      fee_aware_fee_rate: 0.0004,
    },
    selected_cell: { h_ms: 100 },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3, price: 9 }), ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].signal_action, 'LONG_OPEN');
});

test('MicrostructureImbalanceV1Strategy closes instead of reversing on low-edge opposite signal', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: {
      exit_pressure_threshold: 0.1,
      fee_aware_expected_edge_scale: 0.006095348835,
      fee_aware_fee_rate: 0.0004,
    },
    selected_cell: { h_ms: 100 },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, side: 1, qty: 3, price: 9 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 120, side: -1, qty: 6, price: 9 }), ctx);

  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].side, 'SELL');
  assert.equal(ctx.orders[1].qty, 1);
  assert.equal(ctx.orders[1].signal_action, 'LONG_CLOSE_LOW_EDGE_OPPOSITE');
});

test('MicrostructureImbalanceV1Strategy validates family id', async () => {
  await assert.rejects(
    loadStrategy(makeConfig({ family_id: 'return_reversal_v1' })),
    /MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR/
  );
});
