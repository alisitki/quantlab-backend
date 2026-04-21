import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import { StrategyLoader } from '../../interface/StrategyLoader.js';


const STRATEGY_PATH = path.resolve('core/strategy/strategies/MicrostructureImbalanceV2Strategy.js');
const TS0 = 1_700_000_000_000_000_000n;


function makeConfig(overrides = {}) {
  const base = {
    binding_mode: 'PAPER_DIRECTIONAL_V1',
    family_id: 'microstructure_imbalance_v2',
    source_pack_id: 'pack_demo',
    source_decision_tier: 'PHASE7_V2_SMOKE',
    exchange: 'bybit',
    stream: 'trade',
    symbols: ['linkusdt'],
    source_family_report_path: '/tmp/family_microstructure_imbalance_v2_report.json',
    window: '20260410',
    orderQty: 1,
    params: {
      delta_ms_list: [100],
      h_ms_list: [500],
      tolerance_ms: 0,
      exit_pressure_threshold: 0.1,
      confirmation: {
        window_ms: 100,
        venues: ['binance', 'okx'],
        required_alignment_count: 2,
        min_available_count: 2,
        max_divergence_score: 0.35,
      },
      btc_support: {
        mode: 'DISABLED',
      },
    },
    selected_cell: {
      exchange: 'bybit',
      stream: 'trade',
      symbol: 'linkusdt',
      delta_ms: 100,
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
      confirmation: {
        ...base.params.confirmation,
        ...((overrides.params && overrides.params.confirmation) || {}),
      },
      btc_support: {
        ...base.params.btc_support,
        ...((overrides.params && overrides.params.btc_support) || {}),
      },
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
    stats: { processed: 0 },
    getExecutionState() {
      return {
        positions: size === 0 ? {} : {
          LINKUSDT: { size },
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

async function loadStrategy(config = makeConfig()) {
  return StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config,
    autoAdapt: true,
  });
}

function trade({ offsetMs, exchange, side, qty = 1, price = 100 }) {
  return {
    ts_event: TS0 + BigInt(offsetMs) * 1_000_000n,
    exchange,
    symbol: 'LINKUSDT',
    stream: 'trade',
    price,
    qty,
    side,
  };
}

function bbo({ offsetMs, exchange, bidPrice, askPrice, bidQty = 1, askQty = 1 }) {
  return {
    ts_event: TS0 + BigInt(offsetMs) * 1_000_000n,
    exchange,
    symbol: 'BTCUSDT',
    stream: 'bbo',
    bid_price: bidPrice,
    ask_price: askPrice,
    bid_qty: bidQty,
    ask_qty: askQty,
  };
}

test('MicrostructureImbalanceV2Strategy opens LONG only when both external venues confirm', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, exchange: 'binance', side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 1, exchange: 'okx', side: 1, qty: 4 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 2, exchange: 'bybit', side: 1, qty: 5 }), ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].signal_action, 'LONG_OPEN');
  assert.equal(ctx.orders[0].trade_context.opening_trade.venue_alignment_count, 3);
  assert.equal(ctx.orders[0].trade_context.opening_trade.external_alignment_count, 2);
  assert.equal(ctx.orders[0].trade_context.opening_trade.venue_divergence_pass, true);
  assert.equal(ctx.orders[0].trade_context.opening_trade.entry_decision_reason, 'LONG_CONFIRMED');
});

test('MicrostructureImbalanceV2Strategy rejects flat entry without full external confirmation', async () => {
  const strategy = await loadStrategy();
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, exchange: 'binance', side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 2, exchange: 'bybit', side: 1, qty: 5 }), ctx);

  assert.equal(ctx.orders.length, 0);
  assert.equal(strategy.getState().last_action.action, 'SKIP_LONG_REJECT_NO_EXTERNAL_COVERAGE');
  assert.equal(strategy.getState().last_confirmation.reason, 'REJECT_NO_EXTERNAL_COVERAGE');
});

test('MicrostructureImbalanceV2Strategy rejects high-divergence local trigger', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: {
      confirmation: {
        max_divergence_score: 0.15,
      },
    },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, exchange: 'binance', side: 1, qty: 10 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 1, exchange: 'okx', side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 2, exchange: 'okx', side: -1, qty: 2 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 3, exchange: 'bybit', side: 1, qty: 5 }), ctx);

  assert.equal(ctx.orders.length, 0);
  assert.equal(strategy.getState().last_confirmation.reason, 'REJECT_HIGH_DIVERGENCE');
});

test('MicrostructureImbalanceV2Strategy requires supportive BTC context only when enabled', async () => {
  const strategy = await loadStrategy(makeConfig({
    params: {
      btc_support: {
        mode: 'REQUIRE_SUPPORTIVE',
        exchanges: ['bybit', 'okx'],
        window_ms: 100,
      },
    },
  }));
  const ctx = makeContext();

  await strategy.onEvent(bbo({ offsetMs: 0, exchange: 'bybit', bidPrice: 100, askPrice: 101 }), ctx);
  await strategy.onEvent(bbo({ offsetMs: 1, exchange: 'okx', bidPrice: 99, askPrice: 100 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 2, exchange: 'binance', side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 3, exchange: 'okx', side: 1, qty: 4 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 4, exchange: 'bybit', side: 1, qty: 5 }), ctx);

  assert.equal(ctx.orders.length, 0);
  assert.equal(strategy.getState().last_confirmation.reason, 'REJECT_MARKET_UNSUPPORTIVE');
});

test('MicrostructureImbalanceV2Strategy reverses only after opposite confirmation', async () => {
  const strategy = await loadStrategy(makeConfig({
    selected_cell: { h_ms: 100 },
  }));
  const ctx = makeContext();

  await strategy.onEvent(trade({ offsetMs: 0, exchange: 'binance', side: 1, qty: 3 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 1, exchange: 'okx', side: 1, qty: 4 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 2, exchange: 'bybit', side: 1, qty: 5 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 150, exchange: 'binance', side: -1, qty: 5 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 151, exchange: 'okx', side: -1, qty: 6 }), ctx);
  await strategy.onEvent(trade({ offsetMs: 152, exchange: 'bybit', side: -1, qty: 7 }), ctx);

  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].signal_action, 'LONG_TO_SHORT_REVERSAL');
  assert.equal(ctx.orders[1].trade_context.closing_trade.exit_reason, 'REVERSAL_EXIT');
  assert.equal(ctx.orders[1].trade_context.opening_trade.entry_signal_reason, 'REVERSAL_ENTRY');
});
