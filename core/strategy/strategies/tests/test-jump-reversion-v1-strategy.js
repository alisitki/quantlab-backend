import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';

import { StrategyLoader } from '../../interface/StrategyLoader.js';


const STRATEGY_PATH = path.resolve('core/strategy/strategies/JumpReversionV1Strategy.js');


function makeConfig() {
  return {
    binding_mode: 'PAPER_DIRECTIONAL_V1',
    family_id: 'jump_reversion_v1',
    source_pack_id: 'pack_demo',
    source_decision_tier: 'PROMOTE',
    exchange: 'binance',
    stream: 'trade',
    symbols: ['xrpusdt'],
    source_family_report_path: '/tmp/family_jump_reversion_report.json',
    orderQty: 1,
    params: {
      jump_thresh_bps_list: [50],
      h_ms_list: [1000],
      cooldown_ms: 0,
    },
    selected_cell: {
      exchange: 'binance',
      stream: 'trade',
      symbol: 'xrpusdt',
      jump_thresh_bps: 50,
      h_ms: 1000,
      jump_count: 1000,
      mean_signed_reversal: 0.2,
      t_stat: 4.0,
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
          XRPUSDT: { size },
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

test('JumpReversionV1Strategy opens SHORT on upward jump', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onEvent({ ts_event: 1_700_000_000_000_000_000n, symbol: 'XRPUSDT', stream: 'trade', price: 1.0 }, ctx);
  await strategy.onEvent({ ts_event: 1_700_000_001_000_000_000n, symbol: 'XRPUSDT', stream: 'trade', price: 1.01 }, ctx);

  assert.equal(ctx.orders.length, 1);
  assert.equal(ctx.orders[0].side, 'SELL');
  assert.equal(strategy.getState().last_signal.signal_direction, 'SHORT');
});

test('JumpReversionV1Strategy closes after horizon with no new jump', async () => {
  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: makeConfig(),
    autoAdapt: true,
  });
  const ctx = makeContext();
  await strategy.onEvent({ ts_event: 1_700_000_000_000_000_000n, symbol: 'XRPUSDT', stream: 'trade', price: 1.0 }, ctx);
  await strategy.onEvent({ ts_event: 1_700_000_001_000_000_000n, symbol: 'XRPUSDT', stream: 'trade', price: 1.01 }, ctx);
  await strategy.onEvent({ ts_event: 1_700_000_003_000_000_000n, symbol: 'XRPUSDT', stream: 'trade', price: 1.0101 }, ctx);

  assert.equal(ctx.orders.length, 2);
  assert.equal(ctx.orders[1].side, 'BUY');
  assert.equal(strategy.getState().last_action.action, 'SHORT_CLOSE');
});

test('JumpReversionV1Strategy validates family id', async () => {
  await assert.rejects(
    StrategyLoader.loadFromFile(STRATEGY_PATH, {
      config: {
        ...makeConfig(),
        family_id: 'momentum_v1',
      },
      autoAdapt: true,
    }),
    /JUMP_REVERSION_V1_CONFIG_ERROR/
  );
});
