import test from 'node:test';
import assert from 'node:assert/strict';

import { ExecutionEngine, OrderSide } from '../index.js';

test('ExecutionEngine uses trade price as executable price when requiresBbo=false', () => {
  const engine = new ExecutionEngine({
    initialCapital: 10000,
    requiresBbo: false,
  });

  engine.onEvent({
    ts_event: 1769385421076000000n,
    symbol: 'BNBUSDT',
    stream: 'trade',
    price: 866.01,
  });

  const fill = engine.onOrder({
    symbol: 'BNBUSDT',
    side: OrderSide.BUY,
    qty: 0.5,
    ts_event: 1769385421076000000n,
  });

  assert.equal(fill.fillPrice, 866.01);
  const snapshot = engine.snapshot();
  assert.equal(snapshot.positions.BNBUSDT.size, 0.5);
  assert.equal(snapshot.positions.BNBUSDT.currentPrice, 866.01);
});

test('ExecutionEngine still rejects missing BBO in strict mode', () => {
  const engine = new ExecutionEngine({
    initialCapital: 10000,
    requiresBbo: true,
  });

  assert.throws(
    () => engine.onEvent({
      ts_event: 1769385421076000000n,
      symbol: 'BNBUSDT',
      stream: 'trade',
      price: 866.01,
    }),
    /BBO stream required/
  );
});
