import test from 'node:test';
import assert from 'node:assert/strict';

import { MaxPositionRule } from './MaxPositionRule.js';

function makeContext(positionSize = 0) {
  return {
    execution: {
      snapshot() {
        return {
          positions: positionSize === 0 ? {} : {
            BTCUSDT: {
              size: positionSize,
            },
          },
        };
      },
    },
  };
}

test('MaxPositionRule allows explicit long close intents', () => {
  const rule = new MaxPositionRule(1);
  const result = rule.check({
    symbol: 'BTCUSDT',
    side: 'SELL',
    qty: 1,
    action: 'EXIT_LONG',
    position_effect: 'CLOSE',
  }, makeContext(1));

  assert.deepEqual(result, { allowed: true });
});

test('MaxPositionRule allows explicit short close intents', () => {
  const rule = new MaxPositionRule(1);
  const result = rule.check({
    symbol: 'BTCUSDT',
    side: 'BUY',
    qty: 1,
    action: 'EXIT_SHORT',
    position_effect: 'CLOSE',
  }, makeContext(-1));

  assert.deepEqual(result, { allowed: true });
});

test('MaxPositionRule allows reversal intents through an existing position', () => {
  const rule = new MaxPositionRule(1);
  const result = rule.check({
    symbol: 'BTCUSDT',
    side: 'SELL',
    qty: 2,
    action: 'EXIT_LONG',
    position_effect: 'REVERSE',
  }, makeContext(1));

  assert.deepEqual(result, { allowed: true });
});

test('MaxPositionRule still rejects same-side adds when maxPositions is reached', () => {
  const rule = new MaxPositionRule(1);
  const result = rule.check({
    symbol: 'BTCUSDT',
    side: 'BUY',
    qty: 1,
    action: 'LONG',
    position_effect: 'OPEN',
  }, makeContext(1));

  assert.equal(result.allowed, false);
  assert.equal(result.reason, 'max_position_reached (1/1)');
});
