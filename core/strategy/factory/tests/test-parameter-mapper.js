/**
 * Test: StrategyParameterMapper
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { StrategyParameterMapper } from '../StrategyParameterMapper.js';
import { Edge } from '../../../edge/Edge.js';

test('StrategyParameterMapper - map basic edge', () => {
  const mapper = new StrategyParameterMapper();

  const edge = new Edge({
    id: 'test',
    name: 'Test Edge',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false }),
    timeHorizon: 15000,
    expectedAdvantage: { sharpe: 1.0 },
    riskProfile: { maxDrawdown: 0.02 }
  });

  const params = mapper.map(edge, 'mean_reversion');

  assert.ok(params.baseQuantity > 0);
  assert.ok(params.maxQuantity > 0);
  assert.equal(params.timeHorizon, 15000);
  assert.ok(params.cooldownMs > 0);
  assert.ok(Array.isArray(params.enabledFeatures));
  assert.ok(params.gateConfig);
  assert.equal(params.templateType, 'mean_reversion');

  console.log(`  Mapped params: baseQty=${params.baseQuantity}, maxQty=${params.maxQuantity}, cooldown=${params.cooldownMs}ms`);
});
