/**
 * Test: StrategyAssembler
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { StrategyAssembler } from '../StrategyAssembler.js';
import { MeanReversionTemplate } from '../templates/MeanReversionTemplate.js';
import { Edge } from '../../../edge/Edge.js';

test('StrategyAssembler - assemble strategy', () => {
  const assembler = new StrategyAssembler();

  const edge = new Edge({
    id: 'test_edge',
    name: 'Test Edge',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false })
  });

  const params = {
    baseQuantity: 10,
    maxQuantity: 50,
    timeHorizon: 10000,
    cooldownMs: 5000,
    enabledFeatures: ['mid_price'],
    gateConfig: {}
  };

  const strategy = assembler.assemble(MeanReversionTemplate, edge, params);

  assert.ok(strategy);
  assert.ok(strategy.getStrategyId());
  assert.equal(strategy.edge, edge);
  assert.ok(strategy.config);

  const metadata = assembler.getMetadata(strategy);
  assert.ok(metadata.strategyId);
  assert.equal(metadata.edgeId, 'test_edge');
  assert.equal(metadata.templateType, 'mean_reversion');

  console.log(`  Assembled: ${metadata.strategyId}`);
});
