/**
 * Test: StrategyTemplateSelector
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { StrategyTemplateSelector } from '../StrategyTemplateSelector.js';
import { MeanReversionTemplate } from '../templates/MeanReversionTemplate.js';
import { MomentumTemplate } from '../templates/MomentumTemplate.js';
import { BreakoutTemplate } from '../templates/BreakoutTemplate.js';
import { Edge } from '../../../edge/Edge.js';

test('StrategyTemplateSelector - select mean reversion', () => {
  const selector = new StrategyTemplateSelector();

  const edge = new Edge({
    id: 'test',
    name: 'High micro reversion (LONG)',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false })
  });

  const result = selector.select(edge);

  assert.equal(result.templateClass, MeanReversionTemplate);
  console.log(`  Mean reversion: ${result.reason}`);
});

test('StrategyTemplateSelector - select momentum', () => {
  const selector = new StrategyTemplateSelector();

  const edge = new Edge({
    id: 'test',
    name: 'High return momentum (LONG)',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false })
  });

  const result = selector.select(edge);

  assert.equal(result.templateClass, MomentumTemplate);
  console.log(`  Momentum: ${result.reason}`);
});

test('StrategyTemplateSelector - select breakout', () => {
  const selector = new StrategyTemplateSelector();

  const edge = new Edge({
    id: 'test',
    name: 'High volatility compression score (LONG)',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false })
  });

  const result = selector.select(edge);

  assert.equal(result.templateClass, BreakoutTemplate);
  console.log(`  Breakout: ${result.reason}`);
});

test('StrategyTemplateSelector - default to mean reversion', () => {
  const selector = new StrategyTemplateSelector();

  const edge = new Edge({
    id: 'test',
    name: 'Unknown pattern (LONG)',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false })
  });

  const result = selector.select(edge);

  assert.equal(result.templateClass, MeanReversionTemplate);
  console.log(`  Default: ${result.reason}`);
});
