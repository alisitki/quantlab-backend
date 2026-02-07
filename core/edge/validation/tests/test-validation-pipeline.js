/**
 * Test: EdgeValidationPipeline
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EdgeValidationPipeline } from '../EdgeValidationPipeline.js';
import { EdgeRegistry } from '../../EdgeRegistry.js';
import { Edge } from '../../Edge.js';

test('EdgeValidationPipeline - constructor', () => {
  const registry = new EdgeRegistry();
  const pipeline = new EdgeValidationPipeline({ registry });

  assert.ok(pipeline.oosValidator);
  assert.ok(pipeline.wfAnalyzer);
  assert.ok(pipeline.decayDetector);
  assert.ok(pipeline.regimeTester);
  assert.ok(pipeline.scorer);
});

test('EdgeValidationPipeline - validate single edge', async () => {
  const pipeline = new EdgeValidationPipeline();

  const edge = new Edge({
    id: 'test_edge',
    name: 'Test Edge',
    entryCondition: (features) => ({ active: features.signal > 0.5, direction: 'LONG' }),
    exitCondition: () => ({ exit: false }),
    timeHorizon: 10000
  });

  const dataset = {
    rows: Array(200).fill(null).map((_, i) => {
      const signal = Math.random();
      const forwardReturn = signal > 0.5 ? 0.001 : -0.0005;
      return {
        features: { signal },
        regime: 0,
        forwardReturns: { h10: forwardReturn },
        timestamp: BigInt(i * 1000)
      };
    }),
    featureNames: ['signal'],
    metadata: {}
  };

  const result = await pipeline.validate(edge, dataset);

  assert.ok(result, 'Should return result');
  assert.equal(result.edgeId, 'test_edge');
  assert.ok(result.score);
  assert.ok(result.oosResult);
  assert.ok(result.walkForwardResult);
  assert.ok(result.decayResult);
  assert.ok(result.regimeResult);
  assert.ok(['VALIDATED', 'REJECTED'].includes(result.newStatus));

  console.log(`  Validation result: ${result.newStatus}, score: ${result.score.total.toFixed(3)}`);
});

test('EdgeValidationPipeline - validateAll with registry', async () => {
  const registry = new EdgeRegistry();
  const pipeline = new EdgeValidationPipeline({ registry });

  // Register candidate edges
  const edge1 = new Edge({
    id: 'edge1',
    name: 'Edge 1',
    entryCondition: () => ({ active: true, direction: 'LONG' }),
    exitCondition: () => ({ exit: false }),
    status: 'CANDIDATE',
    timeHorizon: 10000
  });

  const edge2 = new Edge({
    id: 'edge2',
    name: 'Edge 2',
    entryCondition: () => ({ active: true, direction: 'SHORT' }),
    exitCondition: () => ({ exit: false }),
    status: 'CANDIDATE',
    timeHorizon: 10000
  });

  registry.register(edge1);
  registry.register(edge2);

  const dataset = {
    rows: Array(100).fill(null).map((_, i) => ({
      features: {},
      regime: 0,
      forwardReturns: { h10: 0.001 },
      timestamp: BigInt(i * 1000)
    })),
    featureNames: [],
    metadata: {}
  };

  const results = await pipeline.validateAll(dataset);

  assert.equal(results.length, 2, 'Should validate 2 edges');
  console.log(`  ValidateAll: ${results.length} edges processed`);
});
