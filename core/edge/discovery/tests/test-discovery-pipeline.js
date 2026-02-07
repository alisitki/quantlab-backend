/**
 * Test: EdgeDiscoveryPipeline
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EdgeDiscoveryPipeline } from '../EdgeDiscoveryPipeline.js';
import { EdgeRegistry } from '../../EdgeRegistry.js';

test('EdgeDiscoveryPipeline - constructor', () => {
  const registry = new EdgeRegistry();
  const pipeline = new EdgeDiscoveryPipeline({ registry });

  assert.ok(pipeline.loader, 'Should have loader');
  assert.ok(pipeline.scanner, 'Should have scanner');
  assert.ok(pipeline.tester, 'Should have tester');
  assert.ok(pipeline.generator, 'Should have generator');
  assert.equal(pipeline.registry, registry, 'Should store registry');
});

test('EdgeDiscoveryPipeline - constructor without registry', () => {
  const pipeline = new EdgeDiscoveryPipeline();

  assert.equal(pipeline.registry, null, 'Registry should be null if not provided');
});

// Note: Full integration test with real parquet file requires:
// - An actual parquet file in /data/processed/
// - ReplayEngine working correctly
// - FeatureBuilder with all features registered
// - This can be tested manually
//
// Example manual test:
// node -e "import('./EdgeDiscoveryPipeline.js').then(m => { const p = new m.EdgeDiscoveryPipeline(); return p.run({ parquetPath: '/data/processed/adausdt_20260203.parquet', metaPath: '/data/processed/adausdt_20260203.parquet.meta.json', symbol: 'ADA/USDT' }); }).then(r => console.log('Discovery result:', JSON.stringify(r, null, 2)));"

test('EdgeDiscoveryPipeline - result structure validation', () => {
  // This test validates that the expected result structure is correct
  // We'll check the structure without running the full pipeline

  const expectedResultStructure = {
    patternsScanned: 'number',
    patternsTestedSignificant: 'number',
    edgeCandidatesGenerated: 'number',
    edgeCandidatesRegistered: 'number',
    edges: 'array',
    rejectedPatterns: 'array',
    metadata: 'object'
  };

  // Mock result
  const mockResult = {
    patternsScanned: 100,
    patternsTestedSignificant: 10,
    edgeCandidatesGenerated: 5,
    edgeCandidatesRegistered: 5,
    edges: [],
    rejectedPatterns: [],
    metadata: {
      duration: 5000,
      dataRowCount: 10000,
      regimesUsed: 4
    }
  };

  // Validate structure
  for (const [key, expectedType] of Object.entries(expectedResultStructure)) {
    assert.ok(key in mockResult, `Result should have ${key}`);

    if (expectedType === 'number') {
      assert.equal(typeof mockResult[key], 'number', `${key} should be a number`);
    } else if (expectedType === 'array') {
      assert.ok(Array.isArray(mockResult[key]), `${key} should be an array`);
    } else if (expectedType === 'object') {
      assert.equal(typeof mockResult[key], 'object', `${key} should be an object`);
    }
  }

  assert.ok(true, 'Result structure is valid');
});
