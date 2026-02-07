/**
 * Test: DiscoveryDataLoader
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DiscoveryDataLoader } from '../DiscoveryDataLoader.js';
import { DISCOVERY_CONFIG } from '../config.js';

test('DiscoveryDataLoader - constructor with defaults', () => {
  const loader = new DiscoveryDataLoader();

  assert.deepEqual(loader.featureNames, DISCOVERY_CONFIG.behaviorFeatures);
  assert.deepEqual(loader.regimeFeatures, DISCOVERY_CONFIG.regimeFeatures);
  assert.equal(loader.regimeK, DISCOVERY_CONFIG.regimeK);
  assert.deepEqual(loader.forwardHorizons, DISCOVERY_CONFIG.forwardHorizons);
  assert.equal(loader.seed, DISCOVERY_CONFIG.seed);
});

test('DiscoveryDataLoader - constructor with custom config', () => {
  const loader = new DiscoveryDataLoader({
    featureNames: ['micro_reversion', 'return_momentum'],
    regimeFeatures: ['volatility_ratio'],
    regimeK: 3,
    forwardHorizons: [20, 50],
    seed: 123
  });

  assert.deepEqual(loader.featureNames, ['micro_reversion', 'return_momentum']);
  assert.deepEqual(loader.regimeFeatures, ['volatility_ratio']);
  assert.equal(loader.regimeK, 3);
  assert.deepEqual(loader.forwardHorizons, [20, 50]);
  assert.equal(loader.seed, 123);
});

// Note: Full integration test with real parquet file requires:
// - An actual parquet file in /data/processed/
// - ReplayEngine working correctly
// - FeatureBuilder with all features registered
//
// This can be tested manually with:
// node -e "import('./DiscoveryDataLoader.js').then(m => { const loader = new m.DiscoveryDataLoader(); return loader.load({ parquetPath: '/data/processed/adausdt_20260203.parquet', metaPath: '/data/processed/adausdt_20260203.parquet.meta.json', symbol: 'ADA/USDT' }); }).then(dataset => console.log('Dataset loaded:', dataset.metadata));"
