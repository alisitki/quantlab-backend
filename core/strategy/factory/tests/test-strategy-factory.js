/**
 * Test: StrategyFactory
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { StrategyFactory } from '../StrategyFactory.js';
import { EdgeRegistry } from '../../../edge/EdgeRegistry.js';
import { Edge } from '../../../edge/Edge.js';

test('StrategyFactory - constructor', () => {
  const registry = new EdgeRegistry();
  const factory = new StrategyFactory({ registry });

  assert.ok(factory.templateSelector);
  assert.ok(factory.parameterMapper);
  assert.ok(factory.assembler);
  assert.ok(factory.backtester);
  assert.ok(factory.deployer);
});

test('StrategyFactory - produce requires dataConfig', async () => {
  const factory = new StrategyFactory();

  const edge = new Edge({
    id: 'test',
    name: 'Test',
    entryCondition: () => ({ active: false }),
    exitCondition: () => ({ exit: false }),
    status: 'VALIDATED'
  });

  const result = await factory.produce(edge);

  assert.equal(result.status, 'ERROR');
  assert.ok(result.error);
  console.log(`  Error handling: ${result.error}`);
});

// Note: Full integration test with real backtest requires:
// - Real parquet file
// - Working ReplayEngine + ExecutionEngine
//
// Manual test command:
// const factory = new StrategyFactory({
//   dataConfig: {
//     parquetPath: '/data/processed/adausdt_20260203.parquet',
//     metaPath: '/data/processed/adausdt_20260203.parquet.meta.json',
//     symbol: 'ADA/USDT'
//   }
// });
// await factory.produce(edge);
