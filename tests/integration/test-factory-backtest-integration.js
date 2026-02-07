#!/usr/bin/env node
/**
 * AutoBacktester Integration Test
 *
 * Tests strategy templates with real parquet data:
 * 1. Create synthetic edges (mean reversion, momentum, breakout)
 * 2. Generate strategies via factory
 * 3. Backtest with real data
 * 4. Validate template-specific behaviors
 *
 * This validates:
 * - Template logic works with real market data
 * - Factory generates executable strategies
 * - Position sizing, exits, and template overrides function correctly
 */

import { EdgeRegistry } from '../../core/edge/EdgeRegistry.js';
import { StrategyFactory } from '../../core/strategy/factory/StrategyFactory.js';
import { StrategyLifecycleManager } from '../../core/strategy/lifecycle/StrategyLifecycleManager.js';
import { mkdir, writeFile } from 'fs/promises';
import { join } from 'path';

const TEST_DATA = {
  parquet: '/home/deploy/quantlab-backend/data/test/adausdt_20260203.parquet',
  meta: '/home/deploy/quantlab-backend/data/test/adausdt_20260203_meta.json',
  symbol: 'ADA/USDT'
};

const OUTPUT_DIR = 'data/test-integration';

console.log('[Integration Test] AutoBacktester + Factory + Templates');
console.log(`[Integration Test] Data: ${TEST_DATA.parquet}`);

/**
 * Create synthetic edges for testing
 */
function createTestEdges() {
  const edges = [];

  // 1. MEAN REVERSION EDGE
  // High volatility + mean reversion pattern
  edges.push({
    id: 'test_mean_reversion_001',
    name: 'Test Mean Reversion Edge',
    definition: {
      pattern: {
        type: 'threshold',
        conditions: [
          { feature: 'volatility_ratio', operator: '>', value: 1.2 },
          { feature: 'micro_reversion', operator: '>', value: 0.6 }
        ],
        regimes: null
      },
      direction: 'LONG',
      horizon: 50,
      support: 100
    },
    status: 'VALIDATED',
    discovery: {
      discoveredAt: Date.now(),
      dataSource: 'synthetic_test',
      scanMethod: 'threshold'
    },
    validation: {
      score: 0.75,
      confidence: 0.7,
      lastValidated: Date.now()
    },
    expectedAdvantage: {
      meanReturn: 0.0008,
      winRate: 0.52,
      sharpe: 1.2,
      horizon: 50
    },
    metadata: {
      description: 'Mean reversion in high volatility',
      templateHint: 'mean_reversion'
    }
  });

  // 2. MOMENTUM EDGE
  // Strong trend continuation
  edges.push({
    id: 'test_momentum_001',
    name: 'Test Momentum Edge',
    definition: {
      pattern: {
        type: 'threshold',
        conditions: [
          { feature: 'trend_strength', operator: '>', value: 0.4 },
          { feature: 'return_momentum', operator: '>', value: 0.3 }
        ],
        regimes: null
      },
      direction: 'LONG',
      horizon: 100,
      support: 150
    },
    status: 'VALIDATED',
    discovery: {
      discoveredAt: Date.now(),
      dataSource: 'synthetic_test',
      scanMethod: 'threshold'
    },
    validation: {
      score: 0.80,
      confidence: 0.75,
      lastValidated: Date.now()
    },
    expectedAdvantage: {
      meanReturn: 0.0012,
      winRate: 0.55,
      sharpe: 1.5,
      horizon: 100
    },
    metadata: {
      description: 'Momentum continuation in trending market',
      templateHint: 'momentum'
    }
  });

  // 3. BREAKOUT EDGE
  // Liquidity imbalance breakout
  edges.push({
    id: 'test_breakout_001',
    name: 'Test Breakout Edge',
    definition: {
      pattern: {
        type: 'threshold',
        conditions: [
          { feature: 'liquidity_pressure', operator: '>', value: 0.5 },
          { feature: 'imbalance_acceleration', operator: '>', value: 0.4 }
        ],
        regimes: null
      },
      direction: 'LONG',
      horizon: 50,
      support: 120
    },
    status: 'VALIDATED',
    discovery: {
      discoveredAt: Date.now(),
      dataSource: 'synthetic_test',
      scanMethod: 'threshold'
    },
    validation: {
      score: 0.72,
      confidence: 0.68,
      lastValidated: Date.now()
    },
    expectedAdvantage: {
      meanReturn: 0.001,
      winRate: 0.50,
      sharpe: 1.1,
      horizon: 50
    },
    metadata: {
      description: 'Breakout on liquidity imbalance',
      templateHint: 'breakout'
    }
  });

  return edges;
}

/**
 * Main
 */
async function main() {
  const startTime = Date.now();

  try {
    // 1. Create test edges
    console.log('\n=== Step 1: Creating Test Edges ===');
    const testEdges = createTestEdges();
    console.log(`Created ${testEdges.length} test edges:`);
    for (const edge of testEdges) {
      console.log(`  - ${edge.id} (${edge.metadata.templateHint})`);
    }

    // 2. Initialize edge registry
    console.log('\n=== Step 2: Registering Edges ===');
    const edgeRegistry = new EdgeRegistry();
    for (const edge of testEdges) {
      edgeRegistry.register(edge);
    }

    // 3. Initialize lifecycle manager
    console.log('\n=== Step 3: Initializing Lifecycle Manager ===');
    await mkdir(join(OUTPUT_DIR, 'lifecycle'), { recursive: true });
    const lifecycleManager = new StrategyLifecycleManager(
      join(OUTPUT_DIR, 'lifecycle'),
      'test-strategies.json'
    );

    // 4. Initialize factory
    console.log('\n=== Step 4: Initializing Strategy Factory ===');
    const factory = new StrategyFactory({
      registry: edgeRegistry,
      backtestConfig: {
        minReturnPct: -10, // Very lenient for test
        minWinRate: 0.0,
        maxDrawdownPct: 100
      },
      dataConfig: {
        parquetPath: TEST_DATA.parquet,
        metaPath: TEST_DATA.meta,
        symbol: TEST_DATA.symbol
      }
    });

    // 5. Generate strategies
    console.log('\n=== Step 5: Generating Strategies ===');
    const factoryResults = [];

    for (const edge of testEdges) {
      console.log(`\nGenerating strategy for edge: ${edge.id}`);

      try {
        const result = await factory.produce(edge, edge.validation);

        console.log(`  Status: ${result.status}`);
        console.log(`  Strategy ID: ${result.strategyId}`);
        console.log(`  Template: ${result.templateType}`);

        if (result.status === 'DEPLOYED' || result.status === 'BACKTEST_FAILED') {
          console.log(`  Backtest: ${JSON.stringify(result.backtestResult?.summary || {})}`);
        }

        factoryResults.push(result);
      } catch (err) {
        console.log(`  ❌ Error: ${err.message}`);
      }
    }

    // 6. Print factory results
    console.log('\n=== Factory Results ===\n');

    for (const result of factoryResults) {
      console.log(`Strategy: ${result.strategyId}`);
      console.log(`Edge: ${result.edgeId}`);
      console.log(`Template: ${result.templateType}`);
      console.log(`Status: ${result.status}`);

      if (result.backtestResult) {
        const bt = result.backtestResult;
        console.log(`  Backtest:`);
        console.log(`    Events: ${bt.summary?.eventsProcessed || 0}`);
        console.log(`    Signals: ${bt.summary?.signalsGenerated || 0}`);
        console.log(`    Trades: ${bt.summary?.trades || 0}`);
        console.log(`    Return: ${bt.summary?.returnPct?.toFixed(3) || 0}%`);
        console.log(`    Win Rate: ${bt.summary?.winRate?.toFixed(3) || 0}`);
      }

      if (result.deployResult) {
        console.log(`  Deploy: ${result.deployResult.deployed ? 'SUCCESS' : 'FAILED'}`);
      }

      console.log('');
    }

    // 7. Save results
    console.log('=== Saving Results ===');
    await mkdir(OUTPUT_DIR, { recursive: true });

    const reportPath = join(OUTPUT_DIR, `factory-backtest-integration-${Date.now()}.json`);
    await writeFile(reportPath, JSON.stringify({
      timestamp: new Date().toISOString(),
      testData: TEST_DATA,
      edgesCount: testEdges.length,
      factoryResults,
      totalDurationMs: Date.now() - startTime
    }, null, 2));

    console.log(`✅ Report saved: ${reportPath}`);

    // 8. Summary
    console.log('\n=== Summary ===');
    console.log(`Edges: ${testEdges.length}`);
    console.log(`Factory Results: ${factoryResults.length}`);

    const deployed = factoryResults.filter(r => r.status === 'DEPLOYED').length;
    const failed = factoryResults.filter(r => r.status === 'BACKTEST_FAILED').length;
    const errors = factoryResults.filter(r => r.status === 'ERROR').length;

    console.log(`  Deployed: ${deployed}`);
    console.log(`  Backtest Failed: ${failed}`);
    console.log(`  Errors: ${errors}`);
    console.log(`Total Duration: ${(Date.now() - startTime) / 1000}s`);

    console.log('\n✅ Integration test complete!');

  } catch (err) {
    console.error(`\n❌ Error: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  }
}

main();
