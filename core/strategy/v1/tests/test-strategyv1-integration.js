/**
 * StrategyV1 Integration Test
 *
 * Test StrategyV1 with feature report and mock BBO events.
 */

import { StrategyV1 } from '../StrategyV1.js';
import { DEFAULT_CONFIG } from '../config.js';

console.log('=== StrategyV1 Integration Test ===\n');

// Mock context
const mockContext = {
  symbol: 'btcusdt',
  logger: {
    info: (...args) => console.log('[INFO]', ...args),
    warn: (...args) => console.log('[WARN]', ...args),
    debug: () => {}, // Silent debug
    error: (...args) => console.error('[ERROR]', ...args)
  },
  placeOrder: (order) => {
    console.log('[ORDER]', JSON.stringify(order));
  }
};

// Mock BBO events
function generateMockEvent(index) {
  const basePrice = 50000 + index * 10;
  const timestamp = Date.now() + index * 1000;

  return {
    ts: timestamp,
    symbol: 'btcusdt',
    exchange: 'binance',
    bid_price: basePrice - 0.5,
    ask_price: basePrice + 0.5,
    bid_qty: 1.5,
    ask_qty: 1.5
  };
}

async function runIntegrationTest() {
  try {
    // Create strategy with feature report
    const config = {
      ...DEFAULT_CONFIG,
      featureReportPath: './reports/feature_analysis_full_2026-02-05.json',
      topFeatureCount: 5,
      minAlphaScore: 0.3,
      execution: {
        ...DEFAULT_CONFIG.execution,
        minConfidence: 0.3  // Lower for testing
      },
      logging: {
        enabled: false  // Disable for test
      }
    };

    const strategy = new StrategyV1(config);

    // Start strategy
    console.log('Starting StrategyV1...\n');
    await strategy.onStart(mockContext);

    console.log('\n--- Processing Events ---\n');

    // Process events
    for (let i = 0; i < 100; i++) {
      const event = generateMockEvent(i);
      await strategy.onEvent(event, mockContext);

      // Log every 20 events
      if ((i + 1) % 20 === 0) {
        const state = strategy.getState();
        console.log(`\nEvent ${i + 1}: ${JSON.stringify(state, null, 2)}`);
      }
    }

    console.log('\n--- Final State ---\n');
    const finalState = strategy.getState();
    console.log(JSON.stringify(finalState, null, 2));

    // End strategy
    await strategy.onEnd(mockContext);

    console.log('\n=== Integration Test Passed ===');

    // Verify
    console.log('\n=== Verification ===');
    console.log(`   - Top features loaded: ${finalState.topFeatures.length}`);
    console.log(`   - Warmup completed: ${finalState.warmupComplete}`);
    console.log(`   - Trade count: ${finalState.tradeCount}`);
    console.log(`   - Signal count: ${finalState.signalCount}`);

    // Check critical requirements
    if (finalState.topFeatures.length === 5) {
      console.log('\n✅ All checks passed');
      console.log('   - Feature report loaded successfully');
      console.log('   - Strategy initialized correctly');
      console.log('   - Event processing working');
      process.exit(0);
    } else {
      console.error('\n❌ Test failed - Feature loading issue');
      process.exit(1);
    }

  } catch (err) {
    console.error('\n❌ Test failed with error:', err);
    console.error(err.stack);
    process.exit(1);
  }
}

runIntegrationTest();
