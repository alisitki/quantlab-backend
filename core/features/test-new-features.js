import assert from 'assert';
import { EMAFeature } from './builders/EMAFeature.js';
import { RSIFeature } from './builders/RSIFeature.js';
import { ATRFeature } from './builders/ATRFeature.js';
import { ROCFeature } from './builders/ROCFeature.js';
import { VolatilityRegimeFeature } from './builders/VolatilityRegimeFeature.js';
import { TrendRegimeFeature } from './builders/TrendRegimeFeature.js';
import { SpreadRegimeFeature } from './builders/SpreadRegimeFeature.js';
import { MicropriceFeature } from './builders/MicropriceFeature.js';
import { ImbalanceEMAFeature } from './builders/ImbalanceEMAFeature.js';
import { EMASlopeFeature } from './builders/EMASlopeFeature.js';
import { BollingerPositionFeature } from './builders/BollingerPositionFeature.js';

// Generate test events
function generateEvents(count, startPrice = 100, volatility = 0.01) {
  const events = [];
  let price = startPrice;
  for (let i = 0; i < count; i++) {
    const change = (Math.random() - 0.5) * 2 * volatility * price;
    price += change;
    events.push({
      bid_price: price - 0.5,
      ask_price: price + 0.5,
      bid_qty: 100 + Math.random() * 100,
      ask_qty: 100 + Math.random() * 100,
      ts_event: 1000 + i * 100
    });
  }
  return events;
}

// Test helper
function runFeatureTest(name, FeatureClass, config, minWarmup) {
  console.log(`Testing ${name}...`);
  const feature = new FeatureClass(config);
  const events = generateEvents(minWarmup + 20);

  // 1. Warm-up test - find when feature becomes warm
  let warmIndex = -1;
  let warmResult = null;
  for (let i = 0; i < events.length; i++) {
    const result = feature.onEvent(events[i]);
    if (result !== null && warmIndex === -1) {
      warmIndex = i;
      warmResult = result;
      break;
    }
  }

  assert.notStrictEqual(warmResult, null, `${name}: Should eventually become warm`);
  assert.ok(warmIndex >= minWarmup - 2, `${name}: Should require at least ${minWarmup-2} events (got warm at ${warmIndex})`);
  console.log(`  Warm-up OK at event ${warmIndex} (value: ${warmResult})`);

  // 2. Reset test
  feature.reset();
  const resetResult = feature.onEvent(events[0]);
  assert.strictEqual(resetResult, null, `${name}: Should return null after reset`);
  console.log(`  Reset OK`);

  // 3. Determinism test
  feature.reset();
  const results1 = events.map(e => feature.onEvent(e));

  feature.reset();
  const results2 = events.map(e => feature.onEvent(e));

  assert.deepStrictEqual(results1, results2, `${name}: Results should be identical`);
  console.log(`  Determinism OK`);

  console.log(`${name} PASSED`);
}

async function testEMA() {
  runFeatureTest('EMAFeature', EMAFeature, { period: 5 }, 5);
}

async function testRSI() {
  runFeatureTest('RSIFeature', RSIFeature, { period: 5 }, 6);
}

async function testATR() {
  runFeatureTest('ATRFeature', ATRFeature, { period: 5 }, 6);
}

async function testROC() {
  runFeatureTest('ROCFeature', ROCFeature, { period: 5 }, 6);
}

async function testVolatilityRegime() {
  runFeatureTest('VolatilityRegimeFeature', VolatilityRegimeFeature,
    { shortWindow: 5, longWindow: 10 }, 16);
}

async function testTrendRegime() {
  runFeatureTest('TrendRegimeFeature', TrendRegimeFeature,
    { fastPeriod: 3, slowPeriod: 8 }, 8);
}

async function testSpreadRegime() {
  runFeatureTest('SpreadRegimeFeature', SpreadRegimeFeature,
    { window: 10 }, 10);
}

async function testMicroprice() {
  console.log('Testing MicropriceFeature...');
  const feature = new MicropriceFeature();

  // Stateless - should not need warm-up
  const event = { bid_price: 100, ask_price: 102, bid_qty: 100, ask_qty: 50 };
  const result = feature.onEvent(event);
  assert.notStrictEqual(result, null, 'Microprice should return immediately');

  // Check formula: (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)
  // (100 * 50 + 102 * 100) / 150 = (5000 + 10200) / 150 = 101.33...
  const expected = (100 * 50 + 102 * 100) / 150;
  assert.strictEqual(result, expected, 'Microprice formula should be correct');

  console.log(`  Value: ${result}`);
  console.log('MicropriceFeature PASSED');
}

async function testImbalanceEMA() {
  runFeatureTest('ImbalanceEMAFeature', ImbalanceEMAFeature, { period: 5 }, 5);
}

async function testEMASlope() {
  runFeatureTest('EMASlopeFeature', EMASlopeFeature,
    { period: 5, lookback: 3 }, 9);
}

async function testBollingerPosition() {
  runFeatureTest('BollingerPositionFeature', BollingerPositionFeature,
    { period: 10, k: 2 }, 10);
}

async function testIntegration() {
  console.log('\nTesting Integration with FeatureRegistry...');

  const { FeatureRegistry } = await import('./FeatureRegistry.js');

  const builder = FeatureRegistry.createFeatureBuilder('btcusdt', {
    enabledFeatures: [
      'mid_price', 'spread', 'return_1', 'volatility',
      'ema', 'rsi', 'atr', 'roc',
      'microprice', 'imbalance_ema'
    ],
    ema: { period: 5 },
    rsi: { period: 5 },
    atr: { period: 5 },
    roc: { period: 5 },
    imbalance_ema: { period: 5 },
    volatility: { windowSize: 5 }
  });

  const events = generateEvents(20);
  let warmVector = null;

  for (const event of events) {
    const result = builder.onEvent(event);
    if (result !== null) {
      warmVector = result;
      break;
    }
  }

  assert.notStrictEqual(warmVector, null, 'Builder should produce feature vector');
  assert.ok('mid_price' in warmVector, 'Should have mid_price');
  assert.ok('ema' in warmVector, 'Should have ema');
  assert.ok('rsi' in warmVector, 'Should have rsi');
  assert.ok('microprice' in warmVector, 'Should have microprice');

  console.log('Feature vector keys:', Object.keys(warmVector).join(', '));
  console.log('Integration PASSED');
}

async function runAllTests() {
  console.log('=== New Feature Tests ===\n');

  // Group 1: Technical Indicators
  console.log('--- Group 1: Technical Indicators ---');
  await testEMA();
  await testRSI();
  await testATR();
  await testROC();

  // Group 2: Regime Detection
  console.log('\n--- Group 2: Regime Detection ---');
  await testVolatilityRegime();
  await testTrendRegime();
  await testSpreadRegime();

  // Group 3: Advanced
  console.log('\n--- Group 3: Advanced ---');
  await testMicroprice();
  await testImbalanceEMA();
  await testEMASlope();
  await testBollingerPosition();

  // Integration
  await testIntegration();

  console.log('\n=== All Tests Passed ===');
}

runAllTests().catch(err => {
  console.error('Test Failed:', err);
  process.exit(1);
});
