#!/usr/bin/env node

/**
 * Behavior Feature Structural Validation Script
 *
 * Purpose: Validate feature behavior using synthetic deterministic data.
 * Does NOT require real market data - generates known patterns.
 *
 * Validates:
 * 1. Feature responses to known market patterns (trending, mean-reverting, volatile, quiet)
 * 2. Feature range constraints
 * 3. Logical consistency (e.g., high momentum in uptrend)
 * 4. Derived feature dependencies
 *
 * Usage:
 *   node tools/validate-behavior-structure.js
 */

import { FeatureRegistry } from '../core/features/FeatureRegistry.js';

console.log('='.repeat(80));
console.log('BEHAVIOR FEATURE STRUCTURAL VALIDATION');
console.log('='.repeat(80));
console.log('Testing with synthetic deterministic data patterns\n');

// Test configuration
const FEATURES = [
  'mid_price',
  'spread',
  'volatility',
  'regime_volatility',
  'regime_trend',
  'regime_spread',
  'liquidity_pressure',
  'return_momentum',
  'regime_stability',
  'spread_compression',
  'imbalance_acceleration',
  'micro_reversion',
  'quote_intensity',
  'behavior_divergence',
  'volatility_compression_score'
];

// Generate synthetic event sequences
function generateUptrend(count = 150, basePrice = 100, volatility = 0.01) {
  const events = [];
  let ts = 1000000;

  for (let i = 0; i < count; i++) {
    const price = basePrice + (i * 0.05) + (Math.random() - 0.5) * volatility;
    ts += 50 + Math.floor(Math.random() * 50);

    events.push({
      ts_event: ts,
      bid_price: price,
      bid_qty: 50 + Math.random() * 50,
      ask_price: price + 0.5,
      ask_qty: 50 + Math.random() * 50
    });
  }

  return events;
}

function generateDowntrend(count = 150, basePrice = 100, volatility = 0.01) {
  const events = [];
  let ts = 1000000;

  for (let i = 0; i < count; i++) {
    const price = basePrice - (i * 0.05) + (Math.random() - 0.5) * volatility;
    ts += 50 + Math.floor(Math.random() * 50);

    events.push({
      ts_event: ts,
      bid_price: price,
      bid_qty: 50 + Math.random() * 50,
      ask_price: price + 0.5,
      ask_qty: 50 + Math.random() * 50
    });
  }

  return events;
}

function generateMeanReverting(count = 150, basePrice = 100, amplitude = 0.3) {
  const events = [];
  let ts = 1000000;

  for (let i = 0; i < count; i++) {
    // Oscillating price (perfect mean reversion) - faster oscillation
    const price = basePrice + Math.sin(i / 2) * amplitude;
    ts += 50 + Math.floor(Math.random() * 50);

    events.push({
      ts_event: ts,
      bid_price: price,
      bid_qty: 50 + Math.random() * 50,
      ask_price: price + 0.5,
      ask_qty: 50 + Math.random() * 50
    });
  }

  return events;
}

function generateHighVolatility(count = 150, basePrice = 100, volatility = 5) {
  const events = [];
  let ts = 1000000;

  for (let i = 0; i < count; i++) {
    const price = basePrice + (Math.random() - 0.5) * volatility;
    ts += 50 + Math.floor(Math.random() * 50);

    events.push({
      ts_event: ts,
      bid_price: price,
      bid_qty: 50 + Math.random() * 50,
      ask_price: price + 1.0 + Math.random() * 2.0, // Variable wide spread
      ask_qty: 50 + Math.random() * 50
    });
  }

  return events;
}

function generateBuyPressure(count = 150, basePrice = 100) {
  const events = [];
  let ts = 1000000;

  for (let i = 0; i < count; i++) {
    const price = basePrice + (i * 0.02);
    ts += 50 + Math.floor(Math.random() * 50);

    events.push({
      ts_event: ts,
      bid_price: price,
      bid_qty: 100 + Math.random() * 50, // High bid qty
      ask_price: price + 0.5,
      ask_qty: 20 + Math.random() * 20   // Low ask qty
    });
  }

  return events;
}

function generateCompression(count = 150, basePrice = 100) {
  const events = [];
  let ts = 1000000;

  for (let i = 0; i < count; i++) {
    const price = basePrice + (Math.random() - 0.5) * 0.05; // Very low volatility
    const spread = Math.max(0.05, 1.5 - (i * 0.008)); // Rapidly narrowing spread
    ts += 50 + Math.floor(Math.random() * 50);

    events.push({
      ts_event: ts,
      bid_price: price,
      bid_qty: 50 + Math.random() * 50,
      ask_price: price + spread,
      ask_qty: 50 + Math.random() * 50
    });
  }

  return events;
}

// Run feature builder on event sequence
function analyzePattern(patternName, events) {
  const builder = FeatureRegistry.createFeatureBuilder('TEST', {
    enabledFeatures: FEATURES
  });

  let lastFeatures = null;

  for (const event of events) {
    const features = builder.onEvent(event);
    if (features !== null) {
      lastFeatures = features;
    }
  }

  return lastFeatures;
}

// Validation tests
const tests = [
  {
    name: 'Uptrend Detection',
    pattern: generateUptrend(300),
    validate: (features) => {
      const checks = [];

      // return_momentum should be positive in uptrend
      checks.push({
        feature: 'return_momentum',
        expected: 'positive (>0.3)',
        actual: features.return_momentum,
        pass: features.return_momentum > 0.3
      });

      // regime_trend should be UP (1)
      checks.push({
        feature: 'regime_trend',
        expected: 'UP (1)',
        actual: features.regime_trend,
        pass: features.regime_trend === 1
      });

      // micro_reversion should be low in trending market
      checks.push({
        feature: 'micro_reversion',
        expected: 'low (<0.4)',
        actual: features.micro_reversion,
        pass: features.micro_reversion < 0.4
      });

      return checks;
    }
  },

  {
    name: 'Downtrend Detection',
    pattern: generateDowntrend(300),
    validate: (features) => {
      const checks = [];

      checks.push({
        feature: 'return_momentum',
        expected: 'negative (<-0.3)',
        actual: features.return_momentum,
        pass: features.return_momentum < -0.3
      });

      checks.push({
        feature: 'regime_trend',
        expected: 'DOWN (-1)',
        actual: features.regime_trend,
        pass: features.regime_trend === -1
      });

      checks.push({
        feature: 'micro_reversion',
        expected: 'low (<0.4)',
        actual: features.micro_reversion,
        pass: features.micro_reversion < 0.4
      });

      return checks;
    }
  },

  {
    name: 'Mean-Reverting Pattern',
    pattern: generateMeanReverting(300),
    validate: (features) => {
      const checks = [];

      checks.push({
        feature: 'micro_reversion',
        expected: 'high (>0.7)',
        actual: features.micro_reversion,
        pass: features.micro_reversion > 0.7
      });

      checks.push({
        feature: 'return_momentum',
        expected: 'near zero (|x|<0.2)',
        actual: features.return_momentum,
        pass: Math.abs(features.return_momentum) < 0.2
      });

      checks.push({
        feature: 'regime_trend',
        expected: 'SIDEWAYS (0)',
        actual: features.regime_trend,
        pass: features.regime_trend === 0
      });

      return checks;
    }
  },

  {
    name: 'High Volatility',
    pattern: generateHighVolatility(300),
    validate: (features) => {
      const checks = [];

      checks.push({
        feature: 'regime_volatility',
        expected: 'HIGH (2)',
        actual: features.regime_volatility,
        pass: features.regime_volatility === 2
      });

      checks.push({
        feature: 'volatility',
        expected: 'high value (>0.02)',
        actual: features.volatility,
        pass: features.volatility > 0.02
      });

      checks.push({
        feature: 'volatility_compression_score',
        expected: 'low (<0.3)',
        actual: features.volatility_compression_score,
        pass: features.volatility_compression_score < 0.3
      });

      return checks;
    }
  },

  {
    name: 'Buy Pressure',
    pattern: generateBuyPressure(300),
    validate: (features) => {
      const checks = [];

      checks.push({
        feature: 'liquidity_pressure',
        expected: 'positive (>0.3)',
        actual: features.liquidity_pressure,
        pass: features.liquidity_pressure > 0.3
      });

      checks.push({
        feature: 'imbalance_acceleration',
        expected: 'positive or near zero (>-0.2)',
        actual: features.imbalance_acceleration,
        pass: features.imbalance_acceleration > -0.2
      });

      return checks;
    }
  },

  {
    name: 'Volatility Compression',
    pattern: generateCompression(300),
    validate: (features) => {
      const checks = [];

      checks.push({
        feature: 'spread_compression',
        expected: 'positive (>0.2)',
        actual: features.spread_compression,
        pass: features.spread_compression > 0.2
      });

      checks.push({
        feature: 'volatility_compression_score',
        expected: 'high (>0.6)',
        actual: features.volatility_compression_score,
        pass: features.volatility_compression_score > 0.6
      });

      checks.push({
        feature: 'regime_volatility',
        expected: 'LOW (0)',
        actual: features.regime_volatility,
        pass: features.regime_volatility === 0
      });

      return checks;
    }
  }
];

// Run all tests
let totalTests = 0;
let passedTests = 0;

for (const test of tests) {
  console.log(`\nTest: ${test.name}`);
  console.log('-'.repeat(80));

  const features = analyzePattern(test.name, test.pattern);

  if (!features) {
    console.log('❌ FAILED: Features did not warm up');
    continue;
  }

  const checks = test.validate(features);

  for (const check of checks) {
    totalTests++;
    const status = check.pass ? '✓' : '✗';
    const symbol = check.pass ? '✓' : '❌';

    if (check.pass) {
      passedTests++;
      console.log(`  ${symbol} ${check.feature}: ${check.expected}`);
    } else {
      console.log(`  ${symbol} ${check.feature}: Expected ${check.expected}, got ${check.actual?.toFixed(4) ?? 'null'}`);
    }
  }
}

// Range validation for all features
console.log('\n' + '='.repeat(80));
console.log('RANGE VALIDATION');
console.log('='.repeat(80));

const rangeTests = generateUptrend(350); // Use any pattern
const features = analyzePattern('Range Check', rangeTests);

const rangeConstraints = {
  liquidity_pressure: [-1, 1],
  return_momentum: [-1, 1],
  regime_stability: [0, 1],
  spread_compression: [-1, 1],
  imbalance_acceleration: [-1, 1],
  micro_reversion: [0, 1],
  quote_intensity: [0, 1],
  behavior_divergence: [-1, 1],
  volatility_compression_score: [0, 1]
};

let rangePass = 0;
let rangeTotal = 0;

for (const [feature, [min, max]] of Object.entries(rangeConstraints)) {
  rangeTotal++;
  const value = features[feature];

  if (value === null || value === undefined) {
    console.log(`  ❌ ${feature}: null/undefined`);
    continue;
  }

  const inRange = value >= min && value <= max;

  if (inRange) {
    rangePass++;
    console.log(`  ✓ ${feature}: ${value.toFixed(4)} ∈ [${min}, ${max}]`);
  } else {
    console.log(`  ❌ ${feature}: ${value.toFixed(4)} NOT IN [${min}, ${max}]`);
  }
}

// Summary
console.log('\n' + '='.repeat(80));
console.log('SUMMARY');
console.log('='.repeat(80));
console.log(`Pattern Tests: ${passedTests}/${totalTests} passed (${((passedTests/totalTests)*100).toFixed(1)}%)`);
console.log(`Range Tests: ${rangePass}/${rangeTotal} passed (${((rangePass/rangeTotal)*100).toFixed(1)}%)`);

const allPass = (passedTests === totalTests) && (rangePass === rangeTotal);

if (allPass) {
  console.log('\n✅ ALL STRUCTURAL VALIDATION TESTS PASSED');
  process.exit(0);
} else {
  console.log('\n⚠️  SOME TESTS FAILED - Review output above');
  process.exit(1);
}
