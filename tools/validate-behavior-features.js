#!/usr/bin/env node

/**
 * Behavior Feature Validation Script
 *
 * Purpose: Validate hypothesis that behavior features predict price outcomes.
 *
 * Analysis:
 * 1. Feature-Outcome Correlation
 *    - For each behavior feature, calculate correlation with future price movement
 *    - Test statistical significance (p < 0.05)
 *
 * 2. Regime Stratification
 *    - Does high regime_stability improve edge performance?
 *    - Compare win rate in stable vs unstable regimes
 *
 * 3. Incremental Value
 *    - Does adding behavior features improve predictions?
 *    - Compare baseline vs behavior-enhanced
 *
 * Usage:
 *   node tools/validate-behavior-features.js --dataset adausdt_20260203 --horizon 10000
 */

import { ReplayEngine } from '../core/replay/ReplayEngine.js';
import { FeatureRegistry } from '../core/features/FeatureRegistry.js';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Parse CLI args
const args = process.argv.slice(2);
const datasetArg = args.find(a => a.startsWith('--dataset='));
const horizonArg = args.find(a => a.startsWith('--horizon='));

if (!datasetArg) {
  console.error('Usage: node validate-behavior-features.js --dataset=<dataset> [--horizon=10000]');
  process.exit(1);
}

const dataset = datasetArg.split('=')[1];
const horizonMs = parseInt(horizonArg?.split('=')[1] || '10000', 10);

// Configuration
const config = {
  dataset,
  horizonMs,
  features: [
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
  ],
  behaviorFeatures: [
    'liquidity_pressure',
    'return_momentum',
    'regime_stability',
    'spread_compression',
    'imbalance_acceleration',
    'micro_reversion',
    'quote_intensity',
    'behavior_divergence',
    'volatility_compression_score'
  ]
};

console.log('='.repeat(80));
console.log('BEHAVIOR FEATURE VALIDATION');
console.log('='.repeat(80));
console.log(`Dataset: ${config.dataset}`);
console.log(`Prediction Horizon: ${config.horizonMs}ms`);
console.log(`Behavior Features: ${config.behaviorFeatures.join(', ')}`);
console.log('='.repeat(80));
console.log();

// Main validation
async function validate() {
  // Step 1: Replay and extract features + outcomes
  console.log('Step 1: Extracting features and outcomes...');
  const data = await extractFeaturesAndOutcomes(config);

  if (data.length === 0) {
    console.error('ERROR: No data extracted. Check dataset path.');
    process.exit(1);
  }

  console.log(`✓ Extracted ${data.length} samples\n`);

  // Step 2: Feature-Outcome Correlation
  console.log('Step 2: Analyzing feature-outcome correlations...');
  console.log('-'.repeat(80));
  analyzeCorrelations(data, config.behaviorFeatures);
  console.log();

  // Step 3: Regime Stratification
  console.log('Step 3: Regime stratification analysis...');
  console.log('-'.repeat(80));
  analyzeRegimeStratification(data);
  console.log();

  // Step 4: Summary
  console.log('='.repeat(80));
  console.log('VALIDATION SUMMARY');
  console.log('='.repeat(80));
  console.log('Review the results above to determine if behavior features are predictive.');
  console.log();
  console.log('Success Criteria:');
  console.log('  ✓ At least 1 behavior feature has |correlation| > 0.05 with p < 0.05');
  console.log('  ✓ Win rate is higher in stable regimes (regime_stability > 0.7)');
  console.log('  ✓ Features show consistent behavior across time');
  console.log('='.repeat(80));
}

/**
 * Extract features and future outcomes from replay
 */
async function extractFeaturesAndOutcomes(config) {
  const dataPath = path.join(__dirname, '../data/compact', config.dataset);

  // Find parquet file
  const fs = await import('fs');
  const files = fs.readdirSync(dataPath);
  const parquetFile = files.find(f => f.endsWith('.parquet'));

  if (!parquetFile) {
    throw new Error(`No parquet file found in ${dataPath}`);
  }

  const parquetPath = path.join(dataPath, parquetFile);
  const metaPath = path.join(dataPath, 'meta.json');

  const replay = new ReplayEngine({
    parquet: parquetPath,
    meta: metaPath
  });

  // Create feature builder
  const builder = FeatureRegistry.createFeatureBuilder('SYMBOL', {
    enabledFeatures: config.features
  });

  const data = [];
  let eventBuffer = [];
  let count = 0;

  for await (const event of replay) {
    const features = builder.onEvent(event);

    if (features) {
      // Store event with features for outcome calculation
      eventBuffer.push({
        ts: event.ts_event,
        mid: features.mid_price,
        features
      });

      // Calculate outcome for events that are old enough
      while (eventBuffer.length > 0) {
        const old = eventBuffer[0];
        const elapsed = event.ts_event - old.ts;

        if (elapsed >= config.horizonMs) {
          // Calculate outcome (future price movement)
          const futureReturn = (features.mid_price - old.mid) / old.mid;

          data.push({
            ...old.features,
            outcome: futureReturn,
            outcome_direction: futureReturn > 0 ? 1 : -1
          });

          eventBuffer.shift();
        } else {
          break;
        }
      }

      count++;
      if (count % 10000 === 0) {
        process.stdout.write(`\r  Processed ${count} events, extracted ${data.length} samples...`);
      }
    }
  }

  process.stdout.write(`\r  Processed ${count} events, extracted ${data.length} samples   \n`);

  return data;
}

/**
 * Analyze correlation between features and outcomes
 */
function analyzeCorrelations(data, behaviorFeatures) {
  for (const featureName of behaviorFeatures) {
    const featureValues = data.map(d => d[featureName]).filter(v => v !== null && v !== undefined);
    const outcomeValues = data.map(d => d.outcome).filter(v => v !== null && v !== undefined);

    if (featureValues.length !== outcomeValues.length) {
      console.log(`${featureName}: SKIPPED (missing data)`);
      continue;
    }

    const correlation = calculateCorrelation(featureValues, outcomeValues);
    const pValue = calculatePValue(correlation, featureValues.length);

    const significant = pValue < 0.05 ? '***' : (pValue < 0.10 ? '**' : '');
    const predictive = Math.abs(correlation) > 0.05 ? 'PREDICTIVE' : 'WEAK';

    console.log(`  ${featureName.padEnd(25)} r=${correlation.toFixed(4)}  p=${pValue.toFixed(4)} ${significant}  ${predictive}`);
  }
}

/**
 * Analyze performance stratified by regime stability
 */
function analyzeRegimeStratification(data) {
  const highStability = data.filter(d => d.regime_stability > 0.7);
  const lowStability = data.filter(d => d.regime_stability <= 0.4);

  console.log(`  High Stability (>0.7): ${highStability.length} samples`);
  console.log(`    Win Rate: ${calculateWinRate(highStability).toFixed(2)}%`);
  console.log(`    Avg Return: ${calculateAvgReturn(highStability).toFixed(6)}`);
  console.log();

  console.log(`  Low Stability (<=0.4): ${lowStability.length} samples`);
  console.log(`    Win Rate: ${calculateWinRate(lowStability).toFixed(2)}%`);
  console.log(`    Avg Return: ${calculateAvgReturn(lowStability).toFixed(6)}`);
  console.log();

  const diff = calculateWinRate(highStability) - calculateWinRate(lowStability);
  const hypothesis = diff > 5 ? 'CONFIRMED' : (diff < -5 ? 'REJECTED' : 'INCONCLUSIVE');
  console.log(`  Hypothesis (stable > unstable): ${diff > 0 ? '+' : ''}${diff.toFixed(2)}% difference - ${hypothesis}`);
}

/**
 * Calculate Pearson correlation
 */
function calculateCorrelation(x, y) {
  const n = x.length;
  const sumX = x.reduce((a, b) => a + b, 0);
  const sumY = y.reduce((a, b) => a + b, 0);
  const sumXY = x.reduce((sum, xi, i) => sum + xi * y[i], 0);
  const sumX2 = x.reduce((sum, xi) => sum + xi * xi, 0);
  const sumY2 = y.reduce((sum, yi) => sum + yi * yi, 0);

  const numerator = n * sumXY - sumX * sumY;
  const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

  return denominator === 0 ? 0 : numerator / denominator;
}

/**
 * Calculate approximate p-value for correlation
 * Using t-distribution approximation
 */
function calculatePValue(r, n) {
  if (n < 3) return 1;

  const t = r * Math.sqrt(n - 2) / Math.sqrt(1 - r * r);
  const df = n - 2;

  // Approximate p-value using t-distribution
  // For large n, |t| > 1.96 → p < 0.05
  // This is a rough approximation
  if (Math.abs(t) > 2.576) return 0.01;  // p < 0.01
  if (Math.abs(t) > 1.96) return 0.05;   // p < 0.05
  if (Math.abs(t) > 1.645) return 0.10;  // p < 0.10
  return 0.20;  // p > 0.10
}

/**
 * Calculate win rate
 */
function calculateWinRate(data) {
  const wins = data.filter(d => d.outcome > 0).length;
  return (wins / data.length) * 100;
}

/**
 * Calculate average return
 */
function calculateAvgReturn(data) {
  const sum = data.reduce((s, d) => s + d.outcome, 0);
  return sum / data.length;
}

// Run validation
validate().catch(err => {
  console.error('ERROR:', err.message);
  console.error(err.stack);
  process.exit(1);
});
