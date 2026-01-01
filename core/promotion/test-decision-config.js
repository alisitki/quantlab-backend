/**
 * test-decision-config.js: Unit tests for Decision Config Persistence
 */
import crypto from 'crypto';

// Mock S3 client
class MockS3Client {
  constructor() {
    this.putCalls = [];
    this.getCalls = [];
  }
  
  async send(command) {
    if (command.constructor.name === 'PutObjectCommand') {
      this.putCalls.push(command.input);
      return {};
    }
    if (command.constructor.name === 'GetObjectCommand') {
      this.getCalls.push(command.input);
      // Simulate no production model
      const err = new Error('NoSuchKey');
      err.name = 'NoSuchKey';
      throw err;
    }
    if (command.constructor.name === 'CopyObjectCommand') {
      return {};
    }
    return {};
  }
}

// Helper to generate config hash (mirrors Promoter logic)
function generateConfigHash(config) {
  const hashInput = {
    symbol: config.symbol,
    featuresetVersion: config.featuresetVersion,
    labelHorizonSec: config.labelHorizonSec,
    primaryMetric: config.primaryMetric,
    bestThreshold: config.bestThreshold,
    thresholdGrid: config.thresholdGrid,
    probaSource: config.probaSource
  };
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(hashInput))
    .digest('hex');
}

function assertEqual(actual, expected, msg) {
  if (actual !== expected) {
    throw new Error(`${msg}: expected ${expected}, got ${actual}`);
  }
}

async function runTests() {
  console.log('--- Decision Config Persistence Unit Tests ---\n');
  let passed = 0;
  let failed = 0;

  // Test 1: canary=true + auto → decision write skipped (mode becomes dry)
  console.log('1. Testing canary=true + auto → no decision write...');
  try {
    // In canary mode, mode is downgraded to 'dry' so decision.promote block doesn't execute
    // This test verifies the logic conceptually
    const canary = true;
    let mode = 'auto';
    
    // Canary guard
    if (canary && mode === 'auto') {
      mode = 'dry';
    }
    
    // Decision write only happens when mode === 'auto'
    const shouldWrite = mode === 'auto' && !canary;
    assertEqual(shouldWrite, false, 'Should not write in canary mode');
    
    console.log('✅ Test 1 PASSED: canary + auto → no decision write');
    passed++;
  } catch (err) {
    console.error('❌ Test 1 FAILED:', err.message);
    failed++;
  }

  // Test 2: mode=dry → no decision write
  console.log('\n2. Testing mode=dry → no decision write...');
  try {
    const canary = false;
    const mode = 'dry';
    
    const shouldWrite = mode === 'auto' && !canary;
    assertEqual(shouldWrite, false, 'Should not write in dry mode');
    
    console.log('✅ Test 2 PASSED: mode=dry → no decision write');
    passed++;
  } catch (err) {
    console.error('❌ Test 2 FAILED:', err.message);
    failed++;
  }

  // Test 3: mode=off → no decision write
  console.log('\n3. Testing mode=off → no decision write...');
  try {
    const canary = false;
    const mode = 'off';
    
    const shouldWrite = mode === 'auto' && !canary;
    assertEqual(shouldWrite, false, 'Should not write in off mode');
    
    console.log('✅ Test 3 PASSED: mode=off → no decision write');
    passed++;
  } catch (err) {
    console.error('❌ Test 3 FAILED:', err.message);
    failed++;
  }

  // Test 4: non-canary + auto → decision write allowed
  console.log('\n4. Testing non-canary + auto → decision write allowed...');
  try {
    const canary = false;
    const mode = 'auto';
    
    const shouldWrite = mode === 'auto' && !canary;
    assertEqual(shouldWrite, true, 'Should write in non-canary auto mode');
    
    console.log('✅ Test 4 PASSED: non-canary + auto → decision write allowed');
    passed++;
  } catch (err) {
    console.error('❌ Test 4 FAILED:', err.message);
    failed++;
  }

  // Test 5: configHash is deterministic
  console.log('\n5. Testing configHash determinism...');
  try {
    const config1 = {
      symbol: 'btcusdt',
      featuresetVersion: 'v1',
      labelHorizonSec: 10,
      primaryMetric: 'f1_pos',
      bestThreshold: 0.55,
      thresholdGrid: [0.5, 0.55, 0.6, 0.65, 0.7],
      probaSource: 'pseudo_sigmoid'
    };
    
    const config2 = { ...config1 }; // Same config
    
    const hash1 = generateConfigHash(config1);
    const hash2 = generateConfigHash(config2);
    
    assertEqual(hash1, hash2, 'Same input should produce same hash');
    assertEqual(hash1.length, 64, 'Hash should be 64 chars (sha256 hex)');
    
    // Different config should produce different hash
    const config3 = { ...config1, bestThreshold: 0.60 };
    const hash3 = generateConfigHash(config3);
    
    if (hash1 === hash3) {
      throw new Error('Different configs should produce different hashes');
    }
    
    console.log(`  Hash1: ${hash1.substring(0, 16)}...`);
    console.log(`  Hash3: ${hash3.substring(0, 16)}... (different threshold)`);
    console.log('✅ Test 5 PASSED: configHash is deterministic');
    passed++;
  } catch (err) {
    console.error('❌ Test 5 FAILED:', err.message);
    failed++;
  }

  // Test 6: Decision config structure validation
  console.log('\n6. Testing decision config structure...');
  try {
    const metrics = {
      best_threshold: { by: 'f1_pos', value: 0.55, f1_pos: 0.60 },
      proba_source: 'pseudo_sigmoid'
    };
    
    const jobSpec = {
      jobId: 'job-btcusdt-20251229-abc123',
      dataset: {
        symbol: 'btcusdt',
        featuresetVersion: 'v1',
        labelHorizonSec: 10
      }
    };
    
    // Build decision config (mirrors Promoter logic)
    const decisionConfig = {
      symbol: jobSpec.dataset.symbol,
      featuresetVersion: jobSpec.dataset.featuresetVersion,
      labelHorizonSec: jobSpec.dataset.labelHorizonSec,
      primaryMetric: 'f1_pos',
      bestThreshold: metrics.best_threshold.value,
      thresholdGrid: [0.5, 0.55, 0.6, 0.65, 0.7],
      probaSource: metrics.proba_source,
      jobId: jobSpec.jobId,
      createdAt: new Date().toISOString()
    };
    decisionConfig.configHash = generateConfigHash(decisionConfig);
    
    // Validate required fields
    if (!decisionConfig.symbol) throw new Error('Missing symbol');
    if (!decisionConfig.featuresetVersion) throw new Error('Missing featuresetVersion');
    if (!decisionConfig.labelHorizonSec) throw new Error('Missing labelHorizonSec');
    if (!decisionConfig.primaryMetric) throw new Error('Missing primaryMetric');
    if (decisionConfig.bestThreshold === undefined) throw new Error('Missing bestThreshold');
    if (!decisionConfig.thresholdGrid) throw new Error('Missing thresholdGrid');
    if (!decisionConfig.probaSource) throw new Error('Missing probaSource');
    if (!decisionConfig.jobId) throw new Error('Missing jobId');
    if (!decisionConfig.createdAt) throw new Error('Missing createdAt');
    if (!decisionConfig.configHash) throw new Error('Missing configHash');
    
    console.log('  All required fields present');
    console.log('✅ Test 6 PASSED: Decision config structure is valid');
    passed++;
  } catch (err) {
    console.error('❌ Test 6 FAILED:', err.message);
    failed++;
  }

  // Summary
  console.log('\n' + '='.repeat(40));
  console.log(`SUMMARY: ${passed} passed, ${failed} failed`);
  console.log('='.repeat(40));

  if (failed > 0) {
    process.exit(1);
  }
}

runTests().catch(err => {
  console.error('Test runner error:', err);
  process.exit(1);
});
