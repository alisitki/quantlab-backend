#!/usr/bin/env node
/**
 * test-scheduler.js: Verification for JobSpecGenerator determinism.
 */
import { JobSpecGenerator } from './JobSpecGenerator.js';
import crypto from 'crypto';

async function runTest() {
  console.log('--- Scheduler Module Verification ---\n');
  let passed = 0;
  let failed = 0;
  
  // Test 1: Hash consistency
  console.log('1. Testing JobSpec Hash Consistency...');
  const spec1 = JobSpecGenerator.generate({ symbol: 'btcusdt', date: '20251228' });
  const spec2 = JobSpecGenerator.generate({ symbol: 'btcusdt', date: '20251228' });
  
  if (spec1.configHash === spec2.configHash && spec1.jobId === spec2.jobId) {
    console.log('   ✅ Hash Consistency: SUCCESS');
    console.log(`      Hash: ${spec1.configHash.substring(0, 32)}...`);
    passed++;
  } else {
    console.log('   ❌ Hash Consistency: FAILED');
    console.log(`      Hash1: ${spec1.configHash}`);
    console.log(`      Hash2: ${spec2.configHash}`);
    failed++;
  }
  
  // Test 2: Different dates produce different hashes
  console.log('\n2. Testing Different Dates = Different Hashes...');
  const spec3 = JobSpecGenerator.generate({ symbol: 'btcusdt', date: '20251229' });
  
  if (spec1.configHash !== spec3.configHash) {
    console.log('   ✅ Date Differentiation: SUCCESS');
    passed++;
  } else {
    console.log('   ❌ Date Differentiation: FAILED (Same hash for different dates!)');
    failed++;
  }
  
  // Test 3: Different symbols produce different hashes
  console.log('\n3. Testing Different Symbols = Different Hashes...');
  const spec4 = JobSpecGenerator.generate({ symbol: 'ethusdt', date: '20251228' });
  
  if (spec1.configHash !== spec4.configHash) {
    console.log('   ✅ Symbol Differentiation: SUCCESS');
    passed++;
  } else {
    console.log('   ❌ Symbol Differentiation: FAILED');
    failed++;
  }
  
  // Test 4: Batch generation
  console.log('\n4. Testing Batch Generation...');
  const batch = JobSpecGenerator.generateBatch({ 
    symbols: ['btcusdt', 'ethusdt', 'solusdt'], 
    date: '20251228' 
  });
  
  if (batch.length === 3 && new Set(batch.map(s => s.configHash)).size === 3) {
    console.log('   ✅ Batch Generation: SUCCESS (3 unique jobs)');
    passed++;
  } else {
    console.log('   ❌ Batch Generation: FAILED');
    failed++;
  }
  
  // Test 5: JobSpec fields populated
  console.log('\n5. Testing JobSpec Structure...');
  const requiredFields = ['jobId', 'dataset', 'model', 'runtime', 'output', 'configHash'];
  const missing = requiredFields.filter(f => !(f in spec1));
  
  if (missing.length === 0) {
    console.log('   ✅ Structure: SUCCESS (All required fields present)');
    
    // Test 5b: Feature paths specifically
    const hasFeaturePath = spec1.dataset.featurePath?.startsWith('s3://');
    const hasMetaPath = spec1.dataset.metaPath?.startsWith('s3://');
    if (hasFeaturePath && hasMetaPath) {
      console.log('   ✅ Feature Paths: SUCCESS');
      console.log(`      Path: ${spec1.dataset.featurePath}`);
      passed++;
    } else {
      console.log('   ❌ Feature Paths: FAILED');
      failed++;
    }
    passed++;
  } else {
    console.log(`   ❌ Structure: FAILED (Missing: ${missing.join(', ')})`);
    failed++;
  }
  
  // Test 6: Date functions
  console.log('\n6. Testing Date Functions...');
  const yesterday = JobSpecGenerator.getYesterdayDate();
  const isValidFormat = /^\d{8}$/.test(yesterday);
  
  if (isValidFormat) {
    console.log(`   ✅ Date Format: SUCCESS (${yesterday})`);
    passed++;
  } else {
    console.log(`   ❌ Date Format: FAILED (${yesterday})`);
    failed++;
  }
  
  // Summary
  console.log('\n--- Summary ---');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  
  if (failed > 0) {
    process.exit(1);
  }
}

runTest().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
