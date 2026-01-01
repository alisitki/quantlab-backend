/**
 * test-audit-logger.js: Unit tests for AuditLogger
 */
import { appendEntry, readEntries, hasEntries, getEntryCount, AUDIT_DIRECTORY } from './AuditLogger.js';
import fs from 'fs';
import path from 'path';

async function runTests() {
  console.log('=== AuditLogger Unit Tests ===\n');
  
  let passed = 0;
  let failed = 0;
  
  function assert(condition, testName) {
    if (condition) {
      console.log(`✅ ${testName}`);
      passed++;
    } else {
      console.log(`❌ ${testName}`);
      failed++;
    }
  }
  
  // Use test date to avoid polluting real data
  const testDate = '99991231';
  const testFilePath = path.join(AUDIT_DIRECTORY, `${testDate}.json`);
  
  // Cleanup before test
  if (fs.existsSync(testFilePath)) {
    fs.unlinkSync(testFilePath);
  }
  
  // Test 1: First entry creates file
  console.log('\n--- Test 1: First Entry ---');
  const hasBefore = hasEntries(testDate);
  assert(hasBefore === false, 'hasEntries should be false before any writes');
  
  appendEntry(testDate, {
    symbol: 'btcusdt',
    mode: 'DRY',
    job_id: 'test-job-1',
    training_status: 'SUCCESS'
  });
  
  const hasAfter = hasEntries(testDate);
  assert(hasAfter === true, 'hasEntries should be true after write');
  
  // Test 2: Read entries
  console.log('\n--- Test 2: Read Entry ---');
  const entries1 = readEntries(testDate);
  assert(entries1.length === 1, 'should have 1 entry');
  assert(entries1[0].symbol === 'btcusdt', 'symbol should match');
  assert(entries1[0].mode === 'DRY', 'mode should match');
  assert(entries1[0].job_id === 'test-job-1', 'job_id should match');
  assert(entries1[0].timestamp !== null, 'timestamp should be set');
  
  // Test 3: Append another entry (append-only)
  console.log('\n--- Test 3: Append Second Entry ---');
  appendEntry(testDate, {
    symbol: 'ethusdt',
    mode: 'LIVE',
    job_id: 'test-job-2',
    training_status: 'SUCCESS',
    promotion_decision: { safety_pass: true, reasons: [] }
  });
  
  const entries2 = readEntries(testDate);
  assert(entries2.length === 2, 'should have 2 entries');
  assert(entries2[0].symbol === 'btcusdt', 'first entry should be unchanged');
  assert(entries2[1].symbol === 'ethusdt', 'second entry should be appended');
  assert(entries2[1].promotion_decision.safety_pass === true, 'promotion_decision should be nested');
  
  // Test 4: Entry count
  console.log('\n--- Test 4: Entry Count ---');
  const count = getEntryCount(testDate);
  assert(count === 2, 'count should be 2');
  
  // Test 5: Non-existent date
  console.log('\n--- Test 5: Non-existent Date ---');
  const emptyEntries = readEntries('00000000');
  assert(emptyEntries.length === 0, 'should return empty array for non-existent date');
  
  // Test 6: Default fields filled
  console.log('\n--- Test 6: Default Fields ---');
  const entries = readEntries(testDate);
  assert(entries[0].date === testDate, 'date should be filled');
  assert(entries[0].training_error === null, 'training_error should default to null');
  assert(entries[0].prod_hash_before === null, 'prod_hash_before should default to null');
  
  // Cleanup after test
  if (fs.existsSync(testFilePath)) {
    fs.unlinkSync(testFilePath);
    console.log(`\n[Cleanup] Removed test file ${testFilePath}`);
  }
  
  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}/${passed + failed}`);
  console.log(`Failed: ${failed}/${passed + failed}`);
  
  if (failed > 0) {
    console.log('\n❌ TESTS FAILED');
    process.exit(1);
  } else {
    console.log('\n✅ ALL AUDIT LOGGER TESTS PASSED');
  }
}

runTests().catch(err => {
  console.error('Test runner error:', err);
  process.exit(1);
});
