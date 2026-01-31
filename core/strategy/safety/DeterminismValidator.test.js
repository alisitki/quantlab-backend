#!/usr/bin/env node
/**
 * DeterminismValidator Test Suite
 * 
 * PHASE 1: Determinism Foundation — Verification
 * 
 * Tests:
 * 1. Hash computation consistency
 * 2. Run ID determinism
 * 3. Ordering validation
 * 4. Twin-run comparison
 */

import assert from 'node:assert';
import {
  computeHash,
  computeStateHash,
  computeFillsHash,
  computeRunId,
  compareEventOrder,
  assertMonotonic,
  compareTwinRuns,
  createIncrementalHasher,
  verifyHash
} from './DeterminismValidator.js';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// HASH COMPUTATION TESTS
// ============================================================================

test('computeHash produces consistent results', () => {
  const obj = { a: 1, b: 2 };
  
  const hash1 = computeHash(obj);
  const hash2 = computeHash(obj);
  
  assert.strictEqual(hash1, hash2, 'Same object should produce same hash');
  assert.strictEqual(hash1.length, 64, 'SHA256 hex should be 64 characters');
});

test('computeHash is order-independent for objects', () => {
  const obj1 = { z: 1, a: 2 };
  const obj2 = { a: 2, z: 1 };
  
  assert.strictEqual(computeHash(obj1), computeHash(obj2), 
    'Objects with same keys in different order should have same hash');
});

test('computeHash handles BigInt', () => {
  const obj = { ts_event: 1234567890123456789n };
  
  // Should not throw
  const hash = computeHash(obj);
  assert.ok(hash, 'Should produce hash for BigInt values');
});

test('computeStateHash normalizes structure', () => {
  const state1 = { cursor: { ts: 1n }, executionState: {}, strategyState: {} };
  const state2 = { strategyState: {}, cursor: { ts: 1n }, executionState: {} };
  
  // Even with different key order, should produce same hash
  assert.strictEqual(
    computeStateHash(state1), 
    computeStateHash(state2),
    'State hash should be order-independent'
  );
});

test('computeFillsHash produces consistent results', () => {
  const fills = [
    { id: 'fill_1', side: 'BUY', fillPrice: 100, qty: 1, ts_event: 1000n },
    { id: 'fill_2', side: 'SELL', fillPrice: 101, qty: 1, ts_event: 2000n }
  ];
  
  const hash1 = computeFillsHash(fills);
  const hash2 = computeFillsHash(fills);
  
  assert.strictEqual(hash1, hash2, 'Same fills should produce same hash');
});

test('computeFillsHash handles empty array', () => {
  const hash = computeFillsHash([]);
  assert.ok(hash, 'Should produce hash for empty fills');
});

// ============================================================================
// RUN ID TESTS
// ============================================================================

test('computeRunId is deterministic', () => {
  const params = {
    dataset: { parquet: 's3://bucket/data.parquet', meta: 's3://bucket/meta.json' },
    config: { fastPeriod: 9, slowPeriod: 21 },
    seed: 'test-seed'
  };
  
  const id1 = computeRunId(params);
  const id2 = computeRunId(params);
  
  assert.strictEqual(id1, id2, 'Same params should produce same run ID');
  assert.ok(id1.startsWith('run_'), 'Run ID should start with run_');
  assert.strictEqual(id1.length, 20, 'Run ID should be run_ + 16 chars');
});

test('computeRunId differs with different inputs', () => {
  const params1 = {
    dataset: { parquet: 's3://bucket/data1.parquet', meta: 's3://bucket/meta.json' }
  };
  const params2 = {
    dataset: { parquet: 's3://bucket/data2.parquet', meta: 's3://bucket/meta.json' }
  };
  
  assert.notStrictEqual(
    computeRunId(params1), 
    computeRunId(params2),
    'Different datasets should produce different run IDs'
  );
});

test('computeRunId seed affects result', () => {
  const params1 = {
    dataset: { parquet: 's3://bucket/data.parquet', meta: 's3://bucket/meta.json' },
    seed: 'seed1'
  };
  const params2 = {
    dataset: { parquet: 's3://bucket/data.parquet', meta: 's3://bucket/meta.json' },
    seed: 'seed2'
  };
  
  assert.notStrictEqual(
    computeRunId(params1), 
    computeRunId(params2),
    'Different seeds should produce different run IDs'
  );
});

// ============================================================================
// ORDERING VALIDATION TESTS
// ============================================================================

test('compareEventOrder allows valid forward progression', () => {
  const prev = { ts_event: 100n, seq: 1n };
  const curr = { ts_event: 100n, seq: 2n };
  
  const result = compareEventOrder(prev, curr);
  assert.ok(result.ok, 'Forward seq should be valid');
});

test('compareEventOrder allows timestamp increase', () => {
  const prev = { ts_event: 100n, seq: 99n };
  const curr = { ts_event: 101n, seq: 1n };
  
  const result = compareEventOrder(prev, curr);
  assert.ok(result.ok, 'Increased timestamp should reset seq allowance');
});

test('compareEventOrder detects backward movement', () => {
  const prev = { ts_event: 100n, seq: 5n };
  const curr = { ts_event: 100n, seq: 3n };
  
  const result = compareEventOrder(prev, curr);
  assert.ok(!result.ok, 'Backward seq should be invalid');
  assert.ok(result.error.includes('ORDERING_VIOLATION'), 'Should report violation');
});

test('compareEventOrder detects duplicate', () => {
  const prev = { ts_event: 100n, seq: 5n };
  const curr = { ts_event: 100n, seq: 5n };
  
  const result = compareEventOrder(prev, curr);
  assert.ok(!result.ok, 'Duplicate should be invalid');
});

test('assertMonotonic throws on violation', () => {
  const prev = { ts_event: 100n, seq: 5n };
  const curr = { ts_event: 100n, seq: 3n };
  
  assert.throws(
    () => assertMonotonic(prev, curr),
    /ORDERING_VIOLATION/,
    'Should throw with ORDERING_VIOLATION message'
  );
});

test('assertMonotonic allows null prev', () => {
  const curr = { ts_event: 100n, seq: 1n };
  
  // Should not throw
  assertMonotonic(null, curr);
});

// ============================================================================
// TWIN-RUN COMPARISON TESTS
// ============================================================================

test('compareTwinRuns detects match', () => {
  const run1 = { stateHash: 'abc123', fillsHash: 'def456', eventCount: 1000 };
  const run2 = { stateHash: 'abc123', fillsHash: 'def456', eventCount: 1000 };
  
  const result = compareTwinRuns(run1, run2);
  assert.ok(result.match, 'Identical runs should match');
});

test('compareTwinRuns detects state mismatch', () => {
  const run1 = { stateHash: 'abc123', fillsHash: 'def456', eventCount: 1000 };
  const run2 = { stateHash: 'xyz789', fillsHash: 'def456', eventCount: 1000 };
  
  const result = compareTwinRuns(run1, run2);
  assert.ok(!result.match, 'State mismatch should not match');
  assert.ok(!result.details.stateHash.match, 'Details should show state mismatch');
});

test('compareTwinRuns detects fills mismatch', () => {
  const run1 = { stateHash: 'abc123', fillsHash: 'def456', eventCount: 1000 };
  const run2 = { stateHash: 'abc123', fillsHash: 'xyz789', eventCount: 1000 };
  
  const result = compareTwinRuns(run1, run2);
  assert.ok(!result.match, 'Fills mismatch should not match');
});

// ============================================================================
// INCREMENTAL HASHER TESTS
// ============================================================================

test('createIncrementalHasher produces consistent results', () => {
  const hasher1 = createIncrementalHasher();
  hasher1.update({ a: 1 });
  hasher1.update({ b: 2 });
  const hash1 = hasher1.digest();
  
  const hasher2 = createIncrementalHasher();
  hasher2.update({ a: 1 });
  hasher2.update({ b: 2 });
  const hash2 = hasher2.digest();
  
  assert.strictEqual(hash1, hash2, 'Same updates should produce same hash');
});

test('verifyHash works correctly', () => {
  const obj = { test: 'data' };
  const hash = computeHash(obj);
  
  assert.ok(verifyHash(obj, hash), 'Correct hash should verify');
  assert.ok(!verifyHash(obj, 'wrong-hash'), 'Wrong hash should not verify');
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== DeterminismValidator Test Suite ===\n');
  
  let passed = 0;
  let failed = 0;
  
  for (const { name, fn } of tests) {
    try {
      await fn();
      console.log(`✓ ${name}`);
      passed++;
    } catch (err) {
      console.error(`✗ ${name}`);
      console.error(`  ${err.message}`);
      failed++;
    }
  }
  
  console.log(`\n--- Results ---`);
  console.log(`Passed: ${passed}/${tests.length}`);
  console.log(`Failed: ${failed}/${tests.length}`);
  
  if (failed > 0) {
    console.log('\nRESULT: FAIL');
    process.exit(1);
  } else {
    console.log('\nRESULT: PASS');
    process.exit(0);
  }
}

run();
