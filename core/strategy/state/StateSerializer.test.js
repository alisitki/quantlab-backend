#!/usr/bin/env node
/**
 * StateSerializer Test Suite
 * 
 * PHASE 1: Determinism Foundation — Verification
 * 
 * Tests:
 * 1. Canonical stringify produces sorted keys
 * 2. BigInt serialization and restoration
 * 3. Round-trip invariant: parse(stringify(x)) === x
 * 4. Determinism: same object → same string
 */

import assert from 'node:assert';
import { 
  canonicalStringify, 
  canonicalParse, 
  canonicalEquals,
  canonicalClone,
  immutableSnapshot
} from './StateSerializer.js';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// TEST CASES
// ============================================================================

test('canonicalStringify sorts object keys', () => {
  const obj1 = { z: 1, a: 2, m: 3 };
  const obj2 = { a: 2, z: 1, m: 3 };
  const obj3 = { m: 3, z: 1, a: 2 };
  
  const str1 = canonicalStringify(obj1);
  const str2 = canonicalStringify(obj2);
  const str3 = canonicalStringify(obj3);
  
  assert.strictEqual(str1, str2, 'Objects with same keys in different order should produce same string');
  assert.strictEqual(str2, str3, 'All orderings should produce same string');
  assert.strictEqual(str1, '{"a":2,"m":3,"z":1}', 'Keys should be sorted alphabetically');
});

test('canonicalStringify handles nested objects', () => {
  const obj1 = { outer: { z: 1, a: 2 }, b: 3 };
  const obj2 = { b: 3, outer: { a: 2, z: 1 } };
  
  const str1 = canonicalStringify(obj1);
  const str2 = canonicalStringify(obj2);
  
  assert.strictEqual(str1, str2, 'Nested objects should also be sorted');
});

test('canonicalStringify handles BigInt', () => {
  const obj = { ts_event: 1234567890123456789n, seq: 42n };
  const str = canonicalStringify(obj);
  
  assert.ok(str.includes('"seq":"42n"'), 'BigInt should be serialized as string with n suffix');
  assert.ok(str.includes('"ts_event":"1234567890123456789n"'), 'Large BigInt should preserve precision');
});

test('canonicalParse restores BigInt', () => {
  const original = { ts_event: 1234567890123456789n, seq: 42n, count: 100 };
  const str = canonicalStringify(original);
  const parsed = canonicalParse(str);
  
  assert.strictEqual(typeof parsed.ts_event, 'bigint', 'ts_event should be restored as BigInt');
  assert.strictEqual(typeof parsed.seq, 'bigint', 'seq should be restored as BigInt');
  assert.strictEqual(typeof parsed.count, 'number', 'Regular numbers should remain numbers');
  assert.strictEqual(parsed.ts_event, 1234567890123456789n, 'BigInt value should be correct');
});

test('round-trip invariant: parse(stringify(x)) === x', () => {
  const testCases = [
    { a: 1, b: 2 },
    { ts: 123456789012345678n },
    { nested: { deep: { value: 42n } } },
    { arr: [1, 2, 3] },
    { mixed: { num: 42, big: 42n, str: 'hello', arr: [1, 2n, 3] } },
    null,
    [1, 2, 3],
    'string',
    42,
    true
  ];
  
  for (const original of testCases) {
    const str = canonicalStringify(original);
    const parsed = canonicalParse(str);
    const roundTrip = canonicalStringify(parsed);
    
    assert.strictEqual(str, roundTrip, `Round-trip should preserve: ${str}`);
  }
});

test('canonicalEquals works correctly', () => {
  const obj1 = { z: 1, a: 2 };
  const obj2 = { a: 2, z: 1 };
  const obj3 = { a: 2, z: 2 };
  
  assert.ok(canonicalEquals(obj1, obj2), 'Same content different order should be equal');
  assert.ok(!canonicalEquals(obj1, obj3), 'Different values should not be equal');
});

test('canonicalClone produces independent copy', () => {
  const original = { nested: { value: 42n } };
  const cloned = canonicalClone(original);
  
  // Modify original
  original.nested.value = 100n;
  
  assert.strictEqual(cloned.nested.value, 42n, 'Cloned value should be independent');
});

test('immutableSnapshot produces frozen object', () => {
  const original = { nested: { value: 42 } };
  const frozen = immutableSnapshot(original);
  
  assert.ok(Object.isFrozen(frozen), 'Top level should be frozen');
  assert.ok(Object.isFrozen(frozen.nested), 'Nested objects should be frozen');
  
  assert.throws(() => {
    frozen.nested.value = 100;
  }, /Cannot assign to read only property|object is not extensible/, 'Should throw on modification attempt');
});

test('canonicalStringify handles arrays correctly', () => {
  const arr = [{ z: 1, a: 2 }, { y: 3, b: 4 }];
  const str = canonicalStringify(arr);
  
  assert.strictEqual(str, '[{"a":2,"z":1},{"b":4,"y":3}]', 'Array items should have sorted keys');
});

test('canonicalStringify omits undefined values', () => {
  const obj = { a: 1, b: undefined, c: 3 };
  const str = canonicalStringify(obj);
  
  assert.ok(!str.includes('b'), 'undefined values should be omitted');
  assert.strictEqual(str, '{"a":1,"c":3}');
});

test('canonicalStringify preserves null', () => {
  const obj = { a: 1, b: null, c: 3 };
  const str = canonicalStringify(obj);
  
  assert.ok(str.includes('"b":null'), 'null values should be preserved');
});

test('determinism: same object always produces same hash', () => {
  const obj = { 
    ts_event: 1234567890n, 
    seq: 1n, 
    nested: { x: 1, y: 2 },
    arr: [1, 2, 3]
  };
  
  const results = [];
  for (let i = 0; i < 100; i++) {
    results.push(canonicalStringify(obj));
  }
  
  const allSame = results.every(r => r === results[0]);
  assert.ok(allSame, 'All serializations should be identical');
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== StateSerializer Test Suite ===\n');
  
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
