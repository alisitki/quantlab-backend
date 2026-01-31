#!/usr/bin/env node
/**
 * StrategyStateContainer Test Suite
 * 
 * PHASE 2: State & Snapshot — Verification
 * 
 * Tests:
 * 1. State management (get, set, update)
 * 2. Immutable snapshots
 * 3. State restoration
 * 4. Hash verification
 */

import assert from 'node:assert';
import { StrategyStateContainer, createStateContainerFactory } from './StrategyStateContainer.js';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// BASIC STATE MANAGEMENT
// ============================================================================

test('constructor initializes with empty state by default', () => {
  const container = new StrategyStateContainer();
  const state = container.get();
  
  assert.deepStrictEqual(state, {}, 'Default state should be empty object');
});

test('constructor accepts initial state', () => {
  const initial = { position: 'FLAT', cooldown: 0 };
  const container = new StrategyStateContainer(initial);
  
  assert.deepStrictEqual(container.get(), initial, 'Should have initial state');
});

test('set replaces entire state', () => {
  const container = new StrategyStateContainer({ a: 1 });
  container.set({ b: 2 });
  
  assert.deepStrictEqual(container.get(), { b: 2 }, 'State should be replaced');
});

test('update merges into state', () => {
  const container = new StrategyStateContainer({ a: 1, b: 2 });
  container.update({ b: 3, c: 4 });
  
  assert.deepStrictEqual(container.get(), { a: 1, b: 3, c: 4 }, 'State should be merged');
});

test('setValue sets specific key', () => {
  const container = new StrategyStateContainer({ a: 1 });
  container.setValue('b', 2);
  
  assert.strictEqual(container.getValue('b'), 2, 'Value should be set');
  assert.strictEqual(container.getValue('a'), 1, 'Other values should remain');
});

test('increment increments numeric value', () => {
  const container = new StrategyStateContainer({ count: 5 });
  container.increment('count');
  container.increment('count', 2);
  
  assert.strictEqual(container.getValue('count'), 8, 'Count should be 5 + 1 + 2');
});

test('increment initializes missing key to 0', () => {
  const container = new StrategyStateContainer({});
  container.increment('count');
  
  assert.strictEqual(container.getValue('count'), 1, 'Should start from 0');
});

// ============================================================================
// IMMUTABILITY
// ============================================================================

test('get returns a clone, not the original', () => {
  const container = new StrategyStateContainer({ nested: { value: 1 } });
  const state = container.get();
  
  // Modify the returned state
  state.nested.value = 999;
  
  // Original should be unchanged
  assert.strictEqual(container.getValue('nested').value, 1, 'Original should not be mutated');
});

test('snapshot returns frozen object', () => {
  const container = new StrategyStateContainer({ a: 1 });
  const snapshot = container.snapshot();
  
  assert.ok(Object.isFrozen(snapshot), 'Snapshot should be frozen');
  assert.ok(Object.isFrozen(snapshot.state), 'Snapshot state should be frozen');
});

test('getState returns immutable state', () => {
  const container = new StrategyStateContainer({ nested: { value: 1 } });
  const state = container.getState();
  
  assert.ok(Object.isFrozen(state), 'State should be frozen');
  assert.ok(Object.isFrozen(state.nested), 'Nested objects should be frozen');
});

// ============================================================================
// SNAPSHOT & RESTORE
// ============================================================================

test('snapshot includes metadata', () => {
  const container = new StrategyStateContainer({ a: 1 });
  container.update({ b: 2 }); // Increment version
  
  const snapshot = container.snapshot();
  
  assert.ok(snapshot.state, 'Should have state');
  assert.ok(snapshot.hash, 'Should have hash');
  assert.ok(snapshot.timestamp, 'Should have timestamp');
  assert.strictEqual(snapshot.version, 1, 'Should have correct version');
});

test('restore from snapshot', () => {
  const container1 = new StrategyStateContainer({ a: 1 });
  container1.update({ b: 2 });
  const snapshot = container1.snapshot();
  
  const container2 = new StrategyStateContainer();
  container2.restore(snapshot);
  
  assert.deepStrictEqual(container2.get(), { a: 1, b: 2 }, 'State should be restored');
});

test('restore verifies hash', () => {
  const snapshot = {
    state: { a: 1 },
    hash: 'invalid-hash',
    version: 1
  };
  
  const container = new StrategyStateContainer();
  
  assert.throws(
    () => container.restore(snapshot),
    /RESTORE_ERROR.*Hash mismatch/,
    'Should throw on hash mismatch'
  );
});

test('restore throws on invalid snapshot', () => {
  const container = new StrategyStateContainer();
  
  assert.throws(() => container.restore(null), /Invalid snapshot/);
  assert.throws(() => container.restore({}), /Invalid snapshot/);
});

test('setState restores without metadata', () => {
  const container = new StrategyStateContainer();
  container.setState({ x: 1, y: 2 });
  
  assert.deepStrictEqual(container.get(), { x: 1, y: 2 }, 'State should be set');
});

// ============================================================================
// RESET
// ============================================================================

test('reset returns to initial state', () => {
  const initial = { position: 'FLAT' };
  const container = new StrategyStateContainer(initial);
  
  container.update({ position: 'LONG', trades: 5 });
  container.reset();
  
  assert.deepStrictEqual(container.get(), initial, 'Should return to initial state');
  assert.strictEqual(container.getVersion(), 0, 'Version should reset');
});

// ============================================================================
// VERSIONING
// ============================================================================

test('version increments on state changes', () => {
  const container = new StrategyStateContainer();
  
  assert.strictEqual(container.getVersion(), 0, 'Initial version should be 0');
  
  container.set({ a: 1 });
  assert.strictEqual(container.getVersion(), 1);
  
  container.update({ b: 2 });
  assert.strictEqual(container.getVersion(), 2);
  
  container.setValue('c', 3);
  assert.strictEqual(container.getVersion(), 3);
  
  container.increment('count');
  assert.strictEqual(container.getVersion(), 4);
});

// ============================================================================
// HASH & EQUALITY
// ============================================================================

test('computeHash is consistent', () => {
  const container = new StrategyStateContainer({ a: 1, b: 2 });
  
  const hash1 = container.computeHash();
  const hash2 = container.computeHash();
  
  assert.strictEqual(hash1, hash2, 'Hash should be consistent');
  assert.strictEqual(hash1.length, 64, 'Should be SHA256 hex');
});

test('equals compares against snapshot', () => {
  const container1 = new StrategyStateContainer({ a: 1 });
  const snapshot = container1.snapshot();
  
  const container2 = new StrategyStateContainer({ a: 1 });
  
  assert.ok(container2.equals(snapshot), 'Equal states should match');
  
  container2.update({ b: 2 });
  assert.ok(!container2.equals(snapshot), 'Modified state should not match');
});

// ============================================================================
// FACTORY
// ============================================================================

test('createStateContainerFactory creates typed containers', () => {
  const createContainer = createStateContainerFactory({
    position: 'FLAT',
    tradeCount: 0,
    signals: []
  });
  
  const container1 = createContainer();
  const container2 = createContainer();
  
  container1.setValue('tradeCount', 5);
  
  assert.strictEqual(container1.getValue('tradeCount'), 5);
  assert.strictEqual(container2.getValue('tradeCount'), 0, 'Containers should be independent');
});

// ============================================================================
// BIGINT SUPPORT
// ============================================================================

test('handles BigInt values', () => {
  const container = new StrategyStateContainer({ 
    ts_event: 1234567890123456789n,
    seq: 42n 
  });
  
  const snapshot = container.snapshot();
  
  const container2 = new StrategyStateContainer();
  container2.restore(snapshot);
  
  assert.strictEqual(container2.getValue('ts_event'), 1234567890123456789n, 'BigInt should be preserved');
  assert.strictEqual(container2.getValue('seq'), 42n);
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== StrategyStateContainer Test Suite ===\n');
  
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
