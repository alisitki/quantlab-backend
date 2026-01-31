#!/usr/bin/env node
/**
 * CheckpointManager Test Suite
 * 
 * PHASE 2: State & Snapshot — Verification
 * 
 * Tests:
 * 1. Save/load checkpoints
 * 2. Hash verification
 * 3. Checkpoint listing
 * 4. Cleanup functionality
 */

import assert from 'node:assert';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';
import { CheckpointManager } from './CheckpointManager.js';

const TEST_DIR = '/tmp/quantlab-checkpoint-test';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// SETUP / CLEANUP
// ============================================================================

async function setup() {
  // Clean test directory
  try {
    await fs.rm(TEST_DIR, { recursive: true });
  } catch {}
  await fs.mkdir(TEST_DIR, { recursive: true });
}

async function cleanup() {
  try {
    await fs.rm(TEST_DIR, { recursive: true });
  } catch {}
}

// ============================================================================
// SAVE TESTS
// ============================================================================

test('save creates checkpoint file', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  const state = { position: 'LONG', equity: 10500 };
  
  const result = await manager.save(state, 'checkpoint_001', 1000);
  
  assert.ok(result.path, 'Should return path');
  assert.ok(result.hash, 'Should return hash');
  assert.ok(result.hash.length === 64, 'Hash should be SHA256');
  
  // Verify file exists
  const exists = await manager.exists('checkpoint_001');
  assert.ok(exists, 'Checkpoint should exist');
});

test('save with runId creates namespaced path', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR, runId: 'run_abc123' });
  const state = { test: true };
  
  const result = await manager.save(state, 'checkpoint_nstest');
  
  assert.ok(result.path.includes('run_abc123'), 'Path should include runId');
});

// ============================================================================
// LOAD TESTS
// ============================================================================

test('load retrieves saved checkpoint', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  const state = { trades: [1, 2, 3], ts_event: 1234567890n };
  
  await manager.save(state, 'checkpoint_load', 500);
  const loaded = await manager.load('checkpoint_load');
  
  assert.deepStrictEqual(loaded.state.trades, [1, 2, 3], 'State should match');
  assert.strictEqual(loaded.state.ts_event, 1234567890n, 'BigInt should be restored');
  assert.strictEqual(loaded.eventIndex, 500, 'Event index should match');
  assert.strictEqual(loaded.checkpointId, 'checkpoint_load', 'ID should match');
});

test('load verifies hash by default', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  const state = { valid: true };
  
  await manager.save(state, 'checkpoint_hash_test');
  
  // Corrupt the file
  const path = join(TEST_DIR, 'checkpoint_hash_test.json');
  const content = await fs.readFile(path, 'utf8');
  const modified = content.replace('"valid":true', '"valid":false');
  await fs.writeFile(path, modified);
  
  await assert.rejects(
    () => manager.load('checkpoint_hash_test'),
    /CHECKPOINT_CORRUPT.*Hash mismatch/,
    'Should reject corrupted checkpoint'
  );
});

test('load can skip hash verification', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  const state = { skiptest: true };
  
  await manager.save(state, 'checkpoint_skip_verify');
  
  // Corrupt the file
  const path = join(TEST_DIR, 'checkpoint_skip_verify.json');
  const content = await fs.readFile(path, 'utf8');
  const modified = content.replace('"skiptest":true', '"skiptest":false');
  await fs.writeFile(path, modified);
  
  // Should load with verification disabled
  const loaded = await manager.load('checkpoint_skip_verify', { verifyHash: false });
  assert.strictEqual(loaded.state.skiptest, false, 'Should load modified state');
});

test('load throws for non-existent checkpoint', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  
  await assert.rejects(
    () => manager.load('nonexistent'),
    /CHECKPOINT_NOT_FOUND/,
    'Should throw not found error'
  );
});

// ============================================================================
// LIST TESTS
// ============================================================================

test('list returns all checkpoints', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR, runId: 'list_test' });
  
  await manager.save({ a: 1 }, 'cp1');
  await manager.save({ b: 2 }, 'cp2');
  await manager.save({ c: 3 }, 'cp3');
  
  const list = await manager.list();
  
  assert.strictEqual(list.length, 3, 'Should find 3 checkpoints');
  assert.ok(list.includes('cp1'), 'Should include cp1');
  assert.ok(list.includes('cp2'), 'Should include cp2');
  assert.ok(list.includes('cp3'), 'Should include cp3');
});

test('list returns empty array for new run', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR, runId: 'empty_run' });
  
  const list = await manager.list();
  
  assert.deepStrictEqual(list, [], 'Should return empty array');
});

// ============================================================================
// EXISTS/DELETE TESTS
// ============================================================================

test('exists returns correct status', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  
  await manager.save({ test: true }, 'exists_test');
  
  assert.ok(await manager.exists('exists_test'), 'Should exist after save');
  assert.ok(!(await manager.exists('never_saved')), 'Should not exist');
});

test('delete removes checkpoint', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  
  await manager.save({ test: true }, 'delete_test');
  assert.ok(await manager.exists('delete_test'), 'Should exist initially');
  
  const deleted = await manager.delete('delete_test');
  assert.ok(deleted, 'Delete should return true');
  assert.ok(!(await manager.exists('delete_test')), 'Should not exist after delete');
});

test('delete returns false for non-existent', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  
  const deleted = await manager.delete('never_existed');
  assert.ok(!deleted, 'Should return false');
});

// ============================================================================
// GET LATEST TEST
// ============================================================================

test('getLatest returns checkpoint with highest eventIndex', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR, runId: 'latest_test' });
  
  await manager.save({ seq: 1 }, 'cp_early', 100);
  await manager.save({ seq: 2 }, 'cp_middle', 500);
  await manager.save({ seq: 3 }, 'cp_latest', 1000);
  
  const latest = await manager.getLatest();
  
  assert.strictEqual(latest.eventIndex, 1000, 'Should return highest eventIndex');
  assert.strictEqual(latest.state.seq, 3, 'Should have correct state');
});

test('getLatest returns null for empty', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR, runId: 'empty_latest' });
  
  const latest = await manager.getLatest();
  assert.strictEqual(latest, null, 'Should return null');
});

// ============================================================================
// CLEANUP TEST
// ============================================================================

test('cleanup keeps only N latest checkpoints', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR, runId: 'cleanup_test' });
  
  await manager.save({ n: 1 }, 'cp_1', 100);
  await manager.save({ n: 2 }, 'cp_2', 200);
  await manager.save({ n: 3 }, 'cp_3', 300);
  await manager.save({ n: 4 }, 'cp_4', 400);
  await manager.save({ n: 5 }, 'cp_5', 500);
  
  const deleted = await manager.cleanup(2);
  
  assert.strictEqual(deleted, 3, 'Should delete 3 checkpoints');
  
  const remaining = await manager.list();
  assert.strictEqual(remaining.length, 2, 'Should have 2 remaining');
  
  // Should keep the latest two
  assert.ok(remaining.includes('cp_5'), 'Should keep cp_5');
  assert.ok(remaining.includes('cp_4'), 'Should keep cp_4');
});

// ============================================================================
// ROUND-TRIP TEST
// ============================================================================

test('round-trip preserves complex state', async () => {
  const manager = new CheckpointManager({ baseDir: TEST_DIR });
  
  const originalState = {
    position: 'LONG',
    entryPrice: 99876.54,
    ts_last: 1706678400000000000n,
    fills: [
      { id: 'fill_1', side: 'BUY', qty: 0.1 },
      { id: 'fill_2', side: 'SELL', qty: 0.05 }
    ],
    metrics: {
      trades: 42,
      wins: 28,
      losses: 14
    }
  };
  
  await manager.save(originalState, 'roundtrip');
  const loaded = await manager.load('roundtrip');
  
  assert.strictEqual(loaded.state.position, originalState.position);
  assert.strictEqual(loaded.state.entryPrice, originalState.entryPrice);
  assert.strictEqual(loaded.state.ts_last, originalState.ts_last);
  assert.deepStrictEqual(loaded.state.fills, originalState.fills);
  assert.deepStrictEqual(loaded.state.metrics, originalState.metrics);
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== CheckpointManager Test Suite ===\n');
  
  await setup();
  
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
  
  await cleanup();
  
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
