#!/usr/bin/env node
/**
 * Safety Module Test Suite
 * 
 * PHASE 4: Safety & Error Containment — Verification
 * 
 * Tests:
 * 1. OrderingGuard monotonicity checks
 * 2. ErrorContainment policy handling
 */

import assert from 'node:assert';
import { OrderingGuard } from './OrderingGuard.js';
import { ErrorContainment } from './ErrorContainment.js';
import { OrderingMode, ErrorPolicy } from '../interface/types.js';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// OrderingGuard Tests
// ============================================================================

test('OrderingGuard allows valid forward progression', () => {
  const guard = new OrderingGuard({ mode: OrderingMode.STRICT });
  
  const event1 = { ts_event: 100n, seq: 1n };
  const event2 = { ts_event: 100n, seq: 2n };
  const event3 = { ts_event: 101n, seq: 1n };
  
  const result1 = guard.check(null, event1);
  assert.ok(result1.ok);
  
  const result2 = guard.check(event1, event2);
  assert.ok(result2.ok);
  
  const result3 = guard.check(event2, event3);
  assert.ok(result3.ok);
});

test('OrderingGuard throws on backward movement in STRICT mode', () => {
  const guard = new OrderingGuard({ mode: OrderingMode.STRICT });
  
  const event1 = { ts_event: 100n, seq: 5n };
  const event2 = { ts_event: 100n, seq: 3n };
  
  assert.throws(
    () => guard.check(event1, event2),
    /ORDERING_VIOLATION/
  );
});

test('OrderingGuard warns but continues in WARN mode', () => {
  const guard = new OrderingGuard({ mode: OrderingMode.WARN });
  
  const event1 = { ts_event: 100n, seq: 5n };
  const event2 = { ts_event: 100n, seq: 3n };
  
  // Should not throw
  const result = guard.check(event1, event2);
  
  assert.ok(!result.ok);
  assert.ok(guard.hasViolations());
  assert.strictEqual(guard.getStats().violationCount, 1);
});

test('OrderingGuard tracks internal state with validate()', () => {
  const guard = new OrderingGuard({ mode: OrderingMode.STRICT });
  
  guard.validate({ ts_event: 100n, seq: 1n });
  guard.validate({ ts_event: 100n, seq: 2n });
  guard.validate({ ts_event: 100n, seq: 3n });
  
  // This should fail - going backwards
  assert.throws(
    () => guard.validate({ ts_event: 100n, seq: 2n }),
    /ORDERING_VIOLATION/
  );
});

test('OrderingGuard reset clears state', () => {
  const guard = new OrderingGuard({ mode: OrderingMode.WARN });
  
  guard.validate({ ts_event: 100n, seq: 5n });
  guard.validate({ ts_event: 100n, seq: 3n }); // Violation in WARN mode
  
  assert.ok(guard.hasViolations());
  
  guard.reset();
  
  assert.ok(!guard.hasViolations());
  assert.strictEqual(guard.getLastEvent(), null);
});

test('OrderingGuard resetTo sets specific event', () => {
  const guard = new OrderingGuard({ mode: OrderingMode.STRICT });
  
  const checkpoint = { ts_event: 500n, seq: 10n };
  guard.resetTo(checkpoint);
  
  // This should fail - seq 5 < seq 10
  assert.throws(
    () => guard.validate({ ts_event: 500n, seq: 5n }),
    /ORDERING_VIOLATION/
  );
  
  // This should succeed - seq 11 > seq 10
  guard.resetTo(checkpoint);
  const result = guard.validate({ ts_event: 500n, seq: 11n });
  assert.ok(result.ok);
});

test('OrderingGuard tracks violation details', () => {
  const guard = new OrderingGuard({ mode: OrderingMode.WARN });
  
  const event1 = { ts_event: 100n, seq: 5n };
  const event2 = { ts_event: 100n, seq: 3n };
  
  guard.check(event1, event2);
  
  const stats = guard.getStats();
  assert.strictEqual(stats.violations.length, 1);
  assert.strictEqual(stats.violations[0].prevSeq, '5');
  assert.strictEqual(stats.violations[0].currSeq, '3');
});

// ============================================================================
// ErrorContainment Tests
// ============================================================================

test('ErrorContainment FAIL_FAST rethrows errors', async () => {
  const containment = new ErrorContainment({ policy: ErrorPolicy.FAIL_FAST });
  
  await assert.rejects(
    () => containment.wrap(async () => {
      throw new Error('Test error');
    }),
    /Test error/
  );
  
  assert.strictEqual(containment.errorCount, 1);
});

test('ErrorContainment SKIP_AND_LOG returns result', async () => {
  const containment = new ErrorContainment({ policy: ErrorPolicy.SKIP_AND_LOG });
  
  const result = await containment.wrap(async () => {
    throw new Error('Skippable error');
  });
  
  assert.ok(!result.ok);
  assert.ok(result.skipped);
  assert.ok(result.error);
  assert.strictEqual(result.error.message, 'Skippable error');
  assert.strictEqual(containment.skippedCount, 1);
});

test('ErrorContainment QUARANTINE logs and continues', async () => {
  const containment = new ErrorContainment({ policy: ErrorPolicy.QUARANTINE });
  
  const result = await containment.wrap(async () => {
    throw new Error('Quarantine error');
  }, { eventIndex: 42 });
  
  assert.ok(!result.ok);
  assert.ok(result.skipped);
  assert.strictEqual(containment.skippedCount, 1);
});

test('ErrorContainment tracks error log', async () => {
  const containment = new ErrorContainment({ policy: ErrorPolicy.SKIP_AND_LOG });
  
  await containment.wrap(async () => { throw new Error('Error 1'); }, { idx: 1 });
  await containment.wrap(async () => { throw new Error('Error 2'); }, { idx: 2 });
  
  const stats = containment.getStats();
  
  assert.strictEqual(stats.errorCount, 2);
  assert.strictEqual(stats.errorLog.length, 2);
  assert.strictEqual(stats.errorLog[0].message, 'Error 1');
  assert.strictEqual(stats.errorLog[0].context.idx, 1);
});

test('ErrorContainment throws when maxErrors exceeded', async () => {
  const containment = new ErrorContainment({ 
    policy: ErrorPolicy.SKIP_AND_LOG,
    maxErrors: 3
  });
  
  await containment.wrap(async () => { throw new Error('1'); });
  await containment.wrap(async () => { throw new Error('2'); });
  
  // Third error should trigger limit
  await assert.rejects(
    () => containment.wrap(async () => { throw new Error('3'); }),
    /ERROR_LIMIT_EXCEEDED/
  );
});

test('ErrorContainment calls onError callback', async () => {
  const errors = [];
  const containment = new ErrorContainment({ 
    policy: ErrorPolicy.SKIP_AND_LOG,
    onError: (err, ctx) => errors.push({ err, ctx })
  });
  
  await containment.wrap(async () => { throw new Error('Callback test'); }, { foo: 'bar' });
  
  assert.strictEqual(errors.length, 1);
  assert.strictEqual(errors[0].err.message, 'Callback test');
  assert.strictEqual(errors[0].ctx.foo, 'bar');
});

test('ErrorContainment wrapSync handles sync functions', () => {
  const containment = new ErrorContainment({ policy: ErrorPolicy.SKIP_AND_LOG });
  
  const result = containment.wrapSync(() => {
    throw new Error('Sync error');
  });
  
  assert.ok(!result.ok);
  assert.ok(result.skipped);
});

test('ErrorContainment reset clears stats', async () => {
  const containment = new ErrorContainment({ policy: ErrorPolicy.SKIP_AND_LOG });
  
  await containment.wrap(async () => { throw new Error('Error'); });
  
  assert.ok(containment.hasErrors());
  
  containment.reset();
  
  assert.ok(!containment.hasErrors());
  assert.strictEqual(containment.errorCount, 0);
  assert.strictEqual(containment.skippedCount, 0);
});

test('ErrorContainment returns ok result on success', async () => {
  const containment = new ErrorContainment({ policy: ErrorPolicy.FAIL_FAST });
  
  const result = await containment.wrap(async () => {
    // Success - no error
    return 'done';
  });
  
  assert.ok(result.ok);
  assert.ok(!result.skipped);
  assert.strictEqual(result.error, null);
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== Safety Module Test Suite ===\n');
  
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
