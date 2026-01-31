#!/usr/bin/env node
/**
 * Interface Module Test Suite
 * 
 * PHASE 6: Legacy Adapter — Verification
 * 
 * Tests:
 * 1. Strategy base class
 * 2. StrategyAdapter v1 → v2 wrapping
 * 3. StrategyLoader detection and loading
 */

import assert from 'node:assert';
import { Strategy } from './Strategy.js';
import { StrategyAdapter } from './StrategyAdapter.js';
import { StrategyLoader } from './StrategyLoader.js';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// Mock Helpers
// ============================================================================

function createMockContext() {
  return {
    runId: 'test_run',
    logger: {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: () => {}
    }
  };
}

// Legacy v1 strategy
class LegacyV1Strategy {
  constructor() {
    this.events = [];
  }
  
  async onStart(ctx) {
    this.events.push('start');
  }
  
  async onEvent(event, ctx) {
    this.events.push('event');
  }
  
  async onEnd(ctx) {
    this.events.push('end');
  }
}

// Modern v2 strategy
class ModernV2Strategy extends Strategy {
  #state = { count: 0 };
  
  async onEvent(event, ctx) {
    this.#state.count++;
  }
  
  getState() {
    return { count: this.#state.count };
  }
  
  setState(state) {
    this.#state.count = state.count ?? 0;
  }
}

// ============================================================================
// Strategy Base Class Tests
// ============================================================================

test('Strategy base class has required methods', () => {
  const strategy = new Strategy({ param: 1 });
  
  assert.ok(strategy.id);
  assert.ok(strategy.version);
  assert.ok(typeof strategy.onInit === 'function');
  assert.ok(typeof strategy.onEvent === 'function');
  assert.ok(typeof strategy.onFinalize === 'function');
  assert.ok(typeof strategy.getState === 'function');
  assert.ok(typeof strategy.setState === 'function');
});

test('Strategy base class freezes config', () => {
  const strategy = new Strategy({ param: 1 });
  
  assert.ok(Object.isFrozen(strategy.config));
});

test('Strategy base onEvent throws', async () => {
  const strategy = new Strategy();
  
  await assert.rejects(
    () => strategy.onEvent({}, createMockContext()),
    /must be implemented/
  );
});

test('Strategy base getState returns empty object', () => {
  const strategy = new Strategy();
  const state = strategy.getState();
  
  assert.deepStrictEqual(state, {});
});

// ============================================================================
// StrategyAdapter Tests
// ============================================================================

test('StrategyAdapter wraps v1 strategy', async () => {
  const v1 = new LegacyV1Strategy();
  const adapted = new StrategyAdapter(v1);
  const ctx = createMockContext();
  
  await adapted.onInit(ctx);
  await adapted.onEvent({}, ctx);
  await adapted.onFinalize(ctx);
  
  assert.deepStrictEqual(v1.events, ['start', 'event', 'end']);
});

test('StrategyAdapter provides id and version', () => {
  const v1 = new LegacyV1Strategy();
  const adapted = new StrategyAdapter(v1);
  
  assert.ok(adapted.id);
  assert.ok(adapted.version);
});

test('StrategyAdapter override id and version', () => {
  const v1 = new LegacyV1Strategy();
  const adapted = new StrategyAdapter(v1, { id: 'custom-id', version: '2.0.0' });
  
  assert.strictEqual(adapted.id, 'custom-id');
  assert.strictEqual(adapted.version, '2.0.0');
});

test('StrategyAdapter getState returns adapted marker', () => {
  const v1 = new LegacyV1Strategy();
  const adapted = new StrategyAdapter(v1);
  
  const state = adapted.getState();
  
  assert.ok(state.__adapted);
  assert.ok(state.__warning);
});

test('StrategyAdapter exposes wrapped strategy', () => {
  const v1 = new LegacyV1Strategy();
  const adapted = new StrategyAdapter(v1);
  
  assert.strictEqual(adapted.getWrapped(), v1);
});

test('StrategyAdapter.needsAdapter detects v1', () => {
  const v1 = new LegacyV1Strategy();
  const v2 = new ModernV2Strategy();
  
  assert.ok(StrategyAdapter.needsAdapter(v1));
  assert.ok(!StrategyAdapter.needsAdapter(v2));
});

test('StrategyAdapter.adapt conditionally wraps', () => {
  const v1 = new LegacyV1Strategy();
  const v2 = new ModernV2Strategy();
  
  const adapted1 = StrategyAdapter.adapt(v1);
  const adapted2 = StrategyAdapter.adapt(v2);
  
  assert.ok(adapted1 instanceof StrategyAdapter);
  assert.ok(adapted2 instanceof ModernV2Strategy);
});

test('StrategyAdapter delegates to wrapped getState if available', () => {
  const v1WithState = {
    async onEvent(event, ctx) {},
    getState() { return { custom: 'state' }; }
  };
  
  const adapted = new StrategyAdapter(v1WithState);
  const state = adapted.getState();
  
  assert.deepStrictEqual(state, { custom: 'state' });
});

// ============================================================================
// StrategyLoader Tests
// ============================================================================

test('StrategyLoader.loadFromClass works', () => {
  const strategy = StrategyLoader.loadFromClass(LegacyV1Strategy);
  
  assert.ok(strategy instanceof StrategyAdapter);
  assert.ok(typeof strategy.onEvent === 'function');
});

test('StrategyLoader.loadFromClass with v2 strategy', () => {
  const strategy = StrategyLoader.loadFromClass(ModernV2Strategy, { config: { x: 1 } });
  
  assert.ok(strategy instanceof ModernV2Strategy);
  assert.ok(typeof strategy.getState === 'function');
});

test('StrategyLoader.wrap wraps v1', () => {
  const v1 = new LegacyV1Strategy();
  const wrapped = StrategyLoader.wrap(v1);
  
  assert.ok(wrapped instanceof StrategyAdapter);
});

test('StrategyLoader.wrap returns v2 as-is', () => {
  const v2 = new ModernV2Strategy();
  const wrapped = StrategyLoader.wrap(v2);
  
  assert.strictEqual(wrapped, v2);
});

test('StrategyLoader.detectVersion identifies v1', () => {
  const v1 = new LegacyV1Strategy();
  
  assert.strictEqual(StrategyLoader.detectVersion(v1), 'v1');
});

test('StrategyLoader.detectVersion identifies v2', () => {
  const v2 = new ModernV2Strategy();
  
  assert.strictEqual(StrategyLoader.detectVersion(v2), 'v2');
});

test('StrategyLoader.detectVersion returns unknown for invalid', () => {
  assert.strictEqual(StrategyLoader.detectVersion({}), 'unknown');
  assert.strictEqual(StrategyLoader.detectVersion({ foo: 'bar' }), 'unknown');
});

test('StrategyLoader.validate checks required methods', () => {
  const valid = StrategyLoader.validate(new LegacyV1Strategy());
  const invalid = StrategyLoader.validate({});
  const nullStrategy = StrategyLoader.validate(null);
  
  assert.ok(valid.valid);
  assert.ok(!invalid.valid);
  assert.ok(invalid.errors.includes('Missing required method: onEvent'));
  assert.ok(!nullStrategy.valid);
});

test('StrategyLoader throws on invalid export', () => {
  assert.throws(
    () => StrategyLoader.wrap({}),
    /must have onEvent/
  );
});

// ============================================================================
// Integration Test
// ============================================================================

test('Full v1 to v2 adaptation lifecycle', async () => {
  // Create a v1 strategy
  const v1 = new LegacyV1Strategy();
  
  // Detect and adapt
  const version = StrategyLoader.detectVersion(v1);
  assert.strictEqual(version, 'v1');
  
  const adapted = StrategyAdapter.adapt(v1);
  
  // Run through lifecycle
  const ctx = createMockContext();
  
  await adapted.onInit(ctx);
  await adapted.onEvent({ ts_event: 1n }, ctx);
  await adapted.onEvent({ ts_event: 2n }, ctx);
  await adapted.onFinalize(ctx);
  
  // Verify lifecycle was called
  assert.strictEqual(v1.events.length, 4);
  assert.strictEqual(v1.events[0], 'start');
  assert.strictEqual(v1.events[3], 'end');
  
  // getState returns adapted marker
  const state = adapted.getState();
  assert.ok(state.__adapted);
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== Interface Module Test Suite ===\n');
  
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
