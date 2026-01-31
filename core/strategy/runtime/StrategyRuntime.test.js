#!/usr/bin/env node
/**
 * StrategyRuntime Test Suite
 * 
 * PHASE 3: Lifecycle & Runtime — Verification
 * 
 * Tests:
 * 1. RuntimeConfig validation
 * 2. RuntimeLifecycle state machine
 * 3. RuntimeState snapshot/restore
 * 4. StrategyRuntime integration
 */

import assert from 'node:assert';
import { RuntimeConfig } from './RuntimeConfig.js';
import { RuntimeContext } from './RuntimeContext.js';
import { RuntimeLifecycle } from './RuntimeLifecycle.js';
import { RuntimeState } from './RuntimeState.js';
import { StrategyRuntime } from './StrategyRuntime.js';
import { RunLifecycleStatus, ErrorPolicy, OrderingMode } from '../interface/types.js';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

// ============================================================================
// MOCK HELPERS
// ============================================================================

function createMockStrategy() {
  const events = [];
  return {
    id: 'mock-strategy',
    version: '1.0.0',
    events,
    async onInit(ctx) {
      events.push({ type: 'init', runId: ctx.runId });
    },
    async onEvent(event, ctx) {
      events.push({ type: 'event', ts: event.ts_event });
    },
    async onFinalize(ctx) {
      events.push({ type: 'finalize' });
    },
    getState() {
      return { eventCount: events.filter(e => e.type === 'event').length };
    },
    setState(state) {
      // Restore not implemented for mock
    }
  };
}

async function* createMockEventStream(count = 10) {
  for (let i = 0; i < count; i++) {
    yield {
      ts_event: BigInt(1000000000 + i * 1000000),
      seq: BigInt(i + 1),
      bid_price: 100 + i * 0.01,
      ask_price: 100.01 + i * 0.01,
      cursor: `cursor_${i}`
    };
  }
}

// ============================================================================
// RuntimeConfig Tests
// ============================================================================

test('RuntimeConfig validates required fields', () => {
  assert.throws(
    () => new RuntimeConfig({}),
    /dataset is required/
  );
  
  assert.throws(
    () => new RuntimeConfig({ dataset: {} }),
    /dataset.parquet is required/
  );
  
  assert.throws(
    () => new RuntimeConfig({ dataset: { parquet: 'x' } }),
    /dataset.meta is required/
  );
  
  assert.throws(
    () => new RuntimeConfig({ 
      dataset: { parquet: 'x', meta: 'y' } 
    }),
    /strategy is required/
  );
});

test('RuntimeConfig applies defaults', () => {
  const config = new RuntimeConfig({
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy: createMockStrategy()
  });
  
  assert.strictEqual(config.batchSize, 10000);
  assert.strictEqual(config.errorPolicy, ErrorPolicy.FAIL_FAST);
  assert.strictEqual(config.orderingMode, OrderingMode.STRICT);
  assert.strictEqual(config.enableMetrics, true);
  assert.strictEqual(config.enableCheckpoints, false);
});

test('RuntimeConfig computes deterministic hash', () => {
  const opts = {
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy: createMockStrategy(),
    strategyConfig: { param: 42 }
  };
  
  const config1 = new RuntimeConfig(opts);
  const config2 = new RuntimeConfig(opts);
  
  assert.strictEqual(config1.configHash, config2.configHash);
  assert.strictEqual(config1.configHash.length, 64);
});

test('RuntimeConfig is immutable', () => {
  const config = new RuntimeConfig({
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy: createMockStrategy()
  });
  
  assert.ok(Object.isFrozen(config));
  assert.ok(Object.isFrozen(config.dataset));
  assert.ok(Object.isFrozen(config.strategyConfig));
});

// ============================================================================
// RuntimeLifecycle Tests
// ============================================================================

test('RuntimeLifecycle starts in CREATED state', () => {
  const lifecycle = new RuntimeLifecycle();
  
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.CREATED);
  assert.ok(!lifecycle.isTerminal);
  assert.ok(!lifecycle.isRunning);
});

test('RuntimeLifecycle allows valid transitions', () => {
  const lifecycle = new RuntimeLifecycle();
  
  lifecycle.initialize();
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.INITIALIZING);
  
  lifecycle.ready();
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.READY);
  
  lifecycle.start();
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.RUNNING);
  assert.ok(lifecycle.isRunning);
  
  lifecycle.pause();
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.PAUSED);
  assert.ok(lifecycle.isPaused);
  
  lifecycle.resume();
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.RUNNING);
  
  lifecycle.finalize();
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.FINALIZING);
  
  lifecycle.complete();
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.DONE);
  assert.ok(lifecycle.isTerminal);
});

test('RuntimeLifecycle rejects invalid transitions', () => {
  const lifecycle = new RuntimeLifecycle();
  
  assert.throws(
    () => lifecycle.start(),
    /Invalid transition from CREATED to RUNNING/
  );
  
  lifecycle.initialize();
  
  assert.throws(
    () => lifecycle.start(),
    /Invalid transition from INITIALIZING to RUNNING/
  );
});

test('RuntimeLifecycle tracks timestamps', () => {
  const lifecycle = new RuntimeLifecycle();
  
  assert.ok(lifecycle.createdAt > 0);
  assert.strictEqual(lifecycle.startedAt, null);
  assert.strictEqual(lifecycle.endedAt, null);
  
  lifecycle.initialize();
  lifecycle.ready();
  lifecycle.start();
  
  assert.ok(lifecycle.startedAt > 0);
  
  lifecycle.finalize();
  lifecycle.complete();
  
  assert.ok(lifecycle.endedAt > 0);
  assert.ok(lifecycle.durationMs >= 0);
});

test('RuntimeLifecycle emits transition events', () => {
  const lifecycle = new RuntimeLifecycle();
  const transitions = [];
  
  lifecycle.on('transition', (e) => transitions.push(e));
  
  lifecycle.initialize();
  lifecycle.ready();
  
  assert.strictEqual(transitions.length, 2);
  assert.strictEqual(transitions[0].from, RunLifecycleStatus.CREATED);
  assert.strictEqual(transitions[0].to, RunLifecycleStatus.INITIALIZING);
});

test('RuntimeLifecycle tracks error on fail', () => {
  const lifecycle = new RuntimeLifecycle();
  
  lifecycle.initialize();
  
  const error = new Error('Test error');
  lifecycle.fail(error);
  
  assert.strictEqual(lifecycle.status, RunLifecycleStatus.FAILED);
  assert.strictEqual(lifecycle.lastError, error);
});

// ============================================================================
// RuntimeState Tests
// ============================================================================

test('RuntimeState initializes with runId', () => {
  const state = new RuntimeState({ runId: 'run_test123' });
  
  assert.strictEqual(state.runId, 'run_test123');
  assert.strictEqual(state.eventCount, 0);
  assert.deepStrictEqual(state.fills, []);
});

test('RuntimeState updates cursor', () => {
  const state = new RuntimeState({ runId: 'run_test' });
  
  state.updateCursor({
    ts_event: 1234567890n,
    seq: 42n,
    encoded: 'cursor_abc'
  });
  
  const cursor = state.cursor;
  assert.strictEqual(cursor.ts_event, 1234567890n);
  assert.strictEqual(cursor.seq, 42n);
  assert.strictEqual(cursor.encoded, 'cursor_abc');
});

test('RuntimeState computes hashes', () => {
  const state = new RuntimeState({ runId: 'run_test' });
  
  state.updateStrategyState({ position: 'LONG' });
  
  const stateHash = state.computeStateHash();
  const fillsHash = state.computeFillsHash();
  
  assert.strictEqual(stateHash.length, 64);
  assert.strictEqual(fillsHash.length, 64);
});

test('RuntimeState snapshot is immutable', () => {
  const state = new RuntimeState({ runId: 'run_test' });
  state.updateStrategyState({ value: 42 });
  
  const snapshot = state.snapshot();
  
  assert.ok(Object.isFrozen(snapshot));
  assert.ok(snapshot.stateHash);
  assert.ok(snapshot.fillsHash);
  assert.ok(snapshot.timestamp);
});

test('RuntimeState restore works', () => {
  const state1 = new RuntimeState({ runId: 'run_test' });
  state1.updateCursor({ ts_event: 100n, seq: 1n });
  state1.updateStrategyState({ count: 5 });
  state1.incrementEventCount(10);
  
  const snapshot = state1.snapshot();
  
  const state2 = new RuntimeState({ runId: 'run_test' });
  state2.restore(snapshot);
  
  assert.strictEqual(state2.eventCount, 10);
  assert.deepStrictEqual(state2.strategyState, { count: 5 });
});

test('RuntimeState compare works', () => {
  const state1 = new RuntimeState({ runId: 'run_test' });
  state1.updateStrategyState({ a: 1 });
  
  const state2 = new RuntimeState({ runId: 'run_test' });
  state2.updateStrategyState({ a: 1 });
  
  const result = state1.compare(state2);
  assert.ok(result.match);
  
  state2.updateStrategyState({ a: 2 });
  const result2 = state1.compare(state2);
  assert.ok(!result2.match);
});

// ============================================================================
// RuntimeContext Tests
// ============================================================================

test('RuntimeContext provides logger', () => {
  const ctx = new RuntimeContext({
    runId: 'run_test',
    dataset: { parquet: 'x', meta: 'y' },
    config: new RuntimeConfig({
      dataset: { parquet: 'x', meta: 'y' },
      strategy: createMockStrategy()
    })
  });
  
  assert.ok(ctx.logger);
  assert.ok(ctx.logger.info);
  assert.ok(ctx.logger.warn);
  assert.ok(ctx.logger.error);
  assert.ok(ctx.logger.debug);
});

test('RuntimeContext tracks cursor', () => {
  const ctx = new RuntimeContext({
    runId: 'run_test',
    dataset: { parquet: 'x', meta: 'y' },
    config: new RuntimeConfig({
      dataset: { parquet: 'x', meta: 'y' },
      strategy: createMockStrategy()
    })
  });
  
  ctx.updateCursor({ ts_event: 100n, seq: 1n, encoded: 'abc' });
  
  assert.strictEqual(ctx.cursor.ts_event, 100n);
  assert.strictEqual(ctx.cursor.encoded, 'abc');
});

test('RuntimeContext throws without execution engine', () => {
  const ctx = new RuntimeContext({
    runId: 'run_test',
    dataset: { parquet: 'x', meta: 'y' },
    config: new RuntimeConfig({
      dataset: { parquet: 'x', meta: 'y' },
      strategy: createMockStrategy()
    })
  });
  
  assert.ok(!ctx.canPlaceOrders());
  assert.throws(
    () => ctx.placeOrder({ symbol: 'BTC', side: 'BUY', qty: 1 }),
    /No execution engine attached/
  );
});

// ============================================================================
// StrategyRuntime Tests
// ============================================================================

test('StrategyRuntime generates deterministic runId', () => {
  const opts = {
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy: createMockStrategy()
  };
  
  const runtime1 = new StrategyRuntime(opts);
  const runtime2 = new StrategyRuntime(opts);
  
  assert.strictEqual(runtime1.runId, runtime2.runId);
  assert.ok(runtime1.runId.startsWith('run_'));
});

test('StrategyRuntime initializes and becomes READY', async () => {
  const strategy = createMockStrategy();
  const runtime = new StrategyRuntime({
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy
  });
  
  await runtime.init();
  
  assert.strictEqual(runtime.status, RunLifecycleStatus.READY);
  assert.strictEqual(strategy.events[0].type, 'init');
});

test('StrategyRuntime processes event stream', async () => {
  const strategy = createMockStrategy();
  const runtime = new StrategyRuntime({
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy
  });
  
  await runtime.init();
  
  const eventStream = createMockEventStream(5);
  const manifest = await runtime.processStream(eventStream);
  
  assert.strictEqual(runtime.status, RunLifecycleStatus.DONE);
  assert.strictEqual(manifest.output.event_count, 5);
  assert.ok(manifest.output.state_hash);
  assert.strictEqual(manifest.ended_reason, 'finished');
});

test('StrategyRuntime manifest is deterministic', async () => {
  async function runOnce() {
    const strategy = createMockStrategy();
    const runtime = new StrategyRuntime({
      dataset: { parquet: 'data.parquet', meta: 'meta.json' },
      strategy
    });
    
    await runtime.init();
    return await runtime.processStream(createMockEventStream(10));
  }
  
  const manifest1 = await runOnce();
  const manifest2 = await runOnce();
  
  assert.strictEqual(manifest1.run_id, manifest2.run_id);
  assert.strictEqual(manifest1.output.state_hash, manifest2.output.state_hash);
  assert.strictEqual(manifest1.output.event_count, manifest2.output.event_count);
});

test('StrategyRuntime emits lifecycle events', async () => {
  const events = [];
  const strategy = createMockStrategy();
  const runtime = new StrategyRuntime({
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy
  });
  
  runtime.on('ready', () => events.push('ready'));
  runtime.on('start', () => events.push('start'));
  runtime.on('complete', () => events.push('complete'));
  
  await runtime.init();
  await runtime.processStream(createMockEventStream(3));
  
  assert.deepStrictEqual(events, ['ready', 'start', 'complete']);
});

test('StrategyRuntime tracks cursor in state', async () => {
  const strategy = createMockStrategy();
  const runtime = new StrategyRuntime({
    dataset: { parquet: 'data.parquet', meta: 'meta.json' },
    strategy
  });
  
  await runtime.init();
  await runtime.processStream(createMockEventStream(5));
  
  const snapshot = runtime.getSnapshot();
  
  assert.ok(snapshot.cursor.ts_event);
  assert.ok(snapshot.cursor.seq);
  assert.ok(snapshot.cursor.encoded);
});

// ============================================================================
// RUN TESTS
// ============================================================================

async function run() {
  console.log('=== StrategyRuntime Test Suite ===\n');
  
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
      if (process.env.DEBUG) {
        console.error(err.stack);
      }
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
