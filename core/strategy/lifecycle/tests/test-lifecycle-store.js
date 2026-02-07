/**
 * Tests for LifecycleStore
 *
 * Verifies JSON persistence with atomic write pattern.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { LifecycleStore } from '../LifecycleStore.js';

const TEST_DIR = 'data/lifecycle-test';
const TEST_FILE = 'test-state.json';

describe('LifecycleStore', () => {
  let store;

  beforeEach(async () => {
    // Create test store
    store = new LifecycleStore(TEST_DIR, TEST_FILE);

    // Clean up before each test
    try {
      await fs.rm(TEST_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore errors
    }
  });

  afterEach(async () => {
    // Clean up after each test
    try {
      await fs.rm(TEST_DIR, { recursive: true, force: true });
    } catch (err) {
      // Ignore errors
    }
  });

  it('should save and load state correctly', async () => {
    const state = {
      version: 1,
      lastUpdated: '2026-01-01T00:00:00Z',
      strategies: {
        'strat-1': {
          strategyId: 'strat-1',
          edgeId: 'edge-1',
          templateType: 'momentum',
          currentStage: 'PAPER',
          stageHistory: [
            { stage: 'CANDIDATE', enteredAt: '2026-01-01T00:00:00Z', exitedAt: '2026-01-02T00:00:00Z' },
            { stage: 'PAPER', enteredAt: '2026-01-02T00:00:00Z' }
          ],
          backtestSummary: { trades: 100, sharpe: 1.5, maxDrawdownPct: 5 },
          validationScore: 0.85,
          deployedAt: '2026-01-01T00:00:00Z',
          promotionGuards: {},
          pendingApproval: false
        }
      },
      performanceData: {
        'strat-1': [
          { runId: 'run-1', completedAt: '2026-01-01T12:00:00Z', trades: 10, pnl: 100, returnPct: 2.5 }
        ]
      }
    };

    // Save
    await store.save(state);

    // Load
    const loaded = await store.load();

    // Verify
    assert.equal(loaded.version, 1);
    assert.ok(loaded.lastUpdated); // Should be updated to current time
    assert.equal(loaded.strategies['strat-1'].strategyId, 'strat-1');
    assert.equal(loaded.strategies['strat-1'].currentStage, 'PAPER');
    assert.equal(loaded.strategies['strat-1'].stageHistory.length, 2);
    assert.equal(loaded.performanceData['strat-1'].length, 1);
  });

  it('should return empty state when file does not exist', async () => {
    const loaded = await store.load();

    assert.equal(loaded.version, 1);
    assert.ok(loaded.lastUpdated);
    assert.deepEqual(loaded.strategies, {});
    assert.deepEqual(loaded.performanceData, {});
  });

  it('should create directory if it does not exist', async () => {
    const state = {
      version: 1,
      lastUpdated: '2026-01-01T00:00:00Z',
      strategies: {},
      performanceData: {}
    };

    // Save (directory doesn't exist yet)
    await store.save(state);

    // Verify directory was created
    const stats = await fs.stat(TEST_DIR);
    assert.ok(stats.isDirectory());

    // Verify file exists
    const fileStats = await fs.stat(store.getPath());
    assert.ok(fileStats.isFile());
  });

  it('should perform atomic write (no temp file left behind)', async () => {
    const state = {
      version: 1,
      lastUpdated: '2026-01-01T00:00:00Z',
      strategies: {},
      performanceData: {}
    };

    // Save
    await store.save(state);

    // Verify temp file doesn't exist
    const tmpPath = `${store.getPath()}.tmp`;
    let tmpExists = false;
    try {
      await fs.access(tmpPath);
      tmpExists = true;
    } catch (err) {
      // Expected - temp file should not exist
    }

    assert.equal(tmpExists, false, 'Temp file should not exist after atomic write');

    // Verify actual file exists
    const fileExists = await fs.access(store.getPath()).then(() => true).catch(() => false);
    assert.ok(fileExists, 'Actual file should exist');
  });

  it('should throw error on invalid version', async () => {
    // Create file with invalid version
    await fs.mkdir(TEST_DIR, { recursive: true });
    const invalidState = { version: 999, strategies: {}, performanceData: {} };
    await fs.writeFile(store.getPath(), JSON.stringify(invalidState));

    // Try to load
    await assert.rejects(
      async () => await store.load(),
      /Unsupported lifecycle state version/
    );
  });
});
