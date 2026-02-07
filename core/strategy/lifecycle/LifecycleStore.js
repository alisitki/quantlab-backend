/**
 * Lifecycle State Persistence
 *
 * JSON file-based storage with atomic write pattern.
 * Stores strategy records and performance data.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { LIFECYCLE_CONFIG } from './config.js';

/**
 * Manages persistence of lifecycle state to disk
 */
export class LifecycleStore {
  constructor(storeDir = null, filename = null) {
    this.storeDir = storeDir || LIFECYCLE_CONFIG.persistence.storeDir;
    this.filename = filename || LIFECYCLE_CONFIG.persistence.filename;
    this.filePath = path.join(this.storeDir, this.filename);
  }

  /**
   * Get the full file path
   * @returns {string}
   */
  getPath() {
    return this.filePath;
  }

  /**
   * Load lifecycle state from disk
   * @returns {Promise<LifecycleState>}
   * @throws {Error} If file cannot be read or parsed
   */
  async load() {
    try {
      const data = await fs.readFile(this.filePath, 'utf8');
      const state = JSON.parse(data);

      // Validate version
      if (state.version !== 1) {
        throw new Error(`Unsupported lifecycle state version: ${state.version}`);
      }

      return state;
    } catch (err) {
      if (err.code === 'ENOENT') {
        // File doesn't exist, return empty state
        return this.#createEmptyState();
      }
      throw err;
    }
  }

  /**
   * Save lifecycle state to disk (atomic write)
   * @param {LifecycleState} state
   * @returns {Promise<void>}
   */
  async save(state) {
    // Ensure directory exists
    await fs.mkdir(this.storeDir, { recursive: true });

    // Update timestamp
    state.lastUpdated = new Date().toISOString();

    // Atomic write: write to temp file, fsync, then rename
    const tmpPath = `${this.filePath}.tmp`;
    const serialized = JSON.stringify(state, null, 2);

    // Write to temp file
    const fileHandle = await fs.open(tmpPath, 'w');
    try {
      await fileHandle.write(serialized);
      await fileHandle.sync(); // fsync
    } finally {
      await fileHandle.close();
    }

    // Atomic rename
    await fs.rename(tmpPath, this.filePath);
  }

  /**
   * Create an empty lifecycle state
   * @private
   * @returns {LifecycleState}
   */
  #createEmptyState() {
    return {
      version: 1,
      lastUpdated: new Date().toISOString(),
      strategies: {},
      performanceData: {}
    };
  }
}

/**
 * @typedef {Object} LifecycleState
 * @property {number} version - State format version
 * @property {string} lastUpdated - ISO timestamp
 * @property {Object.<string, StrategyRecord>} strategies - Map of strategyId to record
 * @property {Object} performanceData - Serialized PerformanceTracker data
 */

/**
 * @typedef {Object} StrategyRecord
 * @property {string} strategyId
 * @property {string} edgeId
 * @property {string} templateType
 * @property {string} currentStage
 * @property {StageHistoryEntry[]} stageHistory
 * @property {BacktestSummary} backtestSummary
 * @property {number} validationScore
 * @property {string} deployedAt - ISO timestamp
 * @property {Object} promotionGuards
 * @property {boolean} pendingApproval
 */

/**
 * @typedef {Object} StageHistoryEntry
 * @property {string} stage
 * @property {string} enteredAt - ISO timestamp
 * @property {string} [exitedAt] - ISO timestamp
 * @property {string} [reason]
 */

/**
 * @typedef {Object} BacktestSummary
 * @property {number} trades
 * @property {number} sharpe
 * @property {number} maxDrawdownPct
 */
