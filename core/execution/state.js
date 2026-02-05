/**
 * QuantLab Execution Engine — State Container
 * Holds execution state: positions, fills, equity curve.
 */

import { Position } from './position.js';
import { FillsStream } from './FillsStream.js';

/**
 * @typedef {Object} EquityPoint
 * @property {bigint} ts_event
 * @property {number} equity
 */

/**
 * @typedef {Object} EquityHistory
 * @property {number} initialEquity
 * @property {number} currentEquity
 * @property {bigint|null} lastUpdate
 */

/**
 * @typedef {Object} ExecutionStateSnapshot
 * @property {Object.<string, import('./position.js').PositionSnapshot>} positions
 * @property {import('./fill.js').FillResult[]} fills - Empty array in fills streaming mode
 * @property {number} [fillsCount] - Total fills count (streaming mode)
 * @property {string} [fillsStreamPath] - Path to fills JSONL file (streaming mode)
 * @property {EquityPoint[]} [equityCurve] - Legacy field, not populated in streaming mode
 * @property {number} [maxDrawdown] - Pre-computed max drawdown (streaming mode)
 * @property {number} [peakEquity] - Peak equity reached (streaming mode)
 * @property {EquityHistory} [equityHistory] - Minimal equity tracking (streaming mode)
 * @property {number} totalRealizedPnl
 * @property {number} totalUnrealizedPnl
 * @property {number} equity
 * @property {number} maxPositionValue
 */

/**
 * Execution state container
 */
export class ExecutionState {
  /** @type {Map<string, Position>} */
  positions = new Map();

  /** @type {import('./fill.js').FillResult[]} */
  fills = [];

  /** @type {EquityPoint[]} - Legacy field, not used in streaming mode */
  equityCurve = [];

  /** @type {number} Initial capital */
  initialCapital;

  /** @type {number} Track max exposure */
  maxPositionValue = 0;

  // Streaming maxDD tracking (private)
  /** @type {number} */
  #peakEquity = 0;

  /** @type {number} */
  #maxDrawdown = 0;

  /** @type {EquityHistory} */
  #equityHistory = {
    initialEquity: 0,
    currentEquity: 0,
    lastUpdate: null
  };

  /** @type {boolean} - Enable streaming maxDD calculation */
  #streamingMaxDD = false;

  // Streaming fills tracking (private)
  /** @type {FillsStream|null} */
  #fillsStream = null;

  /** @type {boolean} - Enable fills streaming to disk */
  #streamingFills = false;

  /**
   * @param {number} [initialCapital=10000] - Starting capital
   * @param {Object} [options={}] - Configuration options
   * @param {boolean} [options.streamingMaxDD=false] - Enable streaming maxDD (O(1) memory)
   * @param {boolean} [options.streamFills=false] - Enable fills streaming to disk
   * @param {string} [options.fillsStreamPath] - Custom path for fills stream (default: /tmp/fills_{timestamp}.jsonl)
   */
  constructor(initialCapital = 10000, options = {}) {
    this.initialCapital = initialCapital;
    this.#peakEquity = initialCapital;
    this.#equityHistory.initialEquity = initialCapital;
    this.#equityHistory.currentEquity = initialCapital;
    this.#streamingMaxDD = options.streamingMaxDD ?? false;

    // Initialize fills streaming if enabled
    if (options.streamFills) {
      this.#streamingFills = true;
      const tempPath = options.fillsStreamPath || `/tmp/fills_${Date.now()}.jsonl`;
      this.#fillsStream = new FillsStream(tempPath);
    }
  }

  /**
   * Get or create position for symbol
   * @param {string} symbol
   * @returns {Position}
   */
  getPosition(symbol) {
    if (!this.positions.has(symbol)) {
      this.positions.set(symbol, new Position(symbol));
    }
    return this.positions.get(symbol);
  }

  /**
   * Record a fill
   * @param {import('./fill.js').FillResult} fill
   */
  recordFill(fill) {
    if (this.#streamingFills) {
      this.#fillsStream.writeFill(fill);
    } else {
      this.fills.push(fill);
    }

    // Update max position exposure
    const position = this.getPosition(fill.symbol);
    const positionValue = Math.abs(position.size) * position.currentPrice;
    if (positionValue > this.maxPositionValue) {
      this.maxPositionValue = positionValue;
    }
  }

  /**
   * Get all fills (loads from disk if streaming)
   * @returns {import('./fill.js').FillResult[]}
   */
  async getFills() {
    if (this.#streamingFills) {
      await this.#fillsStream.close();
      return FillsStream.readFills(this.#fillsStream.getFilePath());
    }
    return this.fills;
  }

  /**
   * Get fill count (without loading from disk)
   * @returns {number}
   */
  getFillCount() {
    if (this.#streamingFills) {
      return this.#fillsStream.getFillCount();
    }
    return this.fills.length;
  }

  /**
   * Record equity point (call after price updates)
   * @param {bigint} ts_event
   */
  recordEquity(ts_event) {
    const equity = this.getEquity();

    if (this.#streamingMaxDD) {
      // Streaming mode: O(1) maxDD calculation
      if (equity > this.#peakEquity) {
        this.#peakEquity = equity;
      }

      const currentDD = (this.#peakEquity - equity) / this.#peakEquity;
      if (currentDD > this.#maxDrawdown) {
        this.#maxDrawdown = currentDD;
      }

      // Update minimal history
      this.#equityHistory.currentEquity = equity;
      this.#equityHistory.lastUpdate = ts_event;
    } else {
      // Legacy mode: full equity curve
      this.equityCurve.push({ ts_event, equity });
    }
  }

  /**
   * Get current max drawdown (streaming mode only)
   * @returns {number}
   */
  getMaxDrawdown() {
    return this.#maxDrawdown;
  }

  /**
   * Get peak equity (streaming mode only)
   * @returns {number}
   */
  getPeakEquity() {
    return this.#peakEquity;
  }

  /**
   * Get minimal equity history (streaming mode only)
   * @returns {EquityHistory}
   */
  getEquityHistory() {
    return { ...this.#equityHistory };
  }

  /**
   * Calculate total realized PnL across all positions
   * @returns {number}
   */
  getTotalRealizedPnl() {
    let total = 0;
    for (const pos of this.positions.values()) {
      total += pos.realizedPnl;
    }
    return total;
  }

  /**
   * Calculate total unrealized PnL across all positions
   * @returns {number}
   */
  getTotalUnrealizedPnl() {
    let total = 0;
    for (const pos of this.positions.values()) {
      total += pos.getUnrealizedPnl();
    }
    return total;
  }

  /**
   * Calculate current equity
   * @returns {number}
   */
  getEquity() {
    return this.initialCapital + this.getTotalRealizedPnl() + this.getTotalUnrealizedPnl();
  }

  /**
   * Get immutable state snapshot
   * @param {Object} [options={}] - Snapshot options
   * @param {boolean} [options.deepCopy=true] - Create deep copies of arrays (set false to reduce peak memory)
   * @returns {ExecutionStateSnapshot}
   */
  snapshot(options = {}) {
    const { deepCopy = true } = options;

    // Flush fills stream buffer (if streaming enabled)
    if (this.#streamingFills && this.#fillsStream) {
      this.#fillsStream.flush();
    }

    const positionsSnapshot = {};
    for (const [symbol, pos] of this.positions.entries()) {
      positionsSnapshot[symbol] = pos.snapshot();
    }

    // Fills handling: deep copy or reference
    let fillsArray;
    if (this.#streamingFills) {
      fillsArray = [];  // Always empty in streaming mode
    } else {
      fillsArray = deepCopy ? [...this.fills] : this.fills;
    }

    const baseSnapshot = {
      positions: positionsSnapshot,
      fills: fillsArray,
      totalRealizedPnl: this.getTotalRealizedPnl(),
      totalUnrealizedPnl: this.getTotalUnrealizedPnl(),
      equity: this.getEquity(),
      maxPositionValue: this.maxPositionValue
    };

    // Add immutability flag
    if (!deepCopy) {
      baseSnapshot._immutable = true;  // Signal: do not modify returned arrays
    }

    // Add fills streaming fields
    if (this.#streamingFills) {
      baseSnapshot.fillsCount = this.#fillsStream.getFillCount();
      baseSnapshot.fillsStreamPath = this.#fillsStream.getFilePath();
    }

    // Add maxDD fields
    if (this.#streamingMaxDD) {
      baseSnapshot.maxDrawdown = this.#maxDrawdown;
      baseSnapshot.peakEquity = this.#peakEquity;
      baseSnapshot.equityHistory = this.getEquityHistory();
    } else {
      // Legacy mode: full equity curve (deep copy or reference)
      baseSnapshot.equityCurve = deepCopy ? [...this.equityCurve] : this.equityCurve;
    }

    return baseSnapshot;
  }
}
