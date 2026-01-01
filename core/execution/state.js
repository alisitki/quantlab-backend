/**
 * QuantLab Execution Engine — State Container
 * Holds execution state: positions, fills, equity curve.
 */

import { Position } from './position.js';

/**
 * @typedef {Object} EquityPoint
 * @property {bigint} ts_event
 * @property {number} equity
 */

/**
 * @typedef {Object} ExecutionStateSnapshot
 * @property {Object.<string, import('./position.js').PositionSnapshot>} positions
 * @property {import('./fill.js').FillResult[]} fills
 * @property {EquityPoint[]} equityCurve
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
  
  /** @type {EquityPoint[]} */
  equityCurve = [];
  
  /** @type {number} Initial capital */
  initialCapital;
  
  /** @type {number} Track max exposure */
  maxPositionValue = 0;

  /**
   * @param {number} [initialCapital=10000] - Starting capital
   */
  constructor(initialCapital = 10000) {
    this.initialCapital = initialCapital;
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
    this.fills.push(fill);
    
    // Update max position exposure
    const position = this.getPosition(fill.symbol);
    const positionValue = Math.abs(position.size) * position.currentPrice;
    if (positionValue > this.maxPositionValue) {
      this.maxPositionValue = positionValue;
    }
  }

  /**
   * Record equity point (call after price updates)
   * @param {bigint} ts_event
   */
  recordEquity(ts_event) {
    const equity = this.getEquity();
    this.equityCurve.push({ ts_event, equity });
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
   * @returns {ExecutionStateSnapshot}
   */
  snapshot() {
    const positionsSnapshot = {};
    for (const [symbol, pos] of this.positions.entries()) {
      positionsSnapshot[symbol] = pos.snapshot();
    }

    return {
      positions: positionsSnapshot,
      fills: [...this.fills],
      equityCurve: [...this.equityCurve],
      totalRealizedPnl: this.getTotalRealizedPnl(),
      totalUnrealizedPnl: this.getTotalUnrealizedPnl(),
      equity: this.getEquity(),
      maxPositionValue: this.maxPositionValue
    };
  }
}
