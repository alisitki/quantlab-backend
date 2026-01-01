/**
 * QuantLab Execution Engine — Position Model
 * Tracks position size, avg entry price, and PnL per symbol.
 */

import { OrderSide } from './order.js';

/**
 * @typedef {Object} PositionSnapshot
 * @property {string} symbol
 * @property {number} size - Positive = long, negative = short
 * @property {number} avgEntryPrice
 * @property {number} realizedPnl
 * @property {number} unrealizedPnl
 * @property {number} currentPrice
 */

/**
 * Position tracker for a single symbol
 */
export class Position {
  /** @type {string} */
  symbol;
  /** @type {number} */
  size = 0;
  /** @type {number} */
  avgEntryPrice = 0;
  /** @type {number} */
  realizedPnl = 0;
  /** @type {number} */
  currentPrice = 0;

  /**
   * @param {string} symbol
   */
  constructor(symbol) {
    this.symbol = symbol;
  }

  /**
   * Apply a fill to update position
   * @param {import('./fill.js').FillResult} fill
   */
  applyFill(fill) {
    const direction = fill.side === OrderSide.BUY ? 1 : -1;
    const fillQty = fill.qty * direction;
    const fillCost = fill.fillValue;
    const fee = fill.fee;

    // Update current price
    this.currentPrice = fill.fillPrice;

    // Case 1: Opening or adding to position
    if ((this.size >= 0 && direction > 0) || (this.size <= 0 && direction < 0)) {
      // Same direction: average the entry price
      const oldCost = Math.abs(this.size) * this.avgEntryPrice;
      const newCost = fillCost;
      const newSize = this.size + fillQty;
      
      if (newSize !== 0) {
        this.avgEntryPrice = (oldCost + newCost) / Math.abs(newSize);
      }
      this.size = newSize;
      
      // Fee is always a cost
      this.realizedPnl -= fee;
    }
    // Case 2: Reducing or closing position
    else {
      const closingQty = Math.min(Math.abs(fillQty), Math.abs(this.size));
      
      // Calculate realized PnL for the closing portion
      const entryValue = closingQty * this.avgEntryPrice;
      const exitValue = closingQty * fill.fillPrice;
      
      // If we were long and now selling: profit = exit - entry
      // If we were short and now buying: profit = entry - exit
      if (this.size > 0) {
        // Was long, selling
        this.realizedPnl += (exitValue - entryValue);
      } else {
        // Was short, buying
        this.realizedPnl += (entryValue - exitValue);
      }
      
      // Deduct fee
      this.realizedPnl -= fee;
      
      // Update size
      this.size += fillQty;
      
      // If position flipped, recalculate avg entry for new direction
      if ((this.size > 0 && direction > 0) || (this.size < 0 && direction < 0)) {
        // Position flipped: remaining qty at new price
        this.avgEntryPrice = fill.fillPrice;
      } else if (this.size === 0) {
        this.avgEntryPrice = 0;
      }
    }
  }

  /**
   * Update unrealized PnL based on current market price
   * @param {number} price
   */
  updateMtm(price) {
    this.currentPrice = price;
  }

  /**
   * Calculate unrealized PnL
   * @returns {number}
   */
  getUnrealizedPnl() {
    if (this.size === 0 || this.currentPrice === 0) return 0;
    
    const marketValue = Math.abs(this.size) * this.currentPrice;
    const entryValue = Math.abs(this.size) * this.avgEntryPrice;
    
    if (this.size > 0) {
      // Long: profit if price went up
      return marketValue - entryValue;
    } else {
      // Short: profit if price went down
      return entryValue - marketValue;
    }
  }

  /**
   * Get immutable position snapshot
   * @returns {PositionSnapshot}
   */
  snapshot() {
    return {
      symbol: this.symbol,
      size: this.size,
      avgEntryPrice: this.avgEntryPrice,
      realizedPnl: this.realizedPnl,
      unrealizedPnl: this.getUnrealizedPnl(),
      currentPrice: this.currentPrice
    };
  }
}
