/**
 * QuantLab Execution Engine — Fill Model
 * Defines fill results and factory functions.
 */

import { OrderSide } from './order.js';

/** Default fee rate (0.04% = 0.0004) */
export const DEFAULT_FEE_RATE = 0.0004;

/**
 * @typedef {Object} FillResult
 * @property {string} id - Unique fill ID (deterministic)
 * @property {string} orderId - Reference to filled order
 * @property {string} symbol - Trading symbol
 * @property {OrderSide} side - BUY or SELL
 * @property {number} qty - Filled quantity
 * @property {number} fillPrice - Execution price
 * @property {number} fillValue - qty * fillPrice
 * @property {number} fee - Fee paid in quote asset
 * @property {bigint} ts_event - Fill timestamp
 */

/** @type {number} */
let fillCounter = 0;

/**
 * Reset fill counter (for determinism in tests)
 */
export function resetFillCounter() {
  fillCounter = 0;
}

/**
 * Create a fill result from an order
 * @param {import('./order.js').Order} order - The order to fill
 * @param {number} price - Fill price from market event
 * @param {number} [feeRate=DEFAULT_FEE_RATE] - Fee rate
 * @returns {FillResult}
 */
export function createFill(order, price, feeRate = DEFAULT_FEE_RATE) {
  if (!order || !order.id) {
    throw new Error('FILL_INVALID: valid order required');
  }
  if (typeof price !== 'number' || price <= 0) {
    throw new Error('FILL_INVALID: price must be positive number');
  }

  fillCounter++;
  
  const fillValue = order.qty * price;
  const fee = fillValue * feeRate;

  return {
    id: `fill_${fillCounter}`,
    orderId: order.id,
    symbol: order.symbol,
    side: order.side,
    qty: order.qty,
    fillPrice: price,
    fillValue,
    fee,
    ts_event: order.ts_event
  };
}
