/**
 * QuantLab Execution Engine — Order Model
 * Defines order types and factory functions for paper trading.
 */

/**
 * Order side enum
 * @readonly
 * @enum {string}
 */
export const OrderSide = {
  BUY: 'BUY',
  SELL: 'SELL'
};

/**
 * @typedef {Object} Order
 * @property {string} id - Unique order ID (deterministic)
 * @property {string} symbol - Trading symbol (e.g., 'BTCUSDT')
 * @property {OrderSide} side - BUY or SELL
 * @property {number} qty - Order quantity in base asset
 * @property {bigint} ts_event - Event timestamp when order was placed
 */

/** @type {number} */
let orderCounter = 0;

/**
 * Reset order counter (for determinism in tests)
 */
export function resetOrderCounter() {
  orderCounter = 0;
}

/**
 * Create a new order with deterministic ID
 * @param {Object} params
 * @param {string} params.symbol
 * @param {OrderSide} params.side
 * @param {number} params.qty
 * @param {bigint} params.ts_event
 * @returns {Order}
 */
export function createOrder({ symbol, side, qty, ts_event }) {
  if (!symbol || typeof symbol !== 'string') {
    throw new Error('ORDER_INVALID: symbol is required');
  }
  if (!Object.values(OrderSide).includes(side)) {
    throw new Error(`ORDER_INVALID: side must be BUY or SELL, got ${side}`);
  }
  if (typeof qty !== 'number' || qty <= 0) {
    throw new Error('ORDER_INVALID: qty must be positive number');
  }
  if (ts_event === undefined) {
    throw new Error('ORDER_INVALID: ts_event is required');
  }

  orderCounter++;
  
  return {
    id: `ord_${orderCounter}`,
    symbol,
    side,
    qty,
    ts_event
  };
}
