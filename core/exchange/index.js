/**
 * Exchange Module - JS Compatibility Layer
 *
 * Note: Full TypeScript implementation in index.ts
 * This file provides minimal stubs for service startup.
 */

// Stub adapters
export class BinanceFuturesAdapter {
  constructor(creds) { this.exchange = 'binance'; }
  async ping() { return { success: false, latency_ms: 0 }; }
  async getPositions() { return []; }
  async submitOrder() { throw new Error('Stub implementation'); }
}

export class BybitFuturesAdapter {
  constructor(creds) { this.exchange = 'bybit'; }
  async ping() { return { success: false, latency_ms: 0 }; }
  async getPositions() { return []; }
  async submitOrder() { throw new Error('Stub implementation'); }
}

export class OkxFuturesAdapter {
  constructor(creds) { this.exchange = 'okx'; }
  async ping() { return { success: false, latency_ms: 0 }; }
  async getPositions() { return []; }
  async submitOrder() { throw new Error('Stub implementation'); }
}

// Execution bridge
export class ExecutionBridge {
  constructor() {
    console.warn('[ExecutionBridge] Using stub implementation');
  }
  async start() { return { success: false, reason: 'stub' }; }
  async stop() { return { success: true }; }
  getStats() { return { orders_total: 0, mode: 'DISABLED' }; }
  async executeOrder() { return { success: false, reason: 'stub' }; }
}

// Position reconciler
export class PositionReconciler {
  constructor() {}
  async reconcile() { return { matches: [], mismatches: [] }; }
  getLastReport() { return null; }
}

// Health monitor
export class ExchangeHealthMonitor {
  constructor() {}
  async checkHealth() { return { healthy: false, reason: 'stub' }; }
  getLastStatus() { return null; }
}

// Slippage analyzer
export class SlippageAnalyzer {
  constructor() {}
  record() {}
  getStats() { return { count: 0, avg_slippage_bps: 0 }; }
}

// Order lifecycle manager
export class OrderLifecycleManager {
  constructor() {}
  getOrder() { return null; }
  getRecentOrders() { return []; }
  getStateCounts() { return {}; }
}

// Bridge mode enum
export const BridgeMode = {
  DISABLED: 'DISABLED',
  SHADOW: 'SHADOW',
  CANARY: 'CANARY',
  ACTIVE: 'ACTIVE'
};

// Config loaders
export function loadExecutionConfigFromEnv() {
  return {
    mode: 'DISABLED',
    exchange: 'binance',
    testnet: true,
    daily_order_limit: 100,
    max_position_size: 1000
  };
}

export function validateExecutionConfig(config) {
  return { valid: true, errors: [] };
}

// Credentials helpers
export function loadCredentialsForExchange(exchange) {
  return {
    apiKey: process.env[`${exchange.toUpperCase()}_API_KEY`] || '',
    apiSecret: process.env[`${exchange.toUpperCase()}_API_SECRET`] || '',
    testnet: true
  };
}

export function validateCredentials(creds) {
  return { valid: !!creds.apiKey, errors: creds.apiKey ? [] : ['Missing API key'] };
}

export function maskCredentials(creds) {
  return {
    ...creds,
    apiKey: creds.apiKey ? `${creds.apiKey.slice(0, 4)}...` : '',
    apiSecret: '***'
  };
}
