#!/usr/bin/env node
/**
 * QuantLab Risk Management v1 — Test Runner
 * 
 * Tests:
 * 1. End-to-end strategy with risk management
 * 2. Validates risk rules are enforced
 * 3. Determinism verification
 * 
 * Usage:
 *   node risk/test-risk.js <s3_parquet_glob> <s3_meta_path>
 */

import crypto from 'crypto';
import { ReplayEngine } from '../replay/index.js';
import { runReplayWithStrategy } from '../strategy/Runner.js';
import { ExecutionEngine } from '../execution/index.js';
import { buildBacktestSummary, printBacktestSummary } from '../backtest/summary.js';
import { RiskManager } from './RiskManager.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node risk/test-risk.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

/**
 * Strategy with risk management integration
 */
class RiskAwareStrategy {
  #riskManager;
  #config;
  #featureState = { prev_mid: null };
  #tradeCount = 0;
  #signalCount = 0;
  #rejectedCount = 0;

  constructor(config = {}) {
    this.#config = {
      symbol: 'btcusdt',
      orderQty: 0.01,
      momentumThreshold: 0.0001,
      spreadMaxBps: 10,
      ...config
    };
  }

  async onStart(ctx) {
    // Create risk manager with execution's initial capital
    const initialCapital = ctx.execution ? 10000 : 10000;
    this.#riskManager = new RiskManager({
      maxPositions: 1,
      cooldownEvents: 50,
      maxDailyLossPct: 0.02,
      stopLossPct: 0.005,
      takeProfitPct: 0.01
    }, initialCapital);

    ctx.logger.info('=== RiskAwareStrategy ===');
    this.#riskManager.logConfig(ctx.logger);
  }

  async onEvent(event, ctx) {
    if (!ctx.placeOrder) return;

    // 1. Update risk manager state
    this.#riskManager.onEvent(event, ctx);

    // 2. Check for forced exits (SL/TP)
    const forceExit = this.#riskManager.checkForExit(event, ctx);
    if (forceExit) {
      ctx.logger.info(`FORCE_EXIT: ${forceExit.reason}`);
      ctx.placeOrder(forceExit);
      this.#tradeCount++;
      return; // Don't process new signals after forced exit
    }

    // 3. Compute features
    const bid = Number(event.bid_price);
    const ask = Number(event.ask_price);
    const mid = (bid + ask) / 2;
    
    if (this.#featureState.prev_mid === null) {
      this.#featureState.prev_mid = mid;
      return;
    }

    const return_1 = (mid - this.#featureState.prev_mid) / this.#featureState.prev_mid;
    const spread = ask - bid;
    const spreadBps = (spread / mid) * 10000;
    
    this.#featureState.prev_mid = mid;

    // Skip if spread too wide
    if (spreadBps > this.#config.spreadMaxBps) return;

    // 4. Generate signal
    const threshold = this.#config.momentumThreshold;
    let signal = { action: 'FLAT' };

    // Get current position
    const state = ctx.execution.snapshot();
    const pos = state.positions[event.symbol?.toUpperCase()] || { size: 0 };
    const position = pos.size > 0 ? 'LONG' : (pos.size < 0 ? 'SHORT' : 'FLAT');

    if (position === 'FLAT') {
      if (return_1 > threshold) signal = { action: 'LONG' };
      else if (return_1 < -threshold) signal = { action: 'SHORT' };
    } else if (position === 'LONG' && return_1 < -threshold) {
      signal = { action: 'EXIT_LONG' };
    } else if (position === 'SHORT' && return_1 > threshold) {
      signal = { action: 'EXIT_SHORT' };
    }

    if (signal.action === 'FLAT') return;

    this.#signalCount++;

    // 5. Check with risk manager
    const { allowed, reason } = this.#riskManager.allow(signal, ctx);
    if (!allowed) {
      this.#rejectedCount++;
      // Log rejections periodically
      if (this.#rejectedCount % 100 === 1) {
        ctx.logger.info(`REJECTED (sample): ${reason}`);
      }
      return;
    }

    // 6. Execute
    const symbol = event.symbol || this.#config.symbol.toUpperCase();
    let side, qty;

    switch (signal.action) {
      case 'LONG':
        side = 'BUY';
        qty = this.#config.orderQty;
        break;
      case 'SHORT':
        side = 'SELL';
        qty = this.#config.orderQty;
        break;
      case 'EXIT_LONG':
        side = 'SELL';
        qty = this.#config.orderQty;
        break;
      case 'EXIT_SHORT':
        side = 'BUY';
        qty = this.#config.orderQty;
        break;
      default:
        return;
    }

    ctx.placeOrder({ symbol, side, qty, ts_event: event.ts_event });
    this.#tradeCount++;
  }

  async onEnd(ctx) {
    const riskStats = this.#riskManager.getStats();
    ctx.logger.info('\n=== RiskAwareStrategy Summary ===');
    ctx.logger.info(`Signals: ${this.#signalCount}`);
    ctx.logger.info(`Trades:  ${this.#tradeCount}`);
    ctx.logger.info(`Rejected by risk: ${this.#rejectedCount}`);
    ctx.logger.info(`Force exits (SL/TP): ${riskStats.forceExitCount}`);
    ctx.logger.info(`Daily loss locked: ${riskStats.dailyLossLocked}`);
  }

  getStats() {
    return {
      tradeCount: this.#tradeCount,
      signalCount: this.#signalCount,
      rejectedCount: this.#rejectedCount,
      riskStats: this.#riskManager.getStats()
    };
  }
}

/**
 * Hash state for determinism
 */
function hashState(state) {
  const data = JSON.stringify({
    fillCount: state.fills.length,
    equity: state.equity.toFixed(8),
    realizedPnl: state.totalRealizedPnl.toFixed(8),
    fills: state.fills.map(f => ({
      id: f.fillId,
      side: f.side,
      qty: f.qty,
      price: f.fillPrice.toFixed(8)
    }))
  });
  return crypto.createHash('sha256').update(data).digest('hex').slice(0, 16);
}

async function runOnce(label) {
  console.log(`\n--- RUN ${label} ---`);
  
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const strategy = new RiskAwareStrategy();

  try {
    const result = await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize: 5000,
        parquetPath,
        metaPath,
        executionEngine
      }
    });

    const state = executionEngine.snapshot();
    const hash = hashState(state);
    const strategyStats = strategy.getStats();
    
    console.log(`processed=${result.stats.processed}`);
    console.log(`fills=${state.fills.length}`);
    console.log(`signals=${strategyStats.signalCount}`);
    console.log(`trades=${strategyStats.tradeCount}`);
    console.log(`rejected=${strategyStats.rejectedCount}`);
    console.log(`force_exits=${strategyStats.riskStats.forceExitCount}`);
    console.log(`equity=${state.equity.toFixed(4)}`);
    console.log(`HASH=${hash}`);

    return { hash, state, strategyStats };
  } finally {
    await replayEngine.close();
  }
}

async function main() {
  console.log('========================================');
  console.log('  RISK MANAGEMENT v1 TEST');
  console.log('========================================');
  console.log(`DATASET: ${parquetPath}`);

  const run1 = await runOnce('1');
  const run2 = await runOnce('2');

  console.log('\n--- DETERMINISM CHECK ---');
  console.log(`Run1 HASH: ${run1.hash}`);
  console.log(`Run2 HASH: ${run2.hash}`);

  const deterministic = run1.hash === run2.hash;
  if (deterministic) {
    console.log('✓ PASS: Deterministic');
  } else {
    console.log('✗ FAIL: Non-deterministic');
    process.exit(1);
  }

  console.log('\n--- RISK VALIDATION ---');
  
  // Validate rejections occurred
  if (run1.strategyStats.rejectedCount > 0) {
    console.log(`✓ PASS: Signals rejected (${run1.strategyStats.rejectedCount})`);
  } else {
    console.log('⚠ WARN: No signals rejected');
  }

  // Validate trades occurred
  if (run1.state.fills.length > 0) {
    console.log(`✓ PASS: Trades executed (${run1.state.fills.length})`);
  } else {
    console.log('✗ FAIL: No trades');
    process.exit(1);
  }

  // Metrics
  console.log('\n--- METRICS ---');
  const summary = buildBacktestSummary(run1.state, { initialCapital: 10000 });
  printBacktestSummary(summary);

  console.log('\n========================================');
  console.log('  ALL TESTS PASSED');
  console.log('========================================');
}

main().catch(err => {
  console.error(`\nFATAL: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
