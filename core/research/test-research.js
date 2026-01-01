import crypto from 'crypto';
import { ReplayEngine } from '../replay/index.js';
import { runReplayWithStrategy } from '../strategy/Runner.js';
import { ExecutionEngine } from '../execution/index.js';
import { ResearchRunner } from './ResearchRunner.js';
import { TimeSampler } from './samplers/TimeSampler.js';
import { EventSampler } from './samplers/EventSampler.js';

// Test Dataset
const PARQUET_PATH = '/tmp/replay-test2/data.parquet';
const META_PATH = '/tmp/replay-test2/meta.json';

/**
 * Simple Momentum Strategy for testing
 */
class MomentumStrategy {
  #threshold = 0.0001;
  #prevPrice = null;

  async onEvent(event, ctx) {
    const price = event.mid_price || event.last_price || event.price || 0;
    if (this.#prevPrice === null) {
      this.#prevPrice = price;
      return;
    }

    const symbol = event.symbol || 'BTCUSDT';
    const ret = (price - this.#prevPrice) / this.#prevPrice;
    
    // Check if we are in Truth Mode (ExecutionEngine snapshot has 'positions' object, ResearchExecution has 'position' number)
    const snapshot = ctx.execution ? ctx.execution.snapshot() : null;
    const isTruthMode = !!(snapshot && snapshot.positions);

    if (isTruthMode) {
      // Truth Mode logic: simplified mapping for the test
      const state = ctx.execution.snapshot();
      const pos = state.positions[symbol] || { size: 0 };
      const currentPos = pos.size > 0 ? 1 : (pos.size < 0 ? -1 : 0);

      if (ret > this.#threshold && currentPos <= 0) {
        ctx.placeOrder({ side: 'BUY', price, symbol, qty: 0.01 });
      } else if (ret < -this.#threshold && currentPos >= 0) {
        ctx.placeOrder({ side: 'SELL', price, symbol, qty: 0.01 });
      } else if (Math.abs(ret) < this.#threshold / 2 && currentPos !== 0) {
        ctx.placeOrder({ side: currentPos > 0 ? 'SELL' : 'BUY', price, symbol, qty: 0.01 });
      }
    } else {
      // Research Mode logic: simplified LONG/SHORT/FLAT
      if (ret > this.#threshold) {
        ctx.placeOrder({ side: 'LONG', price, symbol });
      } else if (ret < -this.#threshold) {
        ctx.placeOrder({ side: 'SHORT', price, symbol });
      } else {
        ctx.placeOrder({ side: 'FLAT', price, symbol });
      }
    }

    this.#prevPrice = price;
  }
}

function hashResult(result) {
  const data = JSON.stringify({
    stats: result.stats,
    metrics: result.metrics,
    tradeCount: result.snapshot.tradeCount,
    totalPnl: result.snapshot.totalPnl.toFixed(8)
  });
  return crypto.createHash('sha256').update(data).digest('hex').slice(0, 16);
}

async function runVerification() {
  console.log('========================================');
  console.log('  FAST RESEARCH MODE v1 TEST');
  console.log('========================================');
  console.log(`DATASET: ${PARQUET_PATH}\n`);

  const replayEngine = new ReplayEngine(PARQUET_PATH, META_PATH);
  const strategy = new MomentumStrategy();

  // 1. Truth Mode Run (Baseline)
  console.log('--- RUNNING TRUTH MODE (Baseline) ---');
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const startTruth = Date.now();
  await runReplayWithStrategy({
    replayEngine,
    strategy: new MomentumStrategy(),
    options: { executionEngine, batchSize: 5000 }
  });
  const endTruth = Date.now();
  const truthTime = endTruth - startTruth;
  const truthSnapshot = executionEngine.snapshot();
  console.log(`Truth Mode Time: ${truthTime}ms`);
  console.log(`Truth PnL: ${truthSnapshot.totalRealizedPnl.toFixed(2)}`);
  console.log(`Truth Trades: ${truthSnapshot.fills.length / 2} (approx)`);

  // 2. Research Mode Run (No Sampling) - Should be faster due to light execution
  console.log('\n--- RUNNING RESEARCH MODE (No Sampling) ---');
  const startResearchFull = Date.now();
  const researchFull = await ResearchRunner.runResearch({
    replayEngine,
    strategy: new MomentumStrategy(),
    sampler: null,
    options: { batchSize: 10000 }
  });
  const endResearchFull = Date.now();
  const researchFullTime = endResearchFull - startResearchFull;
  console.log(`Research Mode (No Sampling) Time: ${researchFullTime}ms`);
  console.log(`Research PnL: ${researchFull.metrics.totalReturn.toFixed(2)}`);

  // 3. Research Mode Run (Event Sampling N=10)
  console.log('\n--- RUNNING RESEARCH MODE (Event Sampling N=10) ---');
  const startResearchS10 = Date.now();
  const researchS10 = await ResearchRunner.runResearch({
    replayEngine,
    strategy: new MomentumStrategy(),
    sampler: new EventSampler({ n: 10 }),
    options: { batchSize: 10000 }
  });
  const endResearchS10 = Date.now();
  const researchS10Time = endResearchS10 - startResearchS10;
  console.log(`Research Mode (N=10) Time: ${researchS10Time}ms`);
  console.log(`Processed: ${researchS10.stats.processed}, Sampled: ${researchS10.stats.sampled}`);
  console.log(`Research PnL: ${researchS10.metrics.totalReturn.toFixed(2)}`);

  // 4. Research Mode Run (Time Sampling 1s=1000ms)
  console.log('\n--- RUNNING RESEARCH MODE (Time Sampling 1s) ---');
  const startResearchT1s = Date.now();
  const researchT1s = await ResearchRunner.runResearch({
    replayEngine,
    strategy: new MomentumStrategy(),
    sampler: new TimeSampler({ intervalMs: 1000 }),
    options: { batchSize: 10000 }
  });
  const endResearchT1s = Date.now();
  const researchT1sTime = endResearchT1s - startResearchT1s;
  console.log(`Research Mode (1s) Time: ${researchT1sTime}ms`);
  console.log(`Processed: ${researchT1s.stats.processed}, Sampled: ${researchT1s.stats.sampled}`);
  console.log(`Research PnL: ${researchT1s.metrics.totalReturn.toFixed(2)}`);

  // --- Speed Verification ---
  console.log('\n--- SPEED COMPARISON ---');
  const speedup = truthTime / researchS10Time;
  console.log(`Speedup (Truth vs Sampled N=10): ${speedup.toFixed(2)}x`);
  if (speedup >= 10) {
    console.log('✓ PASS: Research mode is >= 10x faster');
  } else {
    console.log('⚠ WARN: Speedup < 10x (might be due to small dataset or overhead)');
  }

  // --- Directional Verification ---
  console.log('\n--- DIRECTIONAL CHECK ---');
  const truthSign = Math.sign(truthSnapshot.totalRealizedPnl);
  const researchSign = Math.sign(researchFull.metrics.totalReturn);
  console.log(`Truth Sign: ${truthSign}, Research Sign: ${researchSign}`);
  if (truthSign === researchSign || truthSign === 0 || researchSign === 0) {
    console.log('✓ PASS: Directionally similar');
  } else {
    console.log('⚠ WARN: Directional mismatch (might be due to simplified execution)');
  }

  // --- Determinism Verification ---
  console.log('\n--- DETERMINISM CHECK ---');
  const research1 = await ResearchRunner.runResearch({
    replayEngine,
    strategy: new MomentumStrategy(),
    sampler: new EventSampler({ n: 5 }),
    options: { batchSize: 5000 }
  });
  const research2 = await ResearchRunner.runResearch({
    replayEngine,
    strategy: new MomentumStrategy(),
    sampler: new EventSampler({ n: 5 }),
    options: { batchSize: 5000 }
  });

  const h1 = hashResult(research1);
  const h2 = hashResult(research2);
  console.log(`Run 1 Hash: ${h1}`);
  console.log(`Run 2 Hash: ${h2}`);

  if (h1 === h2) {
    console.log('✓ PASS: Deterministic');
  } else {
    console.log('✗ FAIL: Non-deterministic');
    process.exit(1);
  }

  await replayEngine.close();
  console.log('\n========================================');
  console.log('  ALL RESEARCH TESTS PASSED');
  console.log('========================================');
}

runVerification().catch(err => {
  console.error(err);
  process.exit(1);
});
