#!/usr/bin/env node
/**
 * Strategy Comparison Tool
 *
 * Runs two strategies on the same dataset and compares performance metrics.
 *
 * Usage:
 *   node tools/compare-strategies.js \
 *     --parquet <path> --meta <path> \
 *     --strategy-a BaselineStrategy \
 *     --strategy-b StrategyV1 \
 *     --output text|json|markdown
 *
 * Example:
 *   node tools/compare-strategies.js \
 *     --parquet "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/*.parquet" \
 *     --meta "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/meta.json" \
 *     --strategy-a BaselineStrategy \
 *     --strategy-b StrategyV1
 */

import { parseArgs } from 'node:util';
import { ReplayEngine } from '../core/replay/index.js';
import { runReplayWithStrategy } from '../core/strategy/Runner.js';
import { ExecutionEngine } from '../core/execution/index.js';
import { buildBacktestSummary } from '../core/backtest/summary.js';
import { BaselineStrategy } from '../core/strategy/baseline/BaselineStrategy.js';
import { StrategyV1 } from '../core/strategy/v1/StrategyV1.js';
import { getConfig } from '../core/strategy/v1/config.js';

const STRATEGIES = {
  BaselineStrategy,
  StrategyV1
};

/**
 * Run a single strategy backtest
 */
async function runStrategy(strategyName, parquetPath, metaPath, config = {}) {
  console.log(`\n--- Running ${strategyName} ---`);

  const StrategyClass = STRATEGIES[strategyName];
  if (!StrategyClass) {
    throw new Error(`Unknown strategy: ${strategyName}`);
  }

  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });

  // Strategy-specific config
  let strategy;
  if (strategyName === 'StrategyV1') {
    const v1Config = getConfig(config.preset || 'default');
    v1Config.featureReportPath = './reports/feature_analysis_full_2026-02-05.json';
    strategy = new StrategyClass(v1Config);
  } else if (strategyName === 'BaselineStrategy') {
    // Default BaselineStrategy config
    strategy = new StrategyClass({
      symbol: 'btcusdt',
      orderQty: 0.01,
      cooldownEvents: 50,
      momentumThreshold: 0.0001,
      spreadMaxBps: 10,
      ...config
    });
  } else {
    strategy = new StrategyClass(config);
  }

  try {
    await runReplayWithStrategy({
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
    const summary = buildBacktestSummary(state, { initialCapital: 10000 });

    console.log(`  Fills: ${state.fills.length}`);
    console.log(`  Equity: ${state.equity.toFixed(2)}`);
    console.log(`  Return: ${summary.return_pct >= 0 ? '+' : ''}${summary.return_pct.toFixed(2)}%`);

    return { strategy: strategyName, summary, state };
  } finally {
    await replayEngine.close();
  }
}

/**
 * Compare two backtest results
 */
function compareMetrics(resultA, resultB) {
  const a = resultA.summary;
  const b = resultB.summary;

  return {
    // Returns
    return_pct: {
      a: a.return_pct,
      b: b.return_pct,
      diff: b.return_pct - a.return_pct,
      improvement: a.return_pct !== 0 ? ((b.return_pct - a.return_pct) / Math.abs(a.return_pct)) * 100 : 0
    },

    // Risk
    max_drawdown_pct: {
      a: a.max_drawdown_pct,
      b: b.max_drawdown_pct,
      diff: b.max_drawdown_pct - a.max_drawdown_pct,
      better: b.max_drawdown_pct < a.max_drawdown_pct ? 'B' : 'A'
    },

    // Efficiency
    win_rate: {
      a: a.win_rate,
      b: b.win_rate,
      diff: b.win_rate - a.win_rate,
      improvement: a.win_rate !== 0 ? ((b.win_rate - a.win_rate) / a.win_rate) * 100 : 0
    },

    avg_trade_pnl: {
      a: a.avg_trade_pnl,
      b: b.avg_trade_pnl,
      diff: b.avg_trade_pnl - a.avg_trade_pnl
    },

    // Volume
    trades: {
      a: a.trades,
      b: b.trades,
      diff: b.trades - a.trades
    },

    total_pnl: {
      a: a.total_pnl,
      b: b.total_pnl,
      diff: b.total_pnl - a.total_pnl
    },

    // Composite Score (Return-to-Drawdown Ratio)
    score: {
      a: a.return_pct / Math.max(Math.abs(a.max_drawdown_pct), 1),
      b: b.return_pct / Math.max(Math.abs(b.max_drawdown_pct), 1),
      winner: null  // calculated below
    }
  };
}

/**
 * Generate recommendation based on comparison
 */
function generateRecommendation(comparison, resultA, resultB) {
  const scoreA = comparison.score.a;
  const scoreB = comparison.score.b;
  const scoreDiff = scoreB - scoreA;

  comparison.score.winner = scoreB > scoreA ? 'B' : 'A';

  let confidence;
  if (Math.abs(scoreDiff) > 0.5) confidence = 'HIGH';
  else if (Math.abs(scoreDiff) > 0.2) confidence = 'MEDIUM';
  else confidence = 'LOW';

  let recommendation;
  if (scoreB > scoreA) {
    recommendation = `Strategy B (${resultB.strategy}) outperforms A (${resultA.strategy}) by ${scoreDiff.toFixed(3)}`;
  } else {
    recommendation = `Strategy A (${resultA.strategy}) outperforms B (${resultB.strategy}) by ${Math.abs(scoreDiff).toFixed(3)}`;
  }

  return { recommendation, confidence, scoreDiff };
}

/**
 * Print comparison in text format
 */
function printComparison(resultA, resultB, comparison, recommendation) {
  console.log('\n========================================');
  console.log('  STRATEGY COMPARISON');
  console.log('========================================\n');
  console.log(`Strategy A: ${resultA.strategy}`);
  console.log(`Strategy B: ${resultB.strategy}\n`);

  console.log('--- RETURNS ---');
  console.log(`  A: ${comparison.return_pct.a >= 0 ? '+' : ''}${comparison.return_pct.a.toFixed(2)}%`);
  console.log(`  B: ${comparison.return_pct.b >= 0 ? '+' : ''}${comparison.return_pct.b.toFixed(2)}%`);
  console.log(`  Diff: ${comparison.return_pct.diff >= 0 ? '+' : ''}${comparison.return_pct.diff.toFixed(2)}%`);
  console.log(`  Improvement: ${comparison.return_pct.improvement >= 0 ? '+' : ''}${comparison.return_pct.improvement.toFixed(1)}%\n`);

  console.log('--- RISK (MAX DRAWDOWN) ---');
  console.log(`  A: ${comparison.max_drawdown_pct.a.toFixed(2)}%`);
  console.log(`  B: ${comparison.max_drawdown_pct.b.toFixed(2)}%`);
  console.log(`  Diff: ${comparison.max_drawdown_pct.diff >= 0 ? '+' : ''}${comparison.max_drawdown_pct.diff.toFixed(2)}%`);
  console.log(`  Better Risk: ${comparison.max_drawdown_pct.better}\n`);

  console.log('--- EFFICIENCY ---');
  console.log(`  Win Rate A: ${(comparison.win_rate.a * 100).toFixed(2)}%`);
  console.log(`  Win Rate B: ${(comparison.win_rate.b * 100).toFixed(2)}%`);
  console.log(`  Win Rate Improvement: ${comparison.win_rate.improvement >= 0 ? '+' : ''}${comparison.win_rate.improvement.toFixed(1)}%\n`);
  console.log(`  Avg Trade PnL A: $${comparison.avg_trade_pnl.a.toFixed(4)}`);
  console.log(`  Avg Trade PnL B: $${comparison.avg_trade_pnl.b.toFixed(4)}`);
  console.log(`  Avg Trade Diff: $${comparison.avg_trade_pnl.diff >= 0 ? '+' : ''}${comparison.avg_trade_pnl.diff.toFixed(4)}\n`);

  console.log('--- VOLUME ---');
  console.log(`  Trades A: ${comparison.trades.a}`);
  console.log(`  Trades B: ${comparison.trades.b}`);
  console.log(`  Diff: ${comparison.trades.diff >= 0 ? '+' : ''}${comparison.trades.diff}\n`);

  console.log('--- COMPOSITE SCORE (Return/Drawdown) ---');
  console.log(`  A: ${comparison.score.a.toFixed(3)}`);
  console.log(`  B: ${comparison.score.b.toFixed(3)}`);
  console.log(`  Winner: ${comparison.score.winner}\n`);

  console.log('========================================');
  console.log('  RECOMMENDATION');
  console.log('========================================');
  console.log(`  ${recommendation.recommendation}`);
  console.log(`  Confidence: ${recommendation.confidence}`);
  console.log('========================================\n');
}

/**
 * Print comparison in markdown format
 */
function printMarkdown(resultA, resultB, comparison, recommendation) {
  console.log('# Strategy Comparison\n');
  console.log(`**Strategy A:** ${resultA.strategy}`);
  console.log(`**Strategy B:** ${resultB.strategy}\n`);

  console.log('## Returns\n');
  console.log('| Metric | A | B | Diff | Improvement |');
  console.log('|--------|---|---|------|-------------|');
  console.log(`| Return % | ${comparison.return_pct.a.toFixed(2)}% | ${comparison.return_pct.b.toFixed(2)}% | ${comparison.return_pct.diff >= 0 ? '+' : ''}${comparison.return_pct.diff.toFixed(2)}% | ${comparison.return_pct.improvement >= 0 ? '+' : ''}${comparison.return_pct.improvement.toFixed(1)}% |\n`);

  console.log('## Risk\n');
  console.log('| Metric | A | B | Diff | Better |');
  console.log('|--------|---|---|------|--------|');
  console.log(`| Max Drawdown % | ${comparison.max_drawdown_pct.a.toFixed(2)}% | ${comparison.max_drawdown_pct.b.toFixed(2)}% | ${comparison.max_drawdown_pct.diff >= 0 ? '+' : ''}${comparison.max_drawdown_pct.diff.toFixed(2)}% | ${comparison.max_drawdown_pct.better} |\n`);

  console.log('## Efficiency\n');
  console.log('| Metric | A | B | Diff/Improvement |');
  console.log('|--------|---|---|------------------|');
  console.log(`| Win Rate | ${(comparison.win_rate.a * 100).toFixed(2)}% | ${(comparison.win_rate.b * 100).toFixed(2)}% | ${comparison.win_rate.improvement >= 0 ? '+' : ''}${comparison.win_rate.improvement.toFixed(1)}% |`);
  console.log(`| Avg Trade PnL | $${comparison.avg_trade_pnl.a.toFixed(4)} | $${comparison.avg_trade_pnl.b.toFixed(4)} | $${comparison.avg_trade_pnl.diff >= 0 ? '+' : ''}${comparison.avg_trade_pnl.diff.toFixed(4)} |\n`);

  console.log('## Composite Score\n');
  console.log('| Metric | A | B | Winner |');
  console.log('|--------|---|---|--------|');
  console.log(`| Score (Return/Drawdown) | ${comparison.score.a.toFixed(3)} | ${comparison.score.b.toFixed(3)} | ${comparison.score.winner} |\n`);

  console.log('## Recommendation\n');
  console.log(`**${recommendation.recommendation}**\n`);
  console.log(`Confidence: **${recommendation.confidence}**\n`);
}

/**
 * Main entry point
 */
async function main() {
  const { values } = parseArgs({
    options: {
      parquet: { type: 'string' },
      meta: { type: 'string' },
      'strategy-a': { type: 'string' },
      'strategy-b': { type: 'string' },
      output: { type: 'string', default: 'text' }
    }
  });

  if (!values.parquet || !values.meta) {
    console.error('Usage: node compare-strategies.js --parquet <path> --meta <path> [--strategy-a <name>] [--strategy-b <name>] [--output text|json|markdown]');
    console.error('\nExample:');
    console.error('  node compare-strategies.js \\');
    console.error('    --parquet "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/*.parquet" \\');
    console.error('    --meta "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20251228/meta.json" \\');
    console.error('    --strategy-a BaselineStrategy \\');
    console.error('    --strategy-b StrategyV1');
    process.exit(1);
  }

  const strategyA = values['strategy-a'] || 'BaselineStrategy';
  const strategyB = values['strategy-b'] || 'StrategyV1';

  console.log('========================================');
  console.log('  STRATEGY COMPARISON TOOL');
  console.log('========================================');
  console.log(`Dataset: ${values.parquet}`);
  console.log(`Strategy A: ${strategyA}`);
  console.log(`Strategy B: ${strategyB}`);
  console.log(`Output: ${values.output}`);

  // Run both strategies
  const resultA = await runStrategy(strategyA, values.parquet, values.meta);
  const resultB = await runStrategy(strategyB, values.parquet, values.meta);

  // Compare metrics
  const comparison = compareMetrics(resultA, resultB);
  const recommendation = generateRecommendation(comparison, resultA, resultB);

  // Output results
  if (values.output === 'json') {
    console.log(JSON.stringify({ resultA, resultB, comparison, recommendation }, null, 2));
  } else if (values.output === 'markdown') {
    printMarkdown(resultA, resultB, comparison, recommendation);
  } else {
    printComparison(resultA, resultB, comparison, recommendation);
  }
}

main().catch(err => {
  console.error(`\nFATAL ERROR: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
