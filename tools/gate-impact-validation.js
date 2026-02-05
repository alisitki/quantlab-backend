#!/usr/bin/env node
/**
 * Decision Gate Impact Validation
 *
 * Runs A/B backtests to measure the real effect of the Decision Gating Layer.
 * Tests 3 scenarios on the SAME dataset:
 *   1. Gate OFF (baseline)
 *   2. Gate ON - Default preset
 *   3. Gate ON - Quality preset
 *
 * Usage:
 *   node tools/gate-impact-validation.js <s3_parquet> <s3_meta>
 *
 * Example:
 *   node tools/gate-impact-validation.js \
 *     "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20260203/*.parquet" \
 *     "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20260203/meta.json"
 */

import crypto from 'crypto';
import { ReplayEngine } from '../core/replay/index.js';
import { runReplayWithStrategy } from '../core/strategy/Runner.js';
import { ExecutionEngine } from '../core/execution/index.js';
import { buildBacktestSummary } from '../core/backtest/summary.js';
import { StrategyV1 } from '../core/strategy/v1/StrategyV1.js';
import { DEFAULT_CONFIG } from '../core/strategy/v1/config.js';

/**
 * Test scenarios
 */
const SCENARIOS = [
  {
    name: 'Gate OFF (Baseline)',
    config: {
      ...DEFAULT_CONFIG,
      gate: { enabled: false },
      featureReportPath: './reports/feature_analysis_full_2026-02-05.json'
    }
  },
  {
    name: 'Gate ON - Default',
    config: {
      ...DEFAULT_CONFIG,
      gate: {
        enabled: true,
        minSignalScore: 0.6,
        cooldownMs: 5000,
        regimeTrendMin: -0.5,
        regimeVolMin: 0,
        regimeSpreadMax: 2,
        maxSpreadNormalized: 0.001,
        logBlockedTrades: true
      },
      featureReportPath: './reports/feature_analysis_full_2026-02-05.json'
    }
  },
  {
    name: 'Gate ON - Quality',
    config: {
      ...DEFAULT_CONFIG,
      gate: {
        enabled: true,
        minSignalScore: 0.75,
        cooldownMs: 10000,
        regimeTrendMin: -0.3,
        regimeVolMin: 0,
        regimeSpreadMax: 1,
        maxSpreadNormalized: 0.0005,
        logBlockedTrades: true
      },
      featureReportPath: './reports/feature_analysis_full_2026-02-05.json'
    }
  }
];

/**
 * Run a single backtest scenario
 */
async function runScenario(parquetPath, metaPath, scenario, scenarioIndex) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`SCENARIO ${scenarioIndex + 1}/3: ${scenario.name}`);
  console.log('='.repeat(80));

  if (scenario.config.gate.enabled) {
    console.log('Gate Config:');
    console.log(`  - minSignalScore: ${scenario.config.gate.minSignalScore}`);
    console.log(`  - cooldownMs: ${scenario.config.gate.cooldownMs}`);
    console.log(`  - regimeTrendMin: ${scenario.config.gate.regimeTrendMin}`);
    console.log(`  - maxSpreadNormalized: ${scenario.config.gate.maxSpreadNormalized}`);
  } else {
    console.log('Gate: DISABLED');
  }

  // Initialize engines
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const executionEngine = new ExecutionEngine({ initialCapital: 10000 });
  const strategy = new StrategyV1(scenario.config);

  try {
    // Run backtest
    console.log('\nRunning backtest...');
    const startTime = Date.now();

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

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);

    // Get state and metrics
    const state = executionEngine.snapshot();
    const strategyState = strategy.getState();
    const summary = buildBacktestSummary(state, { initialCapital: 10000 });

    // Calculate additional metrics
    const totalTrades = state.fills.length;
    const winningTrades = state.fills.filter(f => f.realizedPnl > 0).length;
    const winRate = totalTrades > 0 ? (winningTrades / totalTrades * 100) : 0;
    const avgTradePnl = totalTrades > 0 ? (state.totalRealizedPnl / totalTrades) : 0;

    // Estimate spread paid (approximate)
    const totalSpreadPaid = state.fills.reduce((sum, f) => {
      // Assume 0.02% spread on average
      return sum + (f.qty * f.fillPrice * 0.0002);
    }, 0);

    // Average holding time (if we have trade pairs)
    let avgHoldingTime = 0;
    if (totalTrades > 1) {
      const timeDiffs = [];
      for (let i = 1; i < state.fills.length; i++) {
        const diff = state.fills[i].ts - state.fills[i - 1].ts;
        if (diff > 0) timeDiffs.push(diff);
      }
      if (timeDiffs.length > 0) {
        avgHoldingTime = timeDiffs.reduce((a, b) => a + b, 0) / timeDiffs.length;
      }
    }

    // Print immediate summary
    console.log('\n--- Results ---');
    console.log(`Duration: ${duration}s`);
    console.log(`Events processed: ${result.stats.processed.toLocaleString()}`);
    console.log(`Total trades: ${totalTrades.toLocaleString()}`);
    console.log(`Win rate: ${winRate.toFixed(1)}%`);
    console.log(`Avg trade PnL: $${avgTradePnl.toFixed(4)}`);
    console.log(`Total return: ${summary.return_pct.toFixed(2)}%`);
    console.log(`Max drawdown: ${summary.max_drawdown_pct.toFixed(2)}%`);
    console.log(`Sharpe ratio: ${summary.sharpe_ratio?.toFixed(2) || 'N/A'}`);
    console.log(`Final equity: $${state.equity.toFixed(2)}`);

    if (strategyState.gateStats) {
      console.log('\n--- Gate Statistics ---');
      console.log(`Total evaluations: ${strategyState.gateStats.total.toLocaleString()}`);
      console.log(`Passed: ${strategyState.gateStats.passed.toLocaleString()} (${(parseFloat(strategyState.gateStats.passRate) * 100).toFixed(1)}%)`);
      console.log(`Blocked: ${strategyState.gateStats.blocked.toLocaleString()}`);

      if (Object.keys(strategyState.gateStats.blockReasons).length > 0) {
        console.log('\nTop block reasons:');
        const reasons = Object.entries(strategyState.gateStats.blockReasons)
          .sort((a, b) => b[1] - a[1])
          .slice(0, 5);
        reasons.forEach(([reason, count]) => {
          const pct = (count / strategyState.gateStats.blocked * 100).toFixed(1);
          console.log(`  - ${reason}: ${count.toLocaleString()} (${pct}%)`);
        });
      }
    }

    // Return metrics for comparison
    return {
      scenario: scenario.name,
      metrics: {
        total_trades: totalTrades,
        win_rate: winRate,
        avg_trade_pnl: avgTradePnl,
        total_return: summary.return_pct,
        max_drawdown: summary.max_drawdown_pct,
        sharpe_ratio: summary.sharpe_ratio || 0,
        avg_holding_time_ms: avgHoldingTime,
        spread_paid_estimate: totalSpreadPaid,
        final_equity: state.equity,
        events_processed: result.stats.processed
      },
      gateStats: strategyState.gateStats,
      duration
    };

  } finally {
    await replayEngine.close();
  }
}

/**
 * Print comparison table
 */
function printComparisonTable(results) {
  console.log('\n\n');
  console.log('='.repeat(100));
  console.log('DECISION GATE IMPACT - COMPARISON TABLE');
  console.log('='.repeat(100));

  // Table header
  const headers = [
    'Metric',
    'Gate OFF',
    'Gate Default',
    'Gate Quality',
    'Default vs OFF',
    'Quality vs OFF'
  ];

  const colWidths = [25, 15, 15, 15, 15, 15];

  // Print header
  let headerRow = '';
  headers.forEach((h, i) => {
    headerRow += h.padEnd(colWidths[i]);
  });
  console.log(headerRow);
  console.log('-'.repeat(100));

  // Metrics to compare
  const metrics = [
    { key: 'total_trades', label: 'Total Trades', format: v => v.toLocaleString() },
    { key: 'win_rate', label: 'Win Rate (%)', format: v => v.toFixed(1) },
    { key: 'avg_trade_pnl', label: 'Avg Trade PnL ($)', format: v => v.toFixed(4) },
    { key: 'total_return', label: 'Total Return (%)', format: v => v.toFixed(2) },
    { key: 'max_drawdown', label: 'Max Drawdown (%)', format: v => v.toFixed(2) },
    { key: 'sharpe_ratio', label: 'Sharpe Ratio', format: v => v.toFixed(2) },
    { key: 'avg_holding_time_ms', label: 'Avg Hold Time (s)', format: v => (v / 1000).toFixed(1) },
    { key: 'spread_paid_estimate', label: 'Spread Paid ($)', format: v => v.toFixed(2) },
    { key: 'final_equity', label: 'Final Equity ($)', format: v => v.toFixed(2) }
  ];

  const baseline = results[0].metrics;
  const defaultGate = results[1].metrics;
  const qualityGate = results[2].metrics;

  metrics.forEach(({ key, label, format }) => {
    const baseVal = baseline[key];
    const defVal = defaultGate[key];
    const qualVal = qualityGate[key];

    // Calculate changes
    let defChange = '';
    let qualChange = '';

    if (baseVal !== 0) {
      const defPct = ((defVal - baseVal) / Math.abs(baseVal) * 100);
      const qualPct = ((qualVal - baseVal) / Math.abs(baseVal) * 100);

      defChange = `${defPct >= 0 ? '+' : ''}${defPct.toFixed(1)}%`;
      qualChange = `${qualPct >= 0 ? '+' : ''}${qualPct.toFixed(1)}%`;
    }

    let row = '';
    row += label.padEnd(colWidths[0]);
    row += format(baseVal).padEnd(colWidths[1]);
    row += format(defVal).padEnd(colWidths[2]);
    row += format(qualVal).padEnd(colWidths[3]);
    row += defChange.padEnd(colWidths[4]);
    row += qualChange.padEnd(colWidths[5]);

    console.log(row);
  });

  console.log('-'.repeat(100));

  // Gate statistics row
  if (results[1].gateStats) {
    console.log('\nGate Pass Rates:');
    console.log(`  Default Gate: ${(parseFloat(results[1].gateStats.passRate) * 100).toFixed(1)}% (${results[1].gateStats.passed.toLocaleString()}/${results[1].gateStats.total.toLocaleString()})`);
    console.log(`  Quality Gate: ${(parseFloat(results[2].gateStats.passRate) * 100).toFixed(1)}% (${results[2].gateStats.passed.toLocaleString()}/${results[2].gateStats.total.toLocaleString()})`);
  }

  console.log('='.repeat(100));
}

/**
 * Print analysis summary
 */
function printAnalysis(results) {
  console.log('\n\n');
  console.log('='.repeat(100));
  console.log('ANALYSIS SUMMARY');
  console.log('='.repeat(100));

  const baseline = results[0].metrics;
  const defaultGate = results[1].metrics;
  const qualityGate = results[2].metrics;

  // Trade reduction
  const defTradeReduction = ((baseline.total_trades - defaultGate.total_trades) / baseline.total_trades * 100);
  const qualTradeReduction = ((baseline.total_trades - qualityGate.total_trades) / baseline.total_trades * 100);

  console.log('\n1. TRADE FREQUENCY IMPACT');
  console.log(`   Default Gate: ${defTradeReduction.toFixed(1)}% reduction (${baseline.total_trades.toLocaleString()} → ${defaultGate.total_trades.toLocaleString()})`);
  console.log(`   Quality Gate: ${qualTradeReduction.toFixed(1)}% reduction (${baseline.total_trades.toLocaleString()} → ${qualityGate.total_trades.toLocaleString()})`);

  // Signal quality improvement
  console.log('\n2. SIGNAL QUALITY IMPROVEMENT');
  const defWinRateChange = defaultGate.win_rate - baseline.win_rate;
  const qualWinRateChange = qualityGate.win_rate - baseline.win_rate;
  console.log(`   Win Rate Change:`);
  console.log(`     - Default Gate: ${defWinRateChange >= 0 ? '+' : ''}${defWinRateChange.toFixed(1)}pp (${baseline.win_rate.toFixed(1)}% → ${defaultGate.win_rate.toFixed(1)}%)`);
  console.log(`     - Quality Gate: ${qualWinRateChange >= 0 ? '+' : ''}${qualWinRateChange.toFixed(1)}pp (${baseline.win_rate.toFixed(1)}% → ${qualityGate.win_rate.toFixed(1)}%)`);

  const defAvgPnlChange = ((defaultGate.avg_trade_pnl - baseline.avg_trade_pnl) / Math.abs(baseline.avg_trade_pnl) * 100);
  const qualAvgPnlChange = ((qualityGate.avg_trade_pnl - baseline.avg_trade_pnl) / Math.abs(baseline.avg_trade_pnl) * 100);
  console.log(`   Avg Trade PnL Change:`);
  console.log(`     - Default Gate: ${defAvgPnlChange >= 0 ? '+' : ''}${defAvgPnlChange.toFixed(1)}%`);
  console.log(`     - Quality Gate: ${qualAvgPnlChange >= 0 ? '+' : ''}${qualAvgPnlChange.toFixed(1)}%`);

  // Overall performance
  console.log('\n3. OVERALL PERFORMANCE');
  const defReturnChange = defaultGate.total_return - baseline.total_return;
  const qualReturnChange = qualityGate.total_return - baseline.total_return;
  console.log(`   Total Return Change:`);
  console.log(`     - Default Gate: ${defReturnChange >= 0 ? '+' : ''}${defReturnChange.toFixed(2)}pp`);
  console.log(`     - Quality Gate: ${qualReturnChange >= 0 ? '+' : ''}${qualReturnChange.toFixed(2)}pp`);

  console.log(`   Max Drawdown:`);
  console.log(`     - Baseline: ${baseline.max_drawdown.toFixed(2)}%`);
  console.log(`     - Default Gate: ${defaultGate.max_drawdown.toFixed(2)}%`);
  console.log(`     - Quality Gate: ${qualityGate.max_drawdown.toFixed(2)}%`);

  // Recommendation
  console.log('\n4. RECOMMENDATION');

  let bestScenario = 'Gate OFF';
  let bestReturn = baseline.total_return;

  if (defaultGate.total_return > bestReturn) {
    bestScenario = 'Gate Default';
    bestReturn = defaultGate.total_return;
  }

  if (qualityGate.total_return > bestReturn) {
    bestScenario = 'Gate Quality';
    bestReturn = qualityGate.total_return;
  }

  console.log(`   Best performing config: ${bestScenario}`);
  console.log(`   Return: ${bestReturn.toFixed(2)}%`);

  if (defaultGate.total_return > baseline.total_return && defaultGate.win_rate > baseline.win_rate) {
    console.log(`\n   ✅ DEFAULT GATE IMPROVES BOTH RETURN AND WIN RATE`);
  } else if (qualityGate.total_return > baseline.total_return && qualityGate.win_rate > baseline.win_rate) {
    console.log(`\n   ✅ QUALITY GATE IMPROVES BOTH RETURN AND WIN RATE`);
  } else {
    console.log(`\n   ⚠️  Gate reduces trades but does not improve returns`);
  }

  console.log('='.repeat(100));
}

/**
 * Main entry point
 */
async function main() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.error('Usage: node gate-impact-validation.js <s3_parquet> <s3_meta>');
    console.error('\nExample:');
    console.error('  node tools/gate-impact-validation.js \\');
    console.error('    "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20260203/*.parquet" \\');
    console.error('    "s3://quantlab-compact/exchange=binance/stream=bbo/symbol=adausdt/date=20260203/meta.json"');
    process.exit(1);
  }

  const parquetPath = args[0];
  const metaPath = args[1];

  console.log('='.repeat(100));
  console.log('DECISION GATE IMPACT VALIDATION');
  console.log('='.repeat(100));
  console.log(`Dataset: ${parquetPath}`);
  console.log(`Running ${SCENARIOS.length} scenarios...`);
  console.log('='.repeat(100));

  const results = [];

  // Run all scenarios
  for (let i = 0; i < SCENARIOS.length; i++) {
    try {
      const result = await runScenario(parquetPath, metaPath, SCENARIOS[i], i);
      results.push(result);
    } catch (error) {
      console.error(`\n❌ Scenario ${i + 1} failed:`, error.message);
      console.error(error.stack);
      process.exit(1);
    }
  }

  // Print comparison table
  printComparisonTable(results);

  // Print analysis
  printAnalysis(results);

  console.log('\n✅ Validation complete!\n');
}

main().catch(err => {
  console.error(`\n❌ FATAL ERROR: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
