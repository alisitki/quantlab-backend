#!/usr/bin/env node
/**
 * ML Cost Report Script
 *
 * Generates GPU cost reports for ML training jobs.
 *
 * Usage:
 *   node core/scheduler/report_ml_costs.js [options]
 *
 * Options:
 *   --period <7d|24h|30d>  Time period (default: 7d)
 *   --symbol <symbol>      Filter by symbol (e.g., btcusdt)
 *   --json                 Output as JSON
 *   --budget <amount>      Set budget to check against
 */
import 'dotenv/config';
import { getCostCalculator } from '../vast/CostCalculator.js';
import { getCostWriter } from '../vast/CostWriter.js';
import { createVastClient } from '../vast/VastClient.js';

async function main() {
  const args = process.argv.slice(2);
  const options = {
    period: '7d',
    symbol: null,
    json: false,
    budget: null
  };

  // Parse arguments
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--period':
        options.period = args[++i];
        break;
      case '--symbol':
        options.symbol = args[++i];
        break;
      case '--json':
        options.json = true;
        break;
      case '--budget':
        options.budget = parseFloat(args[++i]);
        break;
      case '--help':
        console.log(`
ML Cost Report Script

Usage:
  node core/scheduler/report_ml_costs.js [options]

Options:
  --period <7d|24h|30d>  Time period (default: 7d)
  --symbol <symbol>      Filter by symbol (e.g., btcusdt)
  --json                 Output as JSON
  --budget <amount>      Set budget to check against
  --help                 Show this help
`);
        process.exit(0);
    }
  }

  const calculator = getCostCalculator();
  const writer = getCostWriter();

  console.log(`\n=== ML GPU Cost Report (${options.period}) ===\n`);

  // Try local log first (faster)
  let summary;
  try {
    summary = await writer.getLocalSummary(options.period);
    console.log('[Source: Local log]\n');
  } catch {
    // Fall back to S3 aggregation
    console.log('[Source: S3 aggregation]\n');
    summary = await calculator.aggregateByPeriod(options.period);
    summary = summary.summary;
  }

  // Get account balance if possible
  let accountBalance = null;
  try {
    const vast = createVastClient();
    const account = await vast.getAccountInfo();
    accountBalance = account.balance || account.credit;
  } catch {
    // API key may not be set
  }

  // Prepare report data
  const report = {
    period: options.period,
    generatedAt: new Date().toISOString(),
    summary: {
      totalJobs: summary.totalJobs,
      totalCost: summary.totalCost,
      avgCostPerJob: summary.avgCostPerJob,
      totalRuntimeMs: summary.totalRuntimeMs,
      avgRuntimePerJob: summary.totalJobs > 0
        ? Math.round(summary.totalRuntimeMs / summary.totalJobs)
        : 0
    },
    bySymbol: summary.bySymbol || {},
    accountBalance,
    budget: null
  };

  // Budget check
  if (options.budget) {
    report.budget = calculator.checkBudget(summary.totalCost, options.budget);
  }

  // Output
  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    printReport(report);
  }
}

function printReport(report) {
  const { summary, bySymbol, accountBalance, budget } = report;

  // Summary
  console.log('SUMMARY');
  console.log('─'.repeat(40));
  console.log(`  Total Jobs:        ${summary.totalJobs}`);
  console.log(`  Total Cost:        $${summary.totalCost.toFixed(4)}`);
  console.log(`  Avg Cost/Job:      $${summary.avgCostPerJob.toFixed(4)}`);
  console.log(`  Total Runtime:     ${formatMs(summary.totalRuntimeMs)}`);
  console.log(`  Avg Runtime/Job:   ${formatMs(summary.avgRuntimePerJob)}`);

  // By symbol
  if (Object.keys(bySymbol).length > 0) {
    console.log('\nBY SYMBOL');
    console.log('─'.repeat(40));
    const sorted = Object.entries(bySymbol).sort((a, b) => b[1] - a[1]);
    for (const [sym, cost] of sorted) {
      const pct = summary.totalCost > 0 ? (cost / summary.totalCost * 100).toFixed(1) : 0;
      console.log(`  ${sym.padEnd(12)} $${cost.toFixed(4).padStart(8)}  (${pct}%)`);
    }
  }

  // Account balance
  if (accountBalance !== null) {
    console.log('\nACCOUNT');
    console.log('─'.repeat(40));
    console.log(`  Balance:           $${accountBalance.toFixed(2)}`);
  }

  // Budget
  if (budget) {
    console.log('\nBUDGET');
    console.log('─'.repeat(40));
    console.log(`  Budget:            $${budget.budget.toFixed(2)}`);
    console.log(`  Spent:             $${budget.spent.toFixed(4)}`);
    console.log(`  Remaining:         $${budget.remaining.toFixed(4)}`);
    console.log(`  Usage:             ${budget.percentUsed.toFixed(1)}%`);
    console.log(`  Status:            ${budget.status.toUpperCase()}`);
  }

  console.log('\n' + '═'.repeat(40));
  console.log(`Generated at: ${report.generatedAt}`);
}

function formatMs(ms) {
  if (!ms) return '0s';
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) {
    return `${hours}h ${minutes % 60}m`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  }
  return `${seconds}s`;
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
