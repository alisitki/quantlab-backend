#!/usr/bin/env node
/**
 * eval-compare.js — Evaluation Report Comparison
 * 
 * Usage: node eval-compare.js report1.json report2.json
 */

import { readFile } from 'node:fs/promises';

const [file1, file2] = process.argv.slice(2);

if (!file1 || !file2) {
  console.error('Usage: node eval-compare.js <report1.json> <report2.json>');
  process.exit(1);
}

async function compare() {
  const r1 = JSON.parse(await readFile(file1, 'utf8'));
  const r2 = JSON.parse(await readFile(file2, 'utf8'));

  console.log('--- EVALUATION COMPARISON ---');
  console.log(`${'Metric'.padEnd(25)} | ${'Report 1'.padEnd(20)} | ${'Report 2'.padEnd(20)} | ${'Delta'}`);
  console.log('-'.repeat(80));

  const compareMetric = (path, label) => {
    const v1 = path.split('.').reduce((o, i) => o[i], r1);
    const v2 = path.split('.').reduce((o, i) => o[i], r2);
    const delta = typeof v1 === 'number' ? (v2 - v1).toFixed(8) : (v1 === v2 ? 'MATCH' : 'DIFF');
    console.log(`${label.padEnd(25)} | ${String(v1).padEnd(20)} | ${String(v2).padEnd(20)} | ${delta}`);
  };

  compareMetric('results.pnl_pct', 'PnL %');
  compareMetric('results.max_drawdown_pct', 'Max Drawdown %');
  compareMetric('results.win_rate', 'Win Rate');
  compareMetric('results.trades_count', 'Trades Count');
  compareMetric('results.avg_trade_pnl', 'Avg Trade PnL');
  compareMetric('results.equity_end', 'Equity End');
  compareMetric('determinism.state_hash', 'State Hash');
  compareMetric('determinism.fills_hash', 'Fills Hash');

  console.log('\n--- DETERMINISM CHECK ---');
  const detPass = r1.determinism.state_hash === r2.determinism.state_hash &&
                 r1.determinism.fills_hash === r2.determinism.fills_hash;
  
  if (detPass) {
    console.log('✅ DETERMINISM: Both runs are mathematically identical.');
  } else {
    console.log('❌ DIVERGENCE: Internal state or fills differ.');
  }
}

compare().catch(err => {
  console.error('Comparison failed:', err);
  process.exit(1);
});
