#!/usr/bin/env node
/**
 * run_signal_dry.js: Dry-run CLI to test signal generation with decision.json
 * 
 * Usage:
 *   node ml/decision/run_signal_dry.js --symbol btcusdt --date 20251229 --limit 1000
 */
import 'dotenv/config';

import { loadDecision } from './DecisionLoader.js';
import { applyDecision, getProbaStats } from './applyDecision.js';
import duckdb from 'duckdb';

// Parse CLI arguments
function parseArgs(args) {
  const result = {
    symbol: 'btcusdt',
    date: null,
    limit: 1000
  };
  
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--symbol':
        result.symbol = args[++i];
        break;
      case '--date':
        result.date = args[++i];
        break;
      case '--limit':
        result.limit = parseInt(args[++i], 10);
        break;
    }
  }
  
  return result;
}

// Load features from S3 parquet
async function loadFeatures(symbol, date, limit) {
  const featurePath = `s3://quantlab-compact/features/featureset=v1/exchange=binance/stream=bbo/symbol=${symbol}/date=${date}/data.parquet`;
  
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(':memory:');
    const conn = db.connect();
    
    const endpoint = (process.env.S3_COMPACT_ENDPOINT || '').replace('https://', '');
    const accessKey = process.env.S3_COMPACT_ACCESS_KEY;
    const secretKey = process.env.S3_COMPACT_SECRET_KEY;
    const region = process.env.S3_COMPACT_REGION || 'us-east-1';
    
    const setupQueries = [
      'INSTALL httpfs', 'LOAD httpfs',
      `SET s3_endpoint='${endpoint}'`,
      `SET s3_access_key_id='${accessKey}'`,
      `SET s3_secret_access_key='${secretKey}'`,
      `SET s3_region='${region}'`,
      "SET s3_url_style='path'",
      'SET s3_use_ssl=true'
    ];
    
    let completed = 0;
    for (const q of setupQueries) {
      conn.run(q, () => {
        completed++;
        if (completed === setupQueries.length) {
          runQuery();
        }
      });
    }
    
    function runQuery() {
      const sql = `SELECT * FROM read_parquet('${featurePath}') ORDER BY ts_event ASC LIMIT ${limit}`;
      
      conn.all(sql, (err, rows) => {
        db.close();
        
        if (err) {
          return reject(new Error(`PARQUET_QUERY_FAILED: ${err.message}`));
        }
        
        if (rows.length === 0) {
          return reject(new Error(`No data found: ${featurePath}`));
        }
        
        // Extract feature columns
        const featureNames = Object.keys(rows[0]).filter(k => k.startsWith('f_'));
        const X = rows.map(row => featureNames.map(name => Number(row[name])));
        
        resolve({ X, featureNames, rowCount: rows.length });
      });
    }
  });
}

// Generate pseudo-proba (same logic as XGBoostModel)
function generatePseudoProba(X) {
  // Small-scale feature indices (skip f_mid=0, f_microprice=4)
  const smallScaleIndices = [1, 2, 3, 5, 6, 7, 8, 9];
  
  return X.map(row => {
    let sum = 0;
    for (const i of smallScaleIndices) {
      if (i < row.length) sum += row[i];
    }
    const scale = 10;
    const score = Math.max(-5, Math.min(5, sum / scale));
    return 1 / (1 + Math.exp(-score));
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  
  console.log('='.repeat(60));
  console.log('Signal Dry-Run Tool v1');
  console.log('='.repeat(60));
  console.log(`Symbol: ${args.symbol}`);
  console.log(`Date: ${args.date}`);
  console.log(`Limit: ${args.limit}`);
  console.log('');
  
  if (!args.date) {
    console.error('Error: --date is required');
    process.exit(1);
  }
  
  // 1. Load decision config
  console.log('--- Loading Decision Config ---');
  const decision = await loadDecision(args.symbol);
  
  const decisionLoaded = !decision._fallback;
  console.log(`Decision loaded: ${decisionLoaded ? 'YES (from S3)' : 'NO (using fallback)'}`);
  console.log(`Threshold: ${decision.bestThreshold}`);
  console.log(`Proba Source: ${decision.probaSource}`);
  console.log(`Job ID: ${decision.jobId || 'N/A'}`);
  console.log('');
  
  // 2. Load features
  console.log('--- Loading Features ---');
  const { X, featureNames, rowCount } = await loadFeatures(args.symbol, args.date, args.limit);
  console.log(`Loaded ${rowCount} rows`);
  console.log(`Features: ${featureNames.join(', ')}`);
  console.log('');
  
  // 3. Generate probabilities
  console.log('--- Generating Probabilities ---');
  const probas = generatePseudoProba(X);
  const probaStats = getProbaStats(probas);
  console.log(`Proba Stats: min=${probaStats.min.toFixed(4)}, mean=${probaStats.mean.toFixed(4)}, max=${probaStats.max.toFixed(4)}`);
  console.log('');
  
  // 4. Apply threshold
  console.log('--- Applying Threshold ---');
  const result = applyDecision(probas, decision);
  console.log(`Threshold Used: ${result.thresholdUsed}`);
  console.log(`Proba Source: ${result.probaSource}`);
  console.log(`Pred Pos Count: ${result.pred_pos_count}`);
  console.log(`Pred Pos Rate: ${(result.pred_pos_rate * 100).toFixed(2)}%`);
  console.log('');
  
  // 5. Summary
  console.log('='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  console.log(`Decision Config: ${decisionLoaded ? 'LOADED' : 'FALLBACK'}`);
  console.log(`Threshold: ${result.thresholdUsed}`);
  console.log(`Signals Generated: ${result.pred_pos_count}/${rowCount} (${(result.pred_pos_rate * 100).toFixed(2)}%)`);
  console.log('='.repeat(60));
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
