#!/usr/bin/env node
/**
 * run_ops_report.js: Ops report orchestrator.
 * 
 * Runs health generator + digest generator for each date.
 * Prints TXT digest to stdout.
 * 
 * Usage:
 *   node scheduler/run_ops_report.js                    # yesterday
 *   node scheduler/run_ops_report.js --date 20251229    # single date  
 *   node scheduler/run_ops_report.js --last 3           # last 3 days
 * 
 * NEVER throws - always exits 0.
 * DETERMINISTIC output.
 */
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

function parseArgs(args) {
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--date') result.date = args[++i];
    if (args[i] === '--yesterday') result.yesterday = true;
    if (args[i] === '--last') result.last = parseInt(args[++i], 10);
  }
  return result;
}

function getYesterday() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().split('T')[0].replace(/-/g, '');
}

function getLastNDays(n) {
  const dates = [];
  for (let i = n; i >= 1; i--) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - i);
    dates.push(d.toISOString().split('T')[0].replace(/-/g, ''));
  }
  return dates;
}

function runForDate(date) {
  console.error(`\n[OpsRunner] Processing ${date}...`);
  
  // 1. Generate health
  try {
    execSync(`node scheduler/generate_daily_health.js --date ${date}`, { 
      stdio: ['pipe', 'pipe', 'inherit'] 
    });
  } catch (err) {
    console.error(`[OpsRunner] Health generation failed for ${date}: ${err.message}`);
  }
  
  // 2. Generate digest
  try {
    execSync(`node scheduler/generate_ops_digest.js --date ${date}`, { 
      stdio: ['pipe', 'pipe', 'inherit'] 
    });
  } catch (err) {
    console.error(`[OpsRunner] Digest generation failed for ${date}: ${err.message}`);
  }
  
  // 3. Print TXT digest to stdout
  const txtPath = path.join('ops', `digest_${date}.txt`);
  if (fs.existsSync(txtPath)) {
    console.log(fs.readFileSync(txtPath, 'utf8'));
  } else {
    console.error(`[OpsRunner] No digest found at ${txtPath}`);
  }
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  
  // Determine dates (precedence: --date > --last > --yesterday)
  let dates;
  if (args.date) {
    dates = [args.date];
  } else if (args.last && args.last > 0) {
    dates = getLastNDays(args.last);
  } else {
    dates = [getYesterday()];
  }
  
  console.error(`[OpsRunner] Generating reports for ${dates.length} date(s): ${dates.join(', ')}`);
  
  for (const date of dates) {
    runForDate(date);
  }
  
  console.error(`\n[OpsRunner] Done.`);
  process.exit(0);
}

main();
