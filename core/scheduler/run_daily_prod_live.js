#!/usr/bin/env node
/**
 * run_daily_prod_live.js: Thin wrapper for LIVE mode.
 * Calls unified orchestrator with --mode live.
 */
import { execSync } from 'child_process';

const args = process.argv.slice(2).join(' ');
const cmd = `node scheduler/run_daily_prod.js --mode live ${args}`;

console.log(`[Wrapper] Executing: ${cmd}`);
try {
  execSync(cmd, { stdio: 'inherit', cwd: process.cwd() });
} catch (err) {
  process.exit(err.status || 1);
}
