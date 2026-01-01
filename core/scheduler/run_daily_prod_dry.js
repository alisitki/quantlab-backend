#!/usr/bin/env node
/**
 * run_daily_prod_dry.js: Thin wrapper for DRY mode.
 * Calls unified orchestrator with --mode dry.
 * 
 * NOTE: This file is now a wrapper. The actual logic is in run_daily_prod.js.
 * Kept for backwards compatibility.
 */
import { execSync } from 'child_process';

const args = process.argv.slice(2).join(' ');
const cmd = `node scheduler/run_daily_prod.js --mode dry ${args}`;

console.log(`[Wrapper] Executing: ${cmd}`);
try {
  execSync(cmd, { stdio: 'inherit', cwd: process.cwd() });
} catch (err) {
  process.exit(err.status || 1);
}
