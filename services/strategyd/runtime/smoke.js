#!/usr/bin/env node
/**
 * smoke.js
 *
 * Runs determinism + overflow parity gates and exits non-zero on failure.
 */

import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const steps = [
  {
    name: 'compat',
    script: path.join(__dirname, 'runtime-v2-compat.js')
  },
  {
    name: 'overflow',
    script: path.join(__dirname, 'overflow-parity.js')
  }
];

function runStep(step) {
  const result = spawnSync('node', [step.script], {
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe']
  });

  const stdout = (result.stdout || Buffer.alloc(0)).toString().trim();
  const stderr = (result.stderr || Buffer.alloc(0)).toString().trim();

  if (stdout) {
    process.stdout.write(`${stdout}\n`);
  }
  if (stderr) {
    process.stderr.write(`${stderr}\n`);
  }

  return result.status === 0;
}

for (const step of steps) {
  const ok = runStep(step);
  if (!ok) {
    console.error(`[SMOKE] FAIL step=${step.name} script=${step.script}`);
    process.exit(1);
  }
}

console.log(`[SMOKE] OK compat=${steps[0].script} overflow=${steps[1].script}`);
