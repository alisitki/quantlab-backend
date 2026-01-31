#!/usr/bin/env node
/**
 * verify-validation-pipeline.js — Validation pipeline checks
 *
 * Usage:
 *   node verify-validation-pipeline.js --fail_params '{"strategy":"ema_cross","symbol":"BTCUSDT","date":"2024-01-15"}' \
 *     --pass_params '{"strategy":"ema_cross","symbol":"BTCUSDT","date":"2024-01-15"}'
 */

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { mkdir } from 'node:fs/promises';
import { run as runOrchestrator } from './EvalOrchestrator.js';
import { getValidationMetrics } from './validationMetrics.js';
import { runEval } from './eval-run.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_REPORTS_DIR = path.resolve(__dirname, './reports');

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

function parseJsonParam(value, fallback = {}) {
  if (!value) return fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(`VERIFY_FAIL: ${message}`);
  }
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const failParams = parseJsonParam(args.fail_params, {});
  const passParams = parseJsonParam(args.pass_params, {});

  console.log('--- A) Snapshot run should NOT trigger validation ---');
  const failRunId = `verify_fail_${Date.now()}`;
  const failResult = await runOrchestrator({
    ...failParams,
    run_id: failRunId
  });
  assert(failResult.validation_triggered === false, 'Expected validation_triggered=false for fail case');
  console.log('OK: validation_triggered=false');

  console.log('\n--- B) Snapshot run SHOULD trigger validation + produce validation_report ---');
  const passRunId = `verify_pass_${Date.now()}`;
  const passResult = await runOrchestrator({
    ...passParams,
    run_id: passRunId
  });
  assert(passResult.validation_triggered === true, 'Expected validation_triggered=true for pass case');
  assert(!!passResult.validation_report_path, 'Expected validation_report_path to exist');
  console.log(`OK: validation_report_path=${passResult.validation_report_path}`);

  console.log('\n--- C) Determinism: snapshot run twice => identical hashes ---');
  await mkdir(DEFAULT_REPORTS_DIR, { recursive: true });
  const detRunId1 = `verify_det_1_${Date.now()}`;
  const detRunId2 = `verify_det_2_${Date.now()}`;
  const report1 = await runEval({
    ...passParams,
    mode: 'snapshot',
    run_id: detRunId1,
    out: path.join(DEFAULT_REPORTS_DIR, `${detRunId1}.json`)
  });
  const report2 = await runEval({
    ...passParams,
    mode: 'snapshot',
    run_id: detRunId2,
    out: path.join(DEFAULT_REPORTS_DIR, `${detRunId2}.json`)
  });
  assert(report1.determinism.state_hash === report2.determinism.state_hash, 'state_hash mismatch');
  assert(report1.determinism.fills_hash === report2.determinism.fills_hash, 'fills_hash mismatch');
  console.log('OK: determinism hashes match');

  console.log('\n--- Metrics snapshot ---');
  console.log(getValidationMetrics());
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
