/**
 * EvalOrchestrator — Snapshot → Tick validation pipeline.
 */

import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { runEval } from './eval-run.js';
import { incrementValidationDiverged, incrementValidationTriggered } from './validationMetrics.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = path.resolve(__dirname, './reports');
const VALIDATION_DIR = path.resolve(__dirname, './validation_reports');

async function ensureDirs() {
  await mkdir(REPORTS_DIR, { recursive: true });
  await mkdir(VALIDATION_DIR, { recursive: true });
}

function validationKey(report) {
  const key = `${report.run_id}:${report.params_hash}:${report.input_fingerprint || 'none'}`;
  return createHash('sha256').update(key).digest('hex');
}

function validationPathFor(report) {
  const key = validationKey(report).slice(0, 16);
  return path.join(VALIDATION_DIR, `validation_report_${key}.json`);
}

export function shouldValidate(report) {
  const pnl_pct = report?.results?.pnl_pct ?? 0;
  const max_drawdown_pct = report?.results?.max_drawdown_pct ?? 0;
  const trades_count = report?.results?.trades_count ?? 0;
  return pnl_pct > 0 && max_drawdown_pct > -0.15 && trades_count > 20;
}

export function compareReports(snapshotReport, tickReport) {
  const pnl_diff = (tickReport.results.pnl_pct ?? 0) - (snapshotReport.results.pnl_pct ?? 0);
  const trade_count_diff = (tickReport.results.trades_count ?? 0) - (snapshotReport.results.trades_count ?? 0);
  const input_fingerprint_match = snapshotReport.input_fingerprint === tickReport.input_fingerprint;
  const state_hash_match = snapshotReport.determinism.state_hash === tickReport.determinism.state_hash;
  const fills_hash_match = snapshotReport.determinism.fills_hash === tickReport.determinism.fills_hash;

  return {
    snapshot: {
      pnl_pct: snapshotReport.results.pnl_pct,
      state_hash: snapshotReport.determinism.state_hash,
      fills_hash: snapshotReport.determinism.fills_hash,
      input_fingerprint: snapshotReport.input_fingerprint,
      data_mode: snapshotReport.data_mode
    },
    tick: {
      pnl_pct: tickReport.results.pnl_pct,
      state_hash: tickReport.determinism.state_hash,
      fills_hash: tickReport.determinism.fills_hash,
      input_fingerprint: tickReport.input_fingerprint,
      data_mode: tickReport.data_mode
    },
    deltas: {
      pnl_diff,
      trade_count_diff,
      input_fingerprint_match,
      state_hash_match,
      fills_hash_match
    }
  };
}

function determineValidationStatus(deltas) {
  const isMatch = deltas.input_fingerprint_match && deltas.state_hash_match && deltas.fills_hash_match;
  return isMatch ? 'passed' : 'diverged';
}

async function updateSnapshotReport(reportPath, validation_status, validation_report_path) {
  const raw = await readFile(reportPath, 'utf8');
  const report = JSON.parse(raw);
  report.validation_status = validation_status;
  report.artifacts = report.artifacts || {};
  report.artifacts.validation_report_path = validation_report_path || null;
  await writeFile(reportPath, JSON.stringify(report, null, 2));
  return report;
}

async function readJsonIfExists(filePath) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function run(params) {
  await ensureDirs();

  const baseRunId = params.run_id || `eval_${Date.now()}`;
  const snapshotOut = params.snapshot_out || path.join(REPORTS_DIR, `${baseRunId}_snapshot_report.json`);
  const tickOut = params.tick_out || path.join(REPORTS_DIR, `${baseRunId}_tick_report.json`);

  const snapshotReport = await runEval({
    ...params,
    mode: 'snapshot',
    run_id: baseRunId,
    out: snapshotOut
  });

  const validationAllowed = params.validate_with_tick !== false;
  const validation_triggered = validationAllowed ? shouldValidate(snapshotReport) : false;
  let validation_status = 'not_required';
  let validation_report_path = null;
  let pnl_diff = null;

  if (validation_triggered) {
    incrementValidationTriggered();
    validation_report_path = validationPathFor(snapshotReport);

    const existingReport = await readJsonIfExists(validation_report_path);
    let validationReport = existingReport;

    if (!validationReport) {
      const tickReport = await runEval({
        ...params,
        mode: 'tick',
        run_id: `${baseRunId}_tick`,
        out: tickOut
      });
      validationReport = compareReports(snapshotReport, tickReport);
      await writeFile(validation_report_path, JSON.stringify(validationReport, null, 2));
    }

    validation_status = determineValidationStatus(validationReport.deltas);
    pnl_diff = validationReport.deltas.pnl_diff;
    if (validation_status === 'diverged') {
      incrementValidationDiverged();
    }

    await updateSnapshotReport(snapshotOut, validation_status, validation_report_path);
  } else {
    await updateSnapshotReport(snapshotOut, validation_status, null);
  }

  console.log(`[ORCH] eval_run_id=${snapshotReport.run_id} mode=snapshot validation_triggered=${validation_triggered} validation_status=${validation_status} pnl_pct=${snapshotReport.results.pnl_pct} pnl_diff=${pnl_diff ?? 'n/a'}`);
  return {
    snapshot_report: snapshotOut,
    validation_triggered,
    validation_status,
    validation_report_path
  };
}
