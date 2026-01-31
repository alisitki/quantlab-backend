#!/usr/bin/env node
/**
 * analyzeTwinPair.js — deterministic twin pair analysis (shadow vs off).
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TwinPairIndexer } from '../runtime/TwinPairIndexer.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const PAIRS_INDEX = path.join(RUNS_DIR, 'pairs', 'index.json');
const OBS_DIR = path.join(RUNS_DIR, 'obs');
const ANALYSIS_DIR = path.join(RUNS_DIR, 'analysis');

const ROUND_SCALE = 1e6;
const VERDICT_THRESHOLDS = {
  EDGE_VAR: {
    confidence_mean_delta: 0.05,
    histogram_l1: 50,
    calibration_avg_proba_l1: 0.5,
    calibration_win_rate_l1: 0.5
  },
  EDGE_ZAYIF: {
    confidence_mean_delta: 0.01,
    histogram_l1: 10,
    calibration_avg_proba_l1: 0.1,
    calibration_win_rate_l1: 0.1
  }
};

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith('--')) continue;
    const name = key.slice(2);
    const value = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true;
    args[name] = value;
    if (value !== true) i++;
  }
  return args;
}

async function readJson(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return num;
}

function round(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.round(value * ROUND_SCALE) / ROUND_SCALE;
}

function l1Distance(a = [], b = []) {
  const len = Math.max(a.length, b.length);
  let sum = 0;
  for (let i = 0; i < len; i++) {
    sum += Math.abs(toNumber(a[i], 0) - toNumber(b[i], 0));
  }
  return round(sum);
}

function calibrationDrift(a = [], b = []) {
  const len = Math.max(a.length, b.length);
  let avgProba = 0;
  let winRate = 0;
  for (let i = 0; i < len; i++) {
    const aBin = a[i] || {};
    const bBin = b[i] || {};
    avgProba += Math.abs(toNumber(aBin.avg_proba, 0) - toNumber(bBin.avg_proba, 0));
    winRate += Math.abs(toNumber(aBin.win_rate, 0) - toNumber(bBin.win_rate, 0));
  }
  return {
    calibration_avg_proba_l1: round(avgProba),
    calibration_win_rate_l1: round(winRate)
  };
}

function evaluateVerdict(metrics) {
  const reasons = [];
  const cDelta = Math.abs(toNumber(metrics.confidence_mean_delta, 0));
  const histL1 = toNumber(metrics.proba_histogram_l1, 0)
    + toNumber(metrics.confidence_histogram_l1, 0);
  const calAvg = toNumber(metrics.calibration_avg_proba_l1, 0);
  const calWin = toNumber(metrics.calibration_win_rate_l1, 0);

  if (cDelta >= VERDICT_THRESHOLDS.EDGE_VAR.confidence_mean_delta) {
    reasons.push(`confidence_mean_delta>=${VERDICT_THRESHOLDS.EDGE_VAR.confidence_mean_delta}`);
  }
  if (histL1 >= VERDICT_THRESHOLDS.EDGE_VAR.histogram_l1) {
    reasons.push(`histogram_l1>=${VERDICT_THRESHOLDS.EDGE_VAR.histogram_l1}`);
  }
  if (calAvg >= VERDICT_THRESHOLDS.EDGE_VAR.calibration_avg_proba_l1) {
    reasons.push(`calibration_avg_proba_l1>=${VERDICT_THRESHOLDS.EDGE_VAR.calibration_avg_proba_l1}`);
  }
  if (calWin >= VERDICT_THRESHOLDS.EDGE_VAR.calibration_win_rate_l1) {
    reasons.push(`calibration_win_rate_l1>=${VERDICT_THRESHOLDS.EDGE_VAR.calibration_win_rate_l1}`);
  }

  if (reasons.length > 0) {
    return { verdict: 'EDGE_VAR', verdict_reason: reasons };
  }

  const weakReasons = [];
  if (cDelta >= VERDICT_THRESHOLDS.EDGE_ZAYIF.confidence_mean_delta) {
    weakReasons.push(`confidence_mean_delta>=${VERDICT_THRESHOLDS.EDGE_ZAYIF.confidence_mean_delta}`);
  }
  if (histL1 >= VERDICT_THRESHOLDS.EDGE_ZAYIF.histogram_l1) {
    weakReasons.push(`histogram_l1>=${VERDICT_THRESHOLDS.EDGE_ZAYIF.histogram_l1}`);
  }
  if (calAvg >= VERDICT_THRESHOLDS.EDGE_ZAYIF.calibration_avg_proba_l1) {
    weakReasons.push(`calibration_avg_proba_l1>=${VERDICT_THRESHOLDS.EDGE_ZAYIF.calibration_avg_proba_l1}`);
  }
  if (calWin >= VERDICT_THRESHOLDS.EDGE_ZAYIF.calibration_win_rate_l1) {
    weakReasons.push(`calibration_win_rate_l1>=${VERDICT_THRESHOLDS.EDGE_ZAYIF.calibration_win_rate_l1}`);
  }

  if (weakReasons.length > 0) {
    return { verdict: 'EDGE_ZAYIF', verdict_reason: weakReasons };
  }

  return { verdict: 'EDGE_YOK', verdict_reason: [] };
}

function selectPair(pairs, args) {
  if (!Array.isArray(pairs) || pairs.length === 0) return null;
  if (args.pair_id) {
    return pairs.find((p) => p?.pair_id === args.pair_id) || null;
  }
  if (args.run_id) {
    return pairs.find((p) => p?.run_off === args.run_id || p?.run_shadow === args.run_id) || null;
  }
  if (args.seed) {
    const matches = pairs.filter((p) => p?.strategy_seed === args.seed);
    if (!matches.length) return null;
    matches.sort((a, b) => a.pair_id.localeCompare(b.pair_id));
    return matches[0];
  }
  return null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.pair_id && !args.run_id && !args.seed) {
    console.error('Usage: node analyzeTwinPair.js --pair_id <id> | --run_id <id> | --seed <seed>');
    process.exit(1);
  }

  await fs.mkdir(ANALYSIS_DIR, { recursive: true });
  const indexer = new TwinPairIndexer();
  await indexer.rebuild();

  const pairs = await readJson(PAIRS_INDEX);
  const pair = selectPair(pairs, args);
  if (!pair) {
    console.error('[TwinPairAnalysis] action=error error=PAIR_NOT_FOUND');
    process.exit(1);
  }

  const offRunId = pair.run_off;
  const shadowRunId = pair.run_shadow;

  const offObs = await readJson(path.join(OBS_DIR, `${offRunId}.json`));
  const shadowObs = await readJson(path.join(OBS_DIR, `${shadowRunId}.json`));
  if (!offObs || !shadowObs) {
    console.error('[TwinPairAnalysis] action=error error=OBS_MISSING');
    process.exit(1);
  }

  const offManifest = await readJson(path.join(RUNS_DIR, `${offRunId}.json`));
  const shadowManifest = await readJson(path.join(RUNS_DIR, `${shadowRunId}.json`));

  const confidenceMeanDelta = round(
    toNumber(shadowObs.confidence_mean, 0) - toNumber(offObs.confidence_mean, 0)
  );

  const metrics = {
    confidence_mean_delta: confidenceMeanDelta,
    proba_histogram_l1: l1Distance(offObs.proba_histogram, shadowObs.proba_histogram),
    confidence_histogram_l1: l1Distance(offObs.confidence_histogram, shadowObs.confidence_histogram),
    ...calibrationDrift(offObs.calibration_table, shadowObs.calibration_table)
  };
  const verdict = evaluateVerdict(metrics);

  const analysis = {
    pair_id: pair.pair_id,
    runs: {
      off: {
        run_id: offRunId,
        ml_mode: offManifest?.extra?.ml?.mode || 'off',
        obs_path: path.join('runs', 'obs', `${offRunId}.json`)
      },
      shadow: {
        run_id: shadowRunId,
        ml_mode: shadowManifest?.extra?.ml?.mode || 'shadow',
        obs_path: path.join('runs', 'obs', `${shadowRunId}.json`)
      }
    },
    metrics,
    verdict: verdict.verdict,
    verdict_reason: verdict.verdict_reason
  };

  const outPath = path.join(ANALYSIS_DIR, `${pair.pair_id}.json`);
  await fs.writeFile(outPath, JSON.stringify(analysis, null, 2));
  console.log(`[TwinPairAnalysis] action=analysis_written pair_id=${pair.pair_id} path=${outPath}`);
}

main().catch((err) => {
  console.error(`[TwinPairAnalysis] action=error error=${err.message}`);
  process.exit(1);
});
