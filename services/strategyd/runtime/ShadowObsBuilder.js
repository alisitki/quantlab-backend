/**
 * ShadowObsBuilder — deterministic ML shadow observability artifacts.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { XGBoostModel } from '../../../core/ml/models/XGBoostModel.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const OBS_DIR = path.join(RUNS_DIR, 'obs');
const OBS_INDEX = path.join(OBS_DIR, 'index.json');

const PROBA_BINS = 20;
const CONF_BINS = 20;
const CALIBRATION_BINS = 10;
const ROUND_SCALE = 1e6;

const require = createRequire(import.meta.url);
const parquet = require(path.resolve(__dirname, '../../../core/node_modules/parquetjs-lite'));

export class ShadowObsBuilder {
  async buildForRun({ runId, featureJobId, labelJobId, modelPath, decisionPath }) {
    try {
      await fs.mkdir(OBS_DIR, { recursive: true });
      const manifestPath = path.join(RUNS_DIR, `${runId}.json`);
      const manifest = await readJson(manifestPath);
      if (!manifest) {
        console.error(`[ShadowObsBuilder] run_id=${runId} action=skipped reason=missing_manifest`);
        return null;
      }

      const derived = await this.#computeObs({ runId, manifest, featureJobId, labelJobId, modelPath, decisionPath });
      const outPath = path.join(OBS_DIR, `${runId}.json`);
      await fs.writeFile(outPath, JSON.stringify(derived, null, 2));
      await this.#updateIndex(runId, derived, outPath);

      console.log(`[ShadowObsBuilder] run_id=${runId} action=obs_written path=${outPath}`);
      return derived;
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[ShadowObsBuilder] run_id=${runId} action=error error=${msg}`);
      return null;
    }
  }

  async buildAll({ featureJobId, labelJobId, modelPath, decisionPath }) {
    const files = await fs.readdir(RUNS_DIR);
    const manifests = files.filter((f) => f.endsWith('.json') && f !== 'index.json');
    manifests.sort();

    for (const file of manifests) {
      const runId = file.replace('.json', '');
      const manifest = await readJson(path.join(RUNS_DIR, file));
      if (!manifest?.ended_at || !manifest?.ended_reason) continue;
      await this.buildForRun({ runId, featureJobId, labelJobId, modelPath, decisionPath });
    }
  }

  async #computeObs({ runId, manifest, featureJobId, labelJobId, modelPath, decisionPath }) {
    const base = {
      run_id: runId,
      count_total: 0,
      count_scored: 0,
      proba_histogram: initBins(PROBA_BINS),
      confidence_histogram: initBins(CONF_BINS),
      calibration_table: initCalibration(CALIBRATION_BINS),
      confidence_mean: null,
      confidence_std: null,
      top_k_high_conf: [],
      label_summary: { neg: 0, zero: 0, pos: 0 },
      score_vs_label: { neg: null, zero: null, pos: null }
    };

    const extraMl = manifest?.extra?.ml;
    const mlEnabled = Boolean(extraMl) && extraMl?.mode !== 'off';
    const canBatchScore = mlEnabled && featureJobId && labelJobId && modelPath && decisionPath;

    if (!mlEnabled) {
      base.confidence_mean = 0;
      base.confidence_std = 0;
      return base;
    }

    if (!canBatchScore) {
      const proba = toNumber(extraMl?.proba);
      if (proba === null) {
        base.confidence_mean = 0;
        base.confidence_std = 0;
        return base;
      }
      const conf = toNumber(extraMl?.confidence);
      base.count_total = 1;
      base.count_scored = 1;
      incrementBin(base.proba_histogram, proba);
      incrementBin(base.confidence_histogram, conf ?? 0);
      updateCalibration(base.calibration_table, proba, null);
      base.confidence_mean = round(conf ?? 0);
      base.confidence_std = round(0);
      base.top_k_high_conf = [{
        ts_event: null,
        seq: null,
        proba: round(proba),
        confidence: round(conf ?? 0),
        label_direction: null
      }];
      base.score_vs_label = { neg: null, zero: null, pos: null };
      return base;
    }

    return await this.#batchScore({ runId, featureJobId, labelJobId, modelPath, decisionPath, base });
  }

  async #batchScore({ runId, featureJobId, labelJobId, modelPath, decisionPath, base }) {
    const featureDir = path.resolve(__dirname, '../../featurexd/datasets', featureJobId);
    const labelDir = path.resolve(__dirname, '../../labeld/datasets', labelJobId);

    const featureManifest = await readJson(path.join(featureDir, 'dataset_manifest.json'));
    const labelManifest = await readJson(path.join(labelDir, 'label_manifest.json'));

    const featurePath = path.join(featureDir, 'features.parquet');
    const labelPath = path.join(labelDir, 'labels.parquet');

    const featureReader = await parquet.ParquetReader.openFile(featurePath);
    const labelReader = await parquet.ParquetReader.openFile(labelPath);
    const featureCursor = featureReader.getCursor();
    const labelCursor = labelReader.getCursor();

    const model = new XGBoostModel({ seed: 42 });
    const decision = await readJson(decisionPath);
    await model.load(modelPath);

    const featureList = featureManifest?.features || null;

    let featureRow;
    let labelRow;
    let count = 0;
    let sumNeg = 0;
    let sumZero = 0;
    let sumPos = 0;
    let countNeg = 0;
    let countZero = 0;
    let countPos = 0;
    let confSum = 0;
    let confSumSq = 0;

    const topK = [];
    const TOP_K = 10;

    while ((featureRow = await featureCursor.next()) && (labelRow = await labelCursor.next())) {
      const tsEvent = toBigInt(featureRow.ts_event);
      const seq = toBigInt(featureRow.seq ?? 0);
      const labelTs = toBigInt(labelRow.ts_event);
      const labelSeq = toBigInt(labelRow.seq ?? 0);

      if (tsEvent !== labelTs || seq !== labelSeq) {
        await featureReader.close();
        await labelReader.close();
        throw new Error(`ORDERING_MISMATCH: ${tsEvent}:${seq} vs ${labelTs}:${labelSeq}`);
      }

      const features = buildFeatureVector(featureRow, featureList);
      const probas = model.predictProba([features]);
      const proba = round(probas[0]);
      const confidence = round(Math.abs(proba - 0.5) * 2);

      const labelDir = normalizeLabel(labelRow.label_direction);
      updateLabelSummary(base.label_summary, labelDir);
      ({ sumNeg, sumZero, sumPos, countNeg, countZero, countPos } = updateScoreByLabel(labelDir, proba, { sumNeg, sumZero, sumPos, countNeg, countZero, countPos }));

      incrementBin(base.proba_histogram, proba);
      incrementBin(base.confidence_histogram, confidence);
      updateCalibration(base.calibration_table, proba, labelDir);
      confSum += confidence;
      confSumSq += confidence * confidence;

      addTopK(topK, {
        ts_event: tsEvent.toString(),
        seq: seq.toString(),
        proba,
        confidence,
        label_direction: labelDir
      }, TOP_K);

      count += 1;
    }

    await featureReader.close();
    await labelReader.close();

    base.count_total = count;
    base.count_scored = count;
    if (count > 0) {
      const mean = confSum / count;
      const variance = Math.max(0, confSumSq / count - mean * mean);
      base.confidence_mean = round(mean);
      base.confidence_std = round(Math.sqrt(variance));
    } else {
      base.confidence_mean = null;
      base.confidence_std = null;
    }
    base.top_k_high_conf = topK;
    base.score_vs_label = {
      neg: countNeg > 0 ? round(sumNeg / countNeg) : null,
      zero: countZero > 0 ? round(sumZero / countZero) : null,
      pos: countPos > 0 ? round(sumPos / countPos) : null
    };

    finalizeCalibration(base.calibration_table);

    base.meta = {
      feature_set_id: featureManifest?.feature_set_id || null,
      feature_set_version: featureManifest?.feature_set_version || null,
      label_set_id: labelManifest?.label_set_id || null,
      label_set_version: labelManifest?.label_set_version || null,
      model_type: decision?.model_type || null,
      model_version: decision?.model_version || null
    };

    return base;
  }

  async #updateIndex(runId, obs, obsPath) {
    const entries = await readJson(OBS_INDEX) || [];
    const existing = Array.isArray(entries) ? entries.slice() : [];
    if (existing.some((e) => e?.run_id === runId)) {
      return;
    }
    existing.push({
      run_id: runId,
      path: obsPath,
      count_total: obs.count_total,
      count_scored: obs.count_scored
    });

    existing.sort((a, b) => a.run_id.localeCompare(b.run_id));
    await fs.writeFile(OBS_INDEX, JSON.stringify(existing, null, 2));
  }
}

function initBins(count) {
  return Array.from({ length: count }, () => 0);
}

function incrementBin(bins, value) {
  const v = clamp01(value ?? 0);
  const idx = Math.min(bins.length - 1, Math.floor(v * bins.length));
  bins[idx] += 1;
}

function initCalibration(count) {
  const bins = [];
  for (let i = 0; i < count; i++) {
    bins.push({
      bin: i,
      count: 0,
      sum_proba: 0,
      sum_wins: 0,
      avg_proba: null,
      win_rate: null
    });
  }
  return bins;
}

function updateCalibration(bins, proba, labelDir) {
  const v = clamp01(proba ?? 0);
  const idx = Math.min(bins.length - 1, Math.floor(v * bins.length));
  const bucket = bins[idx];
  bucket.count += 1;
  bucket.sum_proba += v;
  if (labelDir === 1) bucket.sum_wins += 1;
}

function finalizeCalibration(bins) {
  for (const bin of bins) {
    if (bin.count > 0) {
      bin.avg_proba = round(bin.sum_proba / bin.count);
      bin.win_rate = round(bin.sum_wins / bin.count);
    } else {
      bin.avg_proba = null;
      bin.win_rate = null;
    }
    delete bin.sum_proba;
    delete bin.sum_wins;
  }
}

function addTopK(arr, row, limit) {
  arr.push(row);
  arr.sort((a, b) => {
    if (b.confidence !== a.confidence) return b.confidence - a.confidence;
    if (a.ts_event !== b.ts_event) return compareStr(a.ts_event, b.ts_event);
    return compareStr(a.seq, b.seq);
  });
  if (arr.length > limit) arr.length = limit;
}

function compareStr(a, b) {
  if (a === b) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  return String(a).localeCompare(String(b));
}

function buildFeatureVector(row, featureList) {
  const features = featureList || Object.keys(row).filter((k) => k.startsWith('f_')).sort();
  return features.map((name) => Number(row[name]));
}

function normalizeLabel(label) {
  if (label === null || label === undefined) return null;
  const val = Number(label);
  if (val > 0) return 1;
  if (val < 0) return -1;
  return 0;
}

function updateLabelSummary(summary, labelDir) {
  if (labelDir === 1) summary.pos += 1;
  else if (labelDir === -1) summary.neg += 1;
  else if (labelDir === 0) summary.zero += 1;
}

function updateScoreByLabel(labelDir, proba, state) {
  const { sumNeg, sumZero, sumPos, countNeg, countZero, countPos } = state;
  if (labelDir === -1) return { sumNeg: sumNeg + proba, sumZero, sumPos, countNeg: countNeg + 1, countZero, countPos };
  if (labelDir === 0) return { sumNeg, sumZero: sumZero + proba, sumPos, countNeg, countZero: countZero + 1, countPos };
  if (labelDir === 1) return { sumNeg, sumZero, sumPos: sumPos + proba, countNeg, countZero, countPos: countPos + 1 };
  return state;
}

function clamp01(value) {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

function round(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.round(value * ROUND_SCALE) / ROUND_SCALE;
}

function toBigInt(value) {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === 'string' && value.trim() !== '') {
    try {
      return BigInt(value.trim());
    } catch {
      return BigInt(0);
    }
  }
  return BigInt(0);
}

async function readJson(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return num;
}
