/**
 * MLDecisionAdapter — shadow ML inference (read-only, deterministic).
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { XGBoostModel } from '../../../core/ml/models/XGBoostModel.js';
import {
  ML_ACTIVE_C_MIN,
  ML_ACTIVE_C_MAX,
  ML_ACTIVE_MIN_WEIGHT,
  ML_ACTIVE_MAX_WEIGHT
} from './constants.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '../../..');

const FEATURE_COLUMNS = [
  'f_mid_price',
  'f_spread',
  'f_log_return_1s',
  'f_log_return_5s',
  'f_book_imbalance',
  'f_trade_intensity'
];

const ROUND_SCALE = 1e8;

export class MLDecisionAdapter {
  #enabled = false;
  #loaded = false;
  #loadError = null;
  #model = null;
  #decision = null;
  #decisionPath = null;
  #modelPath = null;
  #featureState = new FeatureState();
  #lastResult = null;
  #computing = false;

  constructor(config = {}) {
    this.#enabled = config.enabled === true;
    this.#decisionPath = config.decisionPath || null;
    this.#modelPath = config.modelPath || null;
  }

  getLastResult() {
    return this.#lastResult;
  }

  computeWeight(confidence) {
    const c = toNumber(confidence);
    if (c === null) return 1.0;
    if (c < 0) return 1.0;
    if (c > 1) return 1.0;
    if (c < ML_ACTIVE_C_MIN) return 1.0;
    if (c >= ML_ACTIVE_C_MAX) return clamp(ML_ACTIVE_MAX_WEIGHT, ML_ACTIVE_MIN_WEIGHT, ML_ACTIVE_MAX_WEIGHT);
    const span = ML_ACTIVE_C_MAX - ML_ACTIVE_C_MIN;
    if (span <= 0) return 1.0;
    const ratio = (c - ML_ACTIVE_C_MIN) / span;
    const weight = ML_ACTIVE_MIN_WEIGHT + ratio * (ML_ACTIVE_MAX_WEIGHT - ML_ACTIVE_MIN_WEIGHT);
    return clamp(weight, ML_ACTIVE_MIN_WEIGHT, ML_ACTIVE_MAX_WEIGHT);
  }

  observeEvent(event) {
    if (!this.#enabled) return;
    const payload = event?.payload || event;
    this.#featureState.observe(payload);
  }

  computeShadow(event) {
    if (!this.#enabled) return;
    if (this.#computing) return;
    this.#computing = true;

    setImmediate(async () => {
      try {
        const ok = await this.#ensureLoaded();
        if (!ok) return;

        const payload = event?.payload || event;
        const features = this.#featureState.extract(payload);
        if (!features) return;

        const vector = FEATURE_COLUMNS.map((name) => features[name]);
        let proba;
        try {
          const probs = this.#model.predictProba([vector]);
          proba = probs[0];
        } catch (err) {
          console.error(`[MLDecisionAdapter] action=predict_error error=${err.message}`);
          return;
        }

        const roundedProba = round(proba);
        const confidence = round(Math.abs(roundedProba - 0.5) * 2);

        const result = {
          model_type: this.#decision?.model_type || 'xgboost_v1',
          model_version: this.#decision?.model_version || 'v1',
          proba: roundedProba,
          confidence,
          regime: this.#decision?.regime || null
        };

        this.#lastResult = result;
      } catch (err) {
        console.error(`[MLDecisionAdapter] action=shadow_error error=${err.message}`);
      } finally {
        this.#computing = false;
      }
    });
  }

  async #ensureLoaded() {
    if (this.#loaded) return this.#loadError === null;

    try {
      const decisionPath = this.#decisionPath || resolveDecisionPath();
      const modelPath = this.#modelPath || resolveModelPath();
      if (!decisionPath || !modelPath) {
        this.#loadError = 'missing_paths';
        this.#loaded = true;
        return false;
      }

      const decision = readJson(decisionPath);
      if (!decision) {
        this.#loadError = 'decision_missing';
        this.#loaded = true;
        return false;
      }

      const model = new XGBoostModel({ seed: Number(decision?.seed || 42) });
      await model.load(modelPath);

      this.#decision = decision;
      this.#model = model;
      this.#loaded = true;
      return true;
    } catch (err) {
      this.#loadError = err?.message || 'load_failed';
      this.#loaded = true;
      console.error(`[MLDecisionAdapter] action=load_error error=${this.#loadError}`);
      return false;
    }
  }
}

function resolveDecisionPath() {
  const explicit = process.env.ML_DECISION_PATH;
  if (explicit) return explicit;

  const jobId = process.env.ML_TRAINING_JOB_ID;
  if (!jobId) return null;
  return path.resolve(REPO_ROOT, 'core/ml/artifacts/jobs', jobId, 'decision.json');
}

function resolveModelPath() {
  const explicit = process.env.ML_MODEL_PATH;
  if (explicit) return explicit;

  const jobId = process.env.ML_TRAINING_JOB_ID;
  if (!jobId) return null;
  return path.resolve(REPO_ROOT, 'core/ml/artifacts/jobs', jobId, 'model.bin');
}

function readJson(filePath) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function round(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.round(value * ROUND_SCALE) / ROUND_SCALE;
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) return 1.0;
  if (value < min) return min;
  if (value > max) return max;
  return value;
}

class FeatureState {
  #midHistory = [];
  #eventTimes = [];

  observe(payload) {
    const tsEvent = this.#toNumber(payload?.ts_event);
    if (tsEvent === null) return;

    this.#eventTimes.push(tsEvent);

    const bid = Number(payload?.bid_price ?? payload?.bid ?? NaN);
    const ask = Number(payload?.ask_price ?? payload?.ask ?? NaN);
    if (!Number.isFinite(bid) || !Number.isFinite(ask)) return;

    const mid = (bid + ask) / 2;
    this.#midHistory.push({ ts: tsEvent, mid });
  }

  extract(payload) {
    const tsEvent = this.#toNumber(payload?.ts_event);
    if (tsEvent === null) return null;

    const bid = Number(payload?.bid_price ?? payload?.bid ?? NaN);
    const ask = Number(payload?.ask_price ?? payload?.ask ?? NaN);
    const bidSize = Number(payload?.bid_size ?? payload?.bid_qty ?? 0);
    const askSize = Number(payload?.ask_size ?? payload?.ask_qty ?? 0);

    if (!Number.isFinite(bid) || !Number.isFinite(ask)) return null;

    const mid = (bid + ask) / 2;
    const spread = ask - bid;

    const logReturn1s = this.#logReturn(tsEvent, mid, 1000);
    const logReturn5s = this.#logReturn(tsEvent, mid, 5000);
    const imbalance = this.#bookImbalance(bidSize, askSize);
    const intensity = this.#tradeIntensity(tsEvent, 1000);

    return {
      f_mid_price: round(mid),
      f_spread: round(spread),
      f_log_return_1s: round(logReturn1s),
      f_log_return_5s: round(logReturn5s),
      f_book_imbalance: round(imbalance),
      f_trade_intensity: round(intensity)
    };
  }

  #logReturn(tsEvent, mid, horizonMs) {
    const target = tsEvent - horizonMs;
    let candidate = null;

    while (this.#midHistory.length > 0 && this.#midHistory[0].ts <= target) {
      candidate = this.#midHistory.shift();
    }

    if (!candidate || candidate.mid <= 0 || mid <= 0) return 0;
    return Math.log(mid / candidate.mid);
  }

  #bookImbalance(bidSize, askSize) {
    const denom = bidSize + askSize;
    if (!Number.isFinite(denom) || denom === 0) return 0;
    return (bidSize - askSize) / denom;
  }

  #tradeIntensity(tsEvent, windowMs) {
    const cutoff = tsEvent - windowMs;
    while (this.#eventTimes.length > 0 && this.#eventTimes[0] < cutoff) {
      this.#eventTimes.shift();
    }
    return this.#eventTimes.length;
  }

  #toNumber(value) {
    if (typeof value === 'bigint') return Number(value);
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '') {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  }
}
