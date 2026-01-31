/**
 * RuntimeConfig — deterministic runtime config loader for ML active gating.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  ML_ACTIVE_ENABLED,
  ML_ACTIVE_KILL,
  ML_ACTIVE_MAX_DAILY_IMPACT_PCT,
  ML_ACTIVE_MAX_WEIGHT
} from './constants.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const REPORT_DIR = path.join(RUNS_DIR, 'report');

export function loadActiveConfig(strategyId, seed) {
  if (!strategyId || !seed) return null;
  const safeStrategy = sanitize(strategyId);
  const safeSeed = sanitize(seed);
  const fileName = `active_config_${safeStrategy}_${safeSeed}.json`;
  const filePath = path.join(REPORT_DIR, fileName);
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function loadRuntimeConfig({ strategyId, seed } = {}) {
  const maxWeightEnv = Number(process.env.ML_ACTIVE_MAX_WEIGHT);
  const dailyCapEnv = Number(process.env.ML_ACTIVE_DAILY_CAP_PCT);
  const verdictEnv = process.env.ML_ACTIVE_VERDICT || null;
  const verdictPathEnv = process.env.ML_ACTIVE_VERDICT_PATH || null;
  const activeConfig = loadActiveConfig(strategyId, seed);

  if (activeConfig) {
    const maxWeightFile = Number(activeConfig?.limits?.max_weight);
    const dailyCapFile = Number(activeConfig?.limits?.daily_cap);
    return {
      mlActiveEnabled: true,
      mlActiveKill: ML_ACTIVE_KILL,
      maxWeight: Number.isFinite(maxWeightFile) ? maxWeightFile : ML_ACTIVE_MAX_WEIGHT,
      dailyCap: Number.isFinite(dailyCapFile) ? dailyCapFile : ML_ACTIVE_MAX_DAILY_IMPACT_PCT,
      verdict: verdictEnv,
      verdictPath: verdictPathEnv
    };
  }

  return {
    mlActiveEnabled: false,
    mlActiveKill: ML_ACTIVE_KILL,
    maxWeight: Number.isFinite(maxWeightEnv) ? maxWeightEnv : ML_ACTIVE_MAX_WEIGHT,
    dailyCap: Number.isFinite(dailyCapEnv) ? dailyCapEnv : ML_ACTIVE_MAX_DAILY_IMPACT_PCT,
    verdict: verdictEnv,
    verdictPath: verdictPathEnv
  };
}

function sanitize(value) {
  return String(value).replace(/[^a-zA-Z0-9._-]+/g, '_');
}
