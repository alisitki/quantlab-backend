/**
 * RunCompareService — deterministic run comparison with on-disk cache.
 *
 * Supports ML-aware comparison when include_ml=true:
 * - Model config diff (threshold, probaSource, featureset)
 * - Signal divergence attribution
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const SUMMARY_DIR = path.join(RUNS_DIR, 'summary');
const COMPARE_DIR = path.join(RUNS_DIR, 'compare');
const ML_ARTIFACTS_DIR = path.resolve(__dirname, '../../../core/ml/artifacts/jobs');

export class RunCompareService {
  /**
   * Compare two runs with optional ML-aware analysis.
   *
   * @param {string} runA - First run ID
   * @param {string} runB - Second run ID
   * @param {Object} [options] - Comparison options
   * @param {boolean} [options.includeMl=false] - Include ML model comparison
   * @returns {Promise<Object>} Comparison result
   */
  async compare(runA, runB, options = {}) {
    const { includeMl = false } = options;

    if (!runA || !runB) return { error: 'RUN_NOT_FOUND', status: 404 };
    if (runA === runB) return { error: 'SAME_RUN_ID', status: 400 };

    const [a, b] = [runA, runB].sort();
    const cacheKey = includeMl ? `${a}__${b}_ml` : `${a}__${b}`;
    const outPath = path.join(COMPARE_DIR, `${cacheKey}.json`);

    try {
      const cached = await fs.readFile(outPath, 'utf8');
      return { status: 200, data: JSON.parse(cached), path: outPath };
    } catch {
      // cache miss
    }

    const summaryA = await this.#readJson(path.join(SUMMARY_DIR, `${a}.json`));
    const summaryB = await this.#readJson(path.join(SUMMARY_DIR, `${b}.json`));
    const manifestA = await this.#readJson(path.join(RUNS_DIR, `${a}.json`));
    const manifestB = await this.#readJson(path.join(RUNS_DIR, `${b}.json`));

    if (!summaryA || !summaryB || !manifestA || !manifestB) {
      return { error: 'RUN_NOT_FOUND', status: 404 };
    }

    await fs.mkdir(COMPARE_DIR, { recursive: true });

    let compare = this.#buildCompare(a, b, summaryA, summaryB, manifestA, manifestB);

    // Add ML comparison if requested
    if (includeMl) {
      const mlComparison = await this.#buildMlComparison(summaryA, summaryB, manifestA, manifestB);
      compare = { ...compare, ml_comparison: mlComparison };
    }

    await fs.writeFile(outPath, JSON.stringify(compare, null, 2));

    return { status: 200, data: compare, path: outPath };
  }

  /**
   * Compare two ML models directly by job ID.
   *
   * @param {string} jobIdA - First ML job ID
   * @param {string} jobIdB - Second ML job ID
   * @returns {Promise<Object>} ML model comparison result
   */
  async compareMlModels(jobIdA, jobIdB) {
    if (!jobIdA || !jobIdB) return { error: 'JOB_ID_REQUIRED', status: 400 };
    if (jobIdA === jobIdB) return { error: 'SAME_JOB_ID', status: 400 };

    const [a, b] = [jobIdA, jobIdB].sort();
    const outPath = path.join(COMPARE_DIR, `ml_${a}__${b}.json`);

    try {
      const cached = await fs.readFile(outPath, 'utf8');
      return { status: 200, data: JSON.parse(cached), path: outPath };
    } catch {
      // cache miss
    }

    const decisionA = await this.#readMlDecision(a);
    const decisionB = await this.#readMlDecision(b);
    const metricsA = await this.#readMlMetrics(a);
    const metricsB = await this.#readMlMetrics(b);

    if (!decisionA && !metricsA) {
      return { error: 'ML_JOB_A_NOT_FOUND', status: 404 };
    }
    if (!decisionB && !metricsB) {
      return { error: 'ML_JOB_B_NOT_FOUND', status: 404 };
    }

    await fs.mkdir(COMPARE_DIR, { recursive: true });

    const compare = this.#buildMlModelCompare(a, b, decisionA, decisionB, metricsA, metricsB);
    await fs.writeFile(outPath, JSON.stringify(compare, null, 2));

    return { status: 200, data: compare, path: outPath };
  }

  #buildCompare(a, b, summaryA, summaryB, manifestA, manifestB) {
    const aState = manifestA?.output?.state_hash ?? null;
    const bState = manifestB?.output?.state_hash ?? null;
    const aFills = manifestA?.output?.fills_hash ?? null;
    const bFills = manifestB?.output?.fills_hash ?? null;

    return {
      run_a: a,
      run_b: b,
      state_hash: this.#compareEqual(aState, bState),
      fills_hash: this.#compareEqual(aFills, bFills),
      total_events_delta: this.#delta(summaryA?.total_events, summaryB?.total_events),
      total_signals_delta: this.#delta(summaryA?.total_signals, summaryB?.total_signals),
      total_fills_delta: this.#delta(summaryA?.total_fills, summaryB?.total_fills),
      ended_reason_a: summaryA?.ended_reason ?? null,
      ended_reason_b: summaryB?.ended_reason ?? null,
      duration_ms_delta: this.#delta(summaryA?.duration_ms, summaryB?.duration_ms)
    };
  }

  #compareEqual(a, b) {
    if (a === null || b === null) return 'not_equal';
    return a === b ? 'equal' : 'not_equal';
  }

  #delta(a, b) {
    if (typeof a !== 'number' || typeof b !== 'number') return null;
    return Math.abs(a - b);
  }

  async #readJson(filePath) {
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }

  // ============================================================================
  // ML COMPARISON METHODS
  // ============================================================================

  /**
   * Build ML comparison section for run comparison.
   */
  async #buildMlComparison(summaryA, summaryB, manifestA, manifestB) {
    // Extract ML metadata from run manifests
    const mlA = manifestA?.ml || summaryA?.ml_model_metadata || null;
    const mlB = manifestB?.ml || summaryB?.ml_model_metadata || null;

    if (!mlA && !mlB) {
      return {
        available: false,
        reason: 'No ML metadata in either run'
      };
    }

    const modelAJobId = mlA?.job_id || mlA?.model_job_id || null;
    const modelBJobId = mlB?.job_id || mlB?.model_job_id || null;

    // Load decision configs if job IDs available
    let decisionA = null;
    let decisionB = null;
    if (modelAJobId) decisionA = await this.#readMlDecision(modelAJobId);
    if (modelBJobId) decisionB = await this.#readMlDecision(modelBJobId);

    const thresholdA = decisionA?.best_threshold ?? mlA?.threshold ?? null;
    const thresholdB = decisionB?.best_threshold ?? mlB?.threshold ?? null;
    const probaSourceA = decisionA?.probaSource ?? mlA?.proba_source ?? null;
    const probaSourceB = decisionB?.probaSource ?? mlB?.proba_source ?? null;
    const featuresetA = decisionA?.featureset_version ?? mlA?.featureset_version ?? null;
    const featuresetB = decisionB?.featureset_version ?? mlB?.featureset_version ?? null;

    return {
      available: true,
      models_match: modelAJobId === modelBJobId && modelAJobId !== null,
      model_a: {
        job_id: modelAJobId,
        threshold: thresholdA,
        proba_source: probaSourceA,
        featureset_version: featuresetA,
        enabled: mlA?.enabled ?? null
      },
      model_b: {
        job_id: modelBJobId,
        threshold: thresholdB,
        proba_source: probaSourceB,
        featureset_version: featuresetB,
        enabled: mlB?.enabled ?? null
      },
      config_diff: {
        threshold_diff: this.#safeDiff(thresholdA, thresholdB),
        proba_source_match: probaSourceA === probaSourceB,
        featureset_match: featuresetA === featuresetB
      },
      signal_divergence_likely: this.#assessSignalDivergence(thresholdA, thresholdB, probaSourceA, probaSourceB)
    };
  }

  /**
   * Build full ML model comparison.
   */
  #buildMlModelCompare(jobIdA, jobIdB, decisionA, decisionB, metricsA, metricsB) {
    return {
      job_a: jobIdA,
      job_b: jobIdB,
      comparison_timestamp: new Date().toISOString(),
      model_configs: {
        a: this.#extractModelConfig(decisionA),
        b: this.#extractModelConfig(decisionB)
      },
      metrics_comparison: this.#compareMetrics(metricsA, metricsB),
      config_diff: this.#diffModelConfigs(decisionA, decisionB),
      recommendation: this.#generateRecommendation(metricsA, metricsB)
    };
  }

  #extractModelConfig(decision) {
    if (!decision) return null;
    return {
      model_type: decision.model_type ?? null,
      model_version: decision.model_version ?? null,
      threshold: decision.best_threshold ?? null,
      proba_source: decision.probaSource ?? null,
      featureset_version: decision.featureset_version ?? decision.featuresetVersion ?? null,
      label_horizon: decision.labelHorizonSec ?? null,
      symbol: decision.symbol ?? null,
      generated_at: decision.generated_at ?? null
    };
  }

  #compareMetrics(metricsA, metricsB) {
    if (!metricsA && !metricsB) return null;

    const fields = [
      'accuracy',
      'precision_pos',
      'recall_pos',
      'f1_pos',
      'directionalHitRate'
    ];

    const comparison = {};
    for (const field of fields) {
      const a = metricsA?.[field] ?? null;
      const b = metricsB?.[field] ?? null;
      comparison[field] = {
        a,
        b,
        diff: this.#safeDiff(a, b),
        better: this.#determineBetter(a, b)
      };
    }

    // Best threshold comparison
    if (metricsA?.best_threshold || metricsB?.best_threshold) {
      comparison.best_threshold = {
        a: metricsA?.best_threshold?.value ?? null,
        b: metricsB?.best_threshold?.value ?? null,
        a_metric: metricsA?.best_threshold?.by ?? null,
        b_metric: metricsB?.best_threshold?.by ?? null
      };
    }

    return comparison;
  }

  #diffModelConfigs(decisionA, decisionB) {
    if (!decisionA && !decisionB) return null;

    const thresholdA = decisionA?.best_threshold ?? null;
    const thresholdB = decisionB?.best_threshold ?? null;
    const probaA = decisionA?.probaSource ?? null;
    const probaB = decisionB?.probaSource ?? null;
    const featureA = decisionA?.featureset_version ?? decisionA?.featuresetVersion ?? null;
    const featureB = decisionB?.featureset_version ?? decisionB?.featuresetVersion ?? null;

    return {
      threshold: {
        a: thresholdA,
        b: thresholdB,
        diff: this.#safeDiff(thresholdA, thresholdB),
        significant: Math.abs(this.#safeDiff(thresholdA, thresholdB) ?? 0) > 0.05
      },
      proba_source: {
        a: probaA,
        b: probaB,
        match: probaA === probaB
      },
      featureset: {
        a: featureA,
        b: featureB,
        match: featureA === featureB,
        compatible: this.#areFeatureSetsCompatible(featureA, featureB)
      },
      model_type: {
        a: decisionA?.model_type ?? null,
        b: decisionB?.model_type ?? null,
        match: decisionA?.model_type === decisionB?.model_type
      }
    };
  }

  #generateRecommendation(metricsA, metricsB) {
    if (!metricsA || !metricsB) {
      return {
        winner: 'unknown',
        reason: 'Insufficient metrics data',
        confidence: 'low'
      };
    }

    const f1A = metricsA.f1_pos ?? 0;
    const f1B = metricsB.f1_pos ?? 0;
    const dirA = metricsA.directionalHitRate ?? 0;
    const dirB = metricsB.directionalHitRate ?? 0;

    // Simple scoring: weighted average of F1 and directional hit rate
    const scoreA = f1A * 0.6 + dirA * 0.4;
    const scoreB = f1B * 0.6 + dirB * 0.4;

    const diff = Math.abs(scoreA - scoreB);
    let confidence = 'low';
    if (diff > 0.1) confidence = 'high';
    else if (diff > 0.05) confidence = 'medium';

    if (scoreA > scoreB) {
      return {
        winner: 'model_a',
        reason: `Model A scores ${(scoreA * 100).toFixed(1)}% vs Model B ${(scoreB * 100).toFixed(1)}%`,
        confidence,
        scores: { a: scoreA, b: scoreB }
      };
    } else if (scoreB > scoreA) {
      return {
        winner: 'model_b',
        reason: `Model B scores ${(scoreB * 100).toFixed(1)}% vs Model A ${(scoreA * 100).toFixed(1)}%`,
        confidence,
        scores: { a: scoreA, b: scoreB }
      };
    } else {
      return {
        winner: 'tie',
        reason: 'Models have equivalent scores',
        confidence: 'high',
        scores: { a: scoreA, b: scoreB }
      };
    }
  }

  #assessSignalDivergence(thresholdA, thresholdB, probaSourceA, probaSourceB) {
    if (thresholdA === null || thresholdB === null) return 'unknown';

    const thresholdDiff = Math.abs(thresholdA - thresholdB);
    const probaMatch = probaSourceA === probaSourceB;

    if (thresholdDiff > 0.1 || !probaMatch) return 'high';
    if (thresholdDiff > 0.05) return 'medium';
    return 'low';
  }

  #areFeatureSetsCompatible(featureA, featureB) {
    if (!featureA || !featureB) return true; // Assume compatible if unknown
    // Extract major version
    const majorA = featureA.split('.')[0] || featureA;
    const majorB = featureB.split('.')[0] || featureB;
    return majorA === majorB;
  }

  #safeDiff(a, b) {
    if (typeof a !== 'number' || typeof b !== 'number') return null;
    return Math.round((a - b) * 10000) / 10000; // 4 decimal precision
  }

  #determineBetter(a, b) {
    if (typeof a !== 'number' || typeof b !== 'number') return 'unknown';
    if (a > b) return 'a';
    if (b > a) return 'b';
    return 'equal';
  }

  // ============================================================================
  // ML ARTIFACT READERS
  // ============================================================================

  async #readMlDecision(jobId) {
    const decisionPath = path.join(ML_ARTIFACTS_DIR, jobId, 'decision.json');
    return this.#readJson(decisionPath);
  }

  async #readMlMetrics(jobId) {
    const metricsPath = path.join(ML_ARTIFACTS_DIR, jobId, 'metrics.json');
    return this.#readJson(metricsPath);
  }
}
