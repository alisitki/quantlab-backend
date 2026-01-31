/**
 * TriadReportBuilder — deterministic triad report (off/shadow/active).
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const OBS_DIR = path.join(RUNS_DIR, 'obs');
const ANALYSIS_DIR = path.join(RUNS_DIR, 'analysis');
const AUDIT_DIR = path.join(RUNS_DIR, 'active_audit');
const REPORT_DIR = path.join(RUNS_DIR, 'report');

const ROUND_SCALE = 1e6;

export class TriadReportBuilder {
  async build({ offRunId, shadowRunId, activeRunId }) {
    const offManifest = await this.#readJson(path.join(RUNS_DIR, `${offRunId}.json`));
    const shadowManifest = await this.#readJson(path.join(RUNS_DIR, `${shadowRunId}.json`));
    const activeManifest = await this.#readJson(path.join(RUNS_DIR, `${activeRunId}.json`));
    if (!offManifest || !shadowManifest || !activeManifest) {
      throw new Error('MANIFEST_MISSING');
    }

    const offObs = await this.#readJson(path.join(OBS_DIR, `${offRunId}.json`));
    const shadowObs = await this.#readJson(path.join(OBS_DIR, `${shadowRunId}.json`));
    const activeObs = await this.#readJson(path.join(OBS_DIR, `${activeRunId}.json`));
    const audit = await this.#readJson(path.join(AUDIT_DIR, `${activeRunId}.json`));

    const analysis = await this.#findAnalysis(offRunId, shadowRunId);

    const identity = {
      strategy_id: offManifest?.strategy?.id || offManifest?.strategy_id || null,
      seed: offManifest?.strategy?.seed || null,
      state_hash_off: offManifest?.output?.state_hash || null,
      fills_hash_off: offManifest?.output?.fills_hash || null
    };

    const shadowVsOff = {
      state_hash_equal: offManifest?.output?.state_hash === shadowManifest?.output?.state_hash,
      fills_hash_equal: offManifest?.output?.fills_hash === shadowManifest?.output?.fills_hash,
      obs_delta: shadowObs && offObs ? {
        confidence_mean_delta: this.#round(
          this.#toNumber(shadowObs.confidence_mean, 0) - this.#toNumber(offObs.confidence_mean, 0)
        )
      } : null,
      verdict: analysis?.verdict || null
    };

    const activeApplied = activeManifest?.extra?.ml?.active_applied === true;
    const activeVsShadow = {
      active_applied: activeApplied,
      active_reason: activeManifest?.extra?.ml?.active_reason || null,
      state_hash_equal: activeManifest?.output?.state_hash === shadowManifest?.output?.state_hash,
      fills_hash_equal: activeManifest?.output?.fills_hash === shadowManifest?.output?.fills_hash,
      audit: this.#summarizeAudit(audit)
    };

    const report = {
      triad: {
        off_run_id: offRunId,
        shadow_run_id: shadowRunId,
        active_run_id: activeRunId
      },
      identity,
      shadow_vs_off: shadowVsOff,
      active_vs_shadow: activeVsShadow
    };

    await fs.mkdir(REPORT_DIR, { recursive: true });
    const fileName = this.#sanitizeFileName(`triad_${identity.strategy_id || 'unknown'}_${identity.seed || 'unknown'}.json`);
    const outPath = path.join(REPORT_DIR, fileName);
    await fs.writeFile(outPath, JSON.stringify(report, null, 2));
    return { report, outPath };
  }

  async #findAnalysis(offRunId, shadowRunId) {
    try {
      const fileName = `${offRunId}__${shadowRunId}.json`;
      const direct = await this.#readJson(path.join(ANALYSIS_DIR, fileName));
      if (direct) return direct;
      return null;
    } catch {
      return null;
    }
  }

  #summarizeAudit(audit) {
    if (!Array.isArray(audit) || audit.length === 0) {
      return { entries: 0, violations_count: 0, max_weight_used: null, qty_multiplier: null };
    }
    let maxWeight = null;
    let minMult = null;
    let maxMult = null;
    let sumMult = 0;
    let countMult = 0;
    let violations = 0;
    for (const entry of audit) {
      if (entry?.violation) violations += 1;
      const weight = this.#toNumber(entry?.ml_weight, null);
      if (weight !== null) {
        if (maxWeight === null || weight > maxWeight) maxWeight = weight;
      }
      const baseQty = this.#toNumber(entry?.base_qty, null);
      const appliedQty = this.#toNumber(entry?.applied_qty, null);
      if (baseQty && appliedQty) {
        const mult = appliedQty / baseQty;
        minMult = minMult === null ? mult : Math.min(minMult, mult);
        maxMult = maxMult === null ? mult : Math.max(maxMult, mult);
        sumMult += mult;
        countMult += 1;
      }
    }
    const meanMult = countMult > 0 ? sumMult / countMult : null;
    return {
      entries: audit.length,
      violations_count: violations,
      max_weight_used: maxWeight === null ? null : this.#round(maxWeight),
      qty_multiplier: countMult > 0 ? {
        min: this.#round(minMult),
        mean: this.#round(meanMult),
        max: this.#round(maxMult)
      } : null
    };
  }

  #sanitizeFileName(name) {
    return String(name).replace(/[^a-zA-Z0-9._-]+/g, '_');
  }

  #round(value) {
    if (!Number.isFinite(value)) return 0;
    return Math.round(value * ROUND_SCALE) / ROUND_SCALE;
  }

  #toNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const num = Number(value);
    if (!Number.isFinite(num)) return fallback;
    return num;
  }

  async #readJson(filePath) {
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
}
