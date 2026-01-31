/**
 * ActiveConfigExporter — writes deterministic ACTIVE config artifact.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const REPORT_DIR = path.join(RUNS_DIR, 'report');

export class ActiveConfigExporter {
  async export({ decision, report, guardResult }) {
    if (!guardResult?.allowed) {
      return { written: false, reason: guardResult?.reason || 'not_allowed' };
    }
    const strategyId = report?.identity?.strategy_id || 'unknown';
    const seed = report?.identity?.seed || 'unknown';
    const fileName = `active_config_${this.#sanitize(strategyId)}_${this.#sanitize(seed)}.json`;
    const outPath = path.join(REPORT_DIR, fileName);

    try {
      await fs.access(outPath);
      return { written: false, reason: 'exists', path: outPath };
    } catch {
      // continue
    }

    const thresholds = decision?.thresholds_used || {};
    const config = {
      strategy_id: strategyId,
      seed,
      verdict: report?.shadow_vs_off?.verdict || null,
      decision: decision?.decision || null,
      limits: {
        max_weight: thresholds.max_weight_used ?? null,
        daily_cap: thresholds.daily_cap ?? null
      },
      guards: {
        kill_switch_required: true,
        safety_audit_required: true
      },
      provenance: {
        decision_hash: this.#hashJson(decision),
        triad_report_hash: this.#hashJson(report),
        thresholds_hash: this.#hashJson(thresholds)
      }
    };

    await fs.mkdir(REPORT_DIR, { recursive: true });
    await fs.writeFile(outPath, JSON.stringify(config, null, 2));
    return { written: true, path: outPath };
  }

  #hashJson(obj) {
    const json = this.#stableStringify(obj);
    return createHash('sha256').update(json).digest('hex');
  }

  #stableStringify(value) {
    if (value === null || typeof value !== 'object') {
      return JSON.stringify(value);
    }
    if (Array.isArray(value)) {
      return '[' + value.map((v) => this.#stableStringify(v)).join(',') + ']';
    }
    const keys = Object.keys(value).sort();
    return '{' + keys.map((k) => `"${k}":${this.#stableStringify(value[k])}`).join(',') + '}';
  }

  #sanitize(value) {
    return String(value).replace(/[^a-zA-Z0-9._-]+/g, '_');
  }
}
