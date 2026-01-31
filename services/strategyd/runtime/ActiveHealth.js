/**
 * ActiveHealth — read-only ACTIVE health snapshot.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const REPORT_DIR = path.join(RUNS_DIR, 'report');

export class ActiveHealth {
  constructor({ strategyId, seed }) {
    this.strategyId = strategyId || null;
    this.seed = seed || null;
  }

  getSnapshot() {
    const activeConfig = this.#readActiveConfig();
    const activeConfigPresent = Boolean(activeConfig);
    const killSwitch = process.env.ML_ACTIVE_KILL === '1';
    const activeEnabled = activeConfigPresent && !killSwitch;
    const limits = activeConfig?.limits || null;
    const guards = activeConfig?.guards || null;
    const provenance = activeConfig?.provenance || null;

    return {
      active_enabled: activeEnabled,
      strategy_id: this.strategyId,
      seed: this.seed,
      active_config_present: activeConfigPresent,
      limits: limits ? {
        max_weight: limits.max_weight ?? null,
        daily_cap: limits.daily_cap ?? null
      } : { max_weight: null, daily_cap: null },
      guards: guards ? {
        kill_switch_required: guards.kill_switch_required === true,
        safety_audit_required: guards.safety_audit_required === true
      } : { kill_switch_required: true, safety_audit_required: true },
      provenance: {
        active_config_hash: activeConfigPresent ? this.#hashJson(activeConfig) : null,
        decision_hash: provenance?.decision_hash || null,
        triad_report_hash: provenance?.triad_report_hash || null
      }
    };
  }

  #readActiveConfig() {
    if (!this.strategyId || !this.seed) return null;
    const fileName = `active_config_${this.#sanitize(this.strategyId)}_${this.#sanitize(this.seed)}.json`;
    const filePath = path.join(REPORT_DIR, fileName);
    try {
      const raw = fs.readFileSync(filePath, 'utf8');
      return JSON.parse(raw);
    } catch {
      return null;
    }
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
