/**
 * MLActiveGate — deterministic gate for ML active execution.
 */

import fs from 'node:fs';

export class MLActiveGate {
  #config;
  #verdict = null;
  #verdictSource = 'none';

  constructor(config) {
    this.#config = config || {};
    this.#verdict = this.#resolveVerdict();
  }

  getVerdict() {
    return this.#verdict;
  }

  getVerdictSource() {
    return this.#verdictSource;
  }

  isActiveAllowed() {
    if (!this.#config?.mlActiveEnabled) {
      return { allowed: false, reason: 'active_disabled' };
    }
    if (this.#config?.mlActiveKill) {
      return { allowed: false, reason: 'kill_switch' };
    }
    if (this.#verdict !== 'EDGE_VAR') {
      return { allowed: false, reason: `verdict_${this.#verdict || 'missing'}` };
    }
    return { allowed: true, reason: 'verdict_EDGE_VAR' };
  }

  #resolveVerdict() {
    if (this.#config?.verdict) {
      this.#verdictSource = 'env';
      return this.#config.verdict;
    }
    const path = this.#config?.verdictPath;
    if (path) {
      try {
        const raw = fs.readFileSync(path, 'utf8');
        const json = JSON.parse(raw);
        const verdict = json?.verdict || null;
        this.#verdictSource = 'file';
        return verdict;
      } catch {
        this.#verdictSource = 'file_error';
        return null;
      }
    }
    return null;
  }
}
