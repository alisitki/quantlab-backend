/**
 * ActiveAudit — deterministic ML ACTIVE execution audit + safety checks.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const AUDIT_DIR = path.join(RUNS_DIR, 'active_audit');

export class ActiveAudit {
  #runId;
  #maxWeight;
  #writeChain = Promise.resolve();

  constructor({ runId, maxWeight }) {
    this.#runId = runId;
    this.#maxWeight = Number.isFinite(maxWeight) ? maxWeight : null;
  }

  checkAndRecord({ baseQty, mlWeight, appliedQty, direction, tsEvent, seq }) {
    const violation = this.#evaluateViolation({ baseQty, mlWeight, appliedQty, direction });
    const entry = {
      base_qty: baseQty,
      ml_weight: mlWeight,
      applied_qty: appliedQty,
      direction,
      ts_event: tsEvent ?? null,
      seq: seq ?? null,
      violation: violation ? `safety_violation:${violation}` : null
    };

    this.#writeChain = this.#writeChain
      .then(() => this.#appendEntry(entry))
      .catch(() => {});

    return violation ? { ok: false, reason: `safety_violation:${violation}` } : { ok: true, reason: null };
  }

  #evaluateViolation({ baseQty, mlWeight, appliedQty, direction }) {
    if (direction !== 'BUY' && direction !== 'SELL') return 'direction';
    if (!Number.isFinite(appliedQty) || appliedQty <= 0) return 'applied_qty';
    if (!Number.isFinite(baseQty) || baseQty <= 0) return 'base_qty';
    if (this.#maxWeight !== null && Number.isFinite(mlWeight) && mlWeight > this.#maxWeight) {
      return 'max_weight';
    }
    if (this.#maxWeight !== null && Number.isFinite(baseQty)) {
      const maxApplied = baseQty * this.#maxWeight;
      if (Number.isFinite(maxApplied) && appliedQty > maxApplied) return 'max_weight';
    }
    return null;
  }

  async #appendEntry(entry) {
    try {
      await fs.mkdir(AUDIT_DIR, { recursive: true });
      const filePath = path.join(AUDIT_DIR, `${this.#runId}.json`);
      const existing = await this.#readJson(filePath);
      const list = Array.isArray(existing) ? existing : [];
      list.push(entry);
      await fs.writeFile(filePath, JSON.stringify(list, null, 2));
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[ActiveAudit] run_id=${this.#runId} action=error error=${msg}`);
    }
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
