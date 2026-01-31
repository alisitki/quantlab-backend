/**
 * RunCompareService — deterministic run comparison with on-disk cache.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const SUMMARY_DIR = path.join(RUNS_DIR, 'summary');
const COMPARE_DIR = path.join(RUNS_DIR, 'compare');

export class RunCompareService {
  async compare(runA, runB) {
    if (!runA || !runB) return { error: 'RUN_NOT_FOUND', status: 404 };
    if (runA === runB) return { error: 'SAME_RUN_ID', status: 400 };

    const [a, b] = [runA, runB].sort();
    const outPath = path.join(COMPARE_DIR, `${a}__${b}.json`);

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

    const compare = this.#buildCompare(a, b, summaryA, summaryB, manifestA, manifestB);
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
}
