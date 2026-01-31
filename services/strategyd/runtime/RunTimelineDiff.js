/**
 * RunTimelineDiff — deterministic timeline diff based on manifest data.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const TIMELINE_DIR = path.join(RUNS_DIR, 'timeline');

export class RunTimelineDiff {
  async compare(runA, runB) {
    if (!runA || !runB) return { error: 'RUN_NOT_FOUND', status: 404 };
    if (runA === runB) return { error: 'SAME_RUN_ID', status: 400 };

    const [a, b] = [runA, runB].sort();
    const outPath = path.join(TIMELINE_DIR, `${a}__${b}.json`);

    try {
      const cached = await fs.readFile(outPath, 'utf8');
      return { status: 200, data: JSON.parse(cached), path: outPath };
    } catch {
      // cache miss
    }

    const manifestA = await this.#readJson(path.join(RUNS_DIR, `${a}.json`));
    const manifestB = await this.#readJson(path.join(RUNS_DIR, `${b}.json`));

    if (!manifestA || !manifestB) {
      return { error: 'RUN_NOT_FOUND', status: 404 };
    }

    await fs.mkdir(TIMELINE_DIR, { recursive: true });

    const diff = this.#buildDiff(a, b, manifestA, manifestB);
    await fs.writeFile(outPath, JSON.stringify(diff, null, 2));

    return { status: 200, data: diff, path: outPath };
  }

  #buildDiff(a, b, manifestA, manifestB) {
    const aState = manifestA?.output?.state_hash ?? null;
    const bState = manifestB?.output?.state_hash ?? null;
    const aFills = manifestA?.output?.fills_hash ?? null;
    const bFills = manifestB?.output?.fills_hash ?? null;

    const divergenceReason = this.#divergenceReason(aState, bState, aFills, bFills);

    return {
      run_a: a,
      run_b: b,
      first_divergence_at: null,
      divergence_reason: divergenceReason,
      counters_before: {
        event_count: null,
        fills_count: null
      },
      counters_after: {
        event_count_a: manifestA?.output?.event_count ?? null,
        event_count_b: manifestB?.output?.event_count ?? null,
        fills_count_a: manifestA?.output?.fills_count ?? null,
        fills_count_b: manifestB?.output?.fills_count ?? null
      },
      state_hash_before: null,
      state_hash_after: {
        a: aState,
        b: bState
      }
    };
  }

  #divergenceReason(aState, bState, aFills, bFills) {
    if (aState && bState && aState !== bState) return 'state_hash_mismatch';
    if (aFills && bFills && aFills !== bFills) return 'fills_hash_mismatch';
    return null;
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
