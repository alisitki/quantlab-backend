/**
 * ReplaySeekHelper — derive replay seek hints from timeline diffs.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const TIMELINE_DIR = path.join(RUNS_DIR, 'timeline');
const SEEK_DIR = path.join(RUNS_DIR, 'seek');

export class ReplaySeekHelper {
  async build(runA, runB) {
    if (!runA || !runB) return null;
    if (runA === runB) return null;

    const [a, b] = [runA, runB].sort();
    const timelinePath = path.join(TIMELINE_DIR, `${a}__${b}.json`);
    const outPath = path.join(SEEK_DIR, `${a}__${b}.json`);

    try {
      const cached = await fs.readFile(outPath, 'utf8');
      return JSON.parse(cached);
    } catch {
      // cache miss
    }

    const timeline = await this.#readJson(timelinePath);
    if (!timeline) {
      console.error(`[ReplaySeekHelper] component=strategyd action=skipped reason=missing_timeline path=${timelinePath}`);
      return null;
    }

    const manifestA = await this.#readJson(path.join(RUNS_DIR, `${a}.json`));
    const manifestB = await this.#readJson(path.join(RUNS_DIR, `${b}.json`));

    if (!manifestA || !manifestB) {
      console.error(`[ReplaySeekHelper] component=strategyd action=skipped reason=missing_manifest run_a=${a} run_b=${b}`);
      return null;
    }

    await fs.mkdir(SEEK_DIR, { recursive: true });

    const entry = this.#buildSeek(a, b, timeline, manifestA, manifestB);
    await fs.writeFile(outPath, JSON.stringify(entry, null, 2));
    console.log(`[ReplaySeekHelper] component=strategyd action=seek_written path=${outPath}`);
    return entry;
  }

  #buildSeek(a, b, timeline, manifestA, manifestB) {
    const divergence = timeline?.first_divergence_at ?? null;
    let seekCursor = null;
    let seekTsEvent = null;
    let reason = divergence ? (timeline?.divergence_reason ?? null) : null;

    if (divergence) {
      const pickA = this.#pickClosestCheckpoint(manifestA, divergence);
      const pickB = this.#pickClosestCheckpoint(manifestB, divergence);
      const chosen = this.#pickClosest(driftOrNull(pickA), driftOrNull(pickB));
      if (chosen) {
        seekCursor = chosen.cursor ?? null;
        seekTsEvent = chosen.ts_event ?? null;
      } else {
        seekCursor = manifestA?.output?.last_cursor ?? manifestB?.output?.last_cursor ?? null;
        seekTsEvent = manifestA?.output?.last_ts_event ?? manifestB?.output?.last_ts_event ?? null;
      }
    }

    return {
      run_a: a,
      run_b: b,
      seek_cursor: seekCursor,
      seek_ts_event: seekTsEvent,
      reason
    };
  }

  #pickClosestCheckpoint(manifest, divergence) {
    const checkpoints = this.#extractCheckpoints(manifest);
    if (!checkpoints || checkpoints.length === 0) return null;

    const target = this.#parseTs(divergence);
    if (target === null) return null;

    let best = null;
    for (const cp of checkpoints) {
      const ts = this.#parseTs(cp.ts_event ?? cp.ts ?? cp.timestamp ?? null);
      if (ts === null) continue;
      const diff = ts > target ? ts - target : target - ts;
      if (!best || diff < best.diff) {
        best = {
          diff,
          cursor: cp.cursor ?? null,
          ts_event: cp.ts_event ?? cp.ts ?? cp.timestamp ?? null
        };
      }
    }

    return best;
  }

  #pickClosest(a, b) {
    if (a && b) return a.diff <= b.diff ? a : b;
    return a || b || null;
  }

  #extractCheckpoints(manifest) {
    const output = manifest?.output || {};
    if (Array.isArray(output.checkpoints)) return output.checkpoints;
    if (Array.isArray(output.event_summaries)) return output.event_summaries;
    if (Array.isArray(output.timeline)) return output.timeline;
    return null;
  }

  #parseTs(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === 'bigint') return value;
    if (typeof value === 'number' && Number.isFinite(value)) return BigInt(Math.trunc(value));
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (/^\d+$/.test(trimmed)) {
        try {
          return BigInt(trimmed);
        } catch {
          return null;
        }
      }
      const parsed = Date.parse(trimmed);
      if (!Number.isNaN(parsed)) return BigInt(parsed);
    }
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

function driftOrNull(value) {
  if (!value) return null;
  return value;
}
