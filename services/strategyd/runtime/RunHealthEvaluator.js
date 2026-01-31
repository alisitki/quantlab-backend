/**
 * RunHealthEvaluator — derive deterministic health summaries from manifests.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const HEALTH_DIR = path.join(RUNS_DIR, 'health');

export class RunHealthEvaluator {
  #onRebuildSummary = null;

  constructor({ onRebuildSummary } = {}) {
    this.#onRebuildSummary = onRebuildSummary || null;
  }

  async evaluateAll() {
    try {
      await fs.mkdir(HEALTH_DIR, { recursive: true });
      const files = await fs.readdir(RUNS_DIR);
      const manifests = files.filter((f) => f.endsWith('.json') && f !== 'index.json');
      for (const file of manifests) {
        await this.#evaluateFile(path.join(RUNS_DIR, file));
      }
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[RunHealthEvaluator] component=strategyd action=error error=${msg}`);
    }
  }

  async evaluateManifest(manifestPath, manifest) {
    try {
      await fs.mkdir(HEALTH_DIR, { recursive: true });
      const entry = this.#buildEntry(manifest, manifestPath);
      if (!entry) return;
      await this.#writeEntry(entry);
      if (this.#onRebuildSummary) {
        this.#onRebuildSummary(entry.run_id).catch(() => {});
      }
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[RunHealthEvaluator] component=strategyd action=error error=${msg}`);
    }
  }

  async #evaluateFile(filePath) {
    let manifest;
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      manifest = JSON.parse(raw);
    } catch (err) {
      const msg = err?.message || 'invalid_manifest';
      console.error(`[RunHealthEvaluator] component=strategyd action=skipped reason=invalid_manifest path=${filePath} error=${msg}`);
      return;
    }

    const entry = this.#buildEntry(manifest, filePath);
    if (!entry) return;
    await this.#writeEntry(entry);
    if (this.#onRebuildSummary) {
      this.#onRebuildSummary(entry.run_id).catch(() => {});
    }
  }

  #buildEntry(manifest, manifestPath) {
    const runId = manifest?.run_id || (manifestPath ? path.basename(manifestPath, '.json') : null);
    if (!runId) {
      console.error(`[RunHealthEvaluator] component=strategyd action=skipped reason=missing_run_id path=${manifestPath || 'unknown'}`);
      return null;
    }

    if (!manifest?.ended_at || !manifest?.ended_reason) {
      console.log(`[RunHealthEvaluator] run_id=${runId} action=skipped reason=active_run`);
      return null;
    }

    const totalEvents = typeof manifest?.output?.event_count === 'number' ? manifest.output.event_count : null;
    const totalSignals = typeof manifest?.output?.signal_count === 'number' ? manifest.output.signal_count : null;
    const totalFills = typeof manifest?.output?.fills_count === 'number' ? manifest.output.fills_count : null;

    const endedReason = manifest.ended_reason;
    const queueOverflow = endedReason === 'queue_overflow';
    const backpressureDisconnects = typeof manifest?.output?.backpressure_disconnects === 'number'
      ? manifest.output.backpressure_disconnects
      : null;

    const durationMs = this.#computeDurationMs(manifest.started_at, manifest.ended_at);

    let healthClass = 'degraded';
    if (queueOverflow) {
      healthClass = 'failed';
    } else if (endedReason !== 'finished') {
      healthClass = 'degraded';
    } else if (typeof totalSignals === 'number' && totalSignals > 0 && typeof totalFills === 'number' && totalFills >= 0) {
      healthClass = 'healthy';
    } else {
      healthClass = 'degraded';
    }

    return {
      run_id: runId,
      ended_reason: endedReason,
      total_events: totalEvents,
      total_signals: totalSignals,
      total_fills: totalFills,
      queue_overflow: queueOverflow,
      backpressure_disconnects: backpressureDisconnects,
      run_duration_ms: durationMs,
      class: healthClass
    };
  }

  #computeDurationMs(startedAt, endedAt) {
    if (!startedAt || !endedAt) return null;
    const start = new Date(startedAt).getTime();
    const end = new Date(endedAt).getTime();
    if (Number.isNaN(start) || Number.isNaN(end) || end < start) return null;
    return end - start;
  }

  async #writeEntry(entry) {
    const outPath = path.join(HEALTH_DIR, `${entry.run_id}.json`);
    await fs.writeFile(outPath, JSON.stringify(entry, null, 2));
    console.log(`[RunHealthEvaluator] run_id=${entry.run_id} component=strategyd action=health_written class=${entry.class} path=${outPath}`);
  }
}
