/**
 * RunSummaryBuilder — derive per-run summary snapshots.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const INDEX_PATH = path.join(RUNS_DIR, 'index.json');
const HEALTH_DIR = path.join(RUNS_DIR, 'health');
const SUMMARY_DIR = path.join(RUNS_DIR, 'summary');

export class RunSummaryBuilder {
  async rebuildAll() {
    try {
      await fs.mkdir(SUMMARY_DIR, { recursive: true });
      const files = await fs.readdir(RUNS_DIR);
      const manifests = files.filter((f) => f.endsWith('.json') && f !== 'index.json');
      const runIds = [];

      for (const file of manifests) {
        const filePath = path.join(RUNS_DIR, file);
        const manifest = await this.#readJson(filePath, 'invalid_manifest');
        if (!manifest) continue;
        if (!manifest.ended_at || !manifest.ended_reason) continue;
        const runId = manifest.run_id || path.basename(file, '.json');
        if (runId) runIds.push(runId);
      }

      runIds.sort();
      for (const runId of runIds) {
        await this.rebuildRun(runId);
      }

      console.log(`[RunSummaryBuilder] component=strategyd action=rebuild_all count=${runIds.length} path=${SUMMARY_DIR}`);
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[RunSummaryBuilder] component=strategyd action=error error=${msg}`);
    }
  }

  async rebuildRun(runId) {
    if (!runId) return;
    try {
      await fs.mkdir(SUMMARY_DIR, { recursive: true });

      const manifestPath = path.join(RUNS_DIR, `${runId}.json`);
      const manifest = await this.#readJson(manifestPath, 'missing_manifest');
      if (!manifest) {
        console.log(`[RunSummaryBuilder] run_id=${runId} action=skipped reason=missing_manifest`);
        return;
      }
      if (!manifest.ended_at || !manifest.ended_reason) {
        console.log(`[RunSummaryBuilder] run_id=${runId} action=skipped reason=active_run`);
        return;
      }

      const healthPath = path.join(HEALTH_DIR, `${runId}.json`);
      const health = await this.#readJson(healthPath, 'missing_health');

      const index = await this.#readJson(INDEX_PATH, 'missing_index');
      const indexEntry = Array.isArray(index) ? index.find((entry) => entry?.run_id === runId) : null;

      const summary = {
        run_id: runId,
        started_at: manifest.started_at || null,
        ended_at: manifest.ended_at || null,
        ended_reason: manifest.ended_reason || null,
        health_class: health?.class || null,
        duration_ms: health?.run_duration_ms ?? this.#computeDurationMs(manifest.started_at, manifest.ended_at),
        total_events: health?.total_events ?? manifest?.output?.event_count ?? null,
        total_signals: health?.total_signals ?? manifest?.output?.signal_count ?? null,
        total_fills: health?.total_fills ?? manifest?.output?.fills_count ?? null,
        state_hash: manifest?.output?.state_hash ?? null,
        strategy_id: manifest?.strategy_id ?? manifest?.strategy?.id ?? indexEntry?.strategy_id ?? null,
        stream: manifest?.input?.dataset ?? indexEntry?.stream ?? null,
        symbol: manifest?.input?.symbol ?? indexEntry?.symbol ?? null
      };

      const outPath = path.join(SUMMARY_DIR, `${runId}.json`);
      await fs.writeFile(outPath, JSON.stringify(summary, null, 2));
      console.log(`[RunSummaryBuilder] run_id=${runId} component=strategyd action=summary_written path=${outPath}`);
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[RunSummaryBuilder] run_id=${runId} component=strategyd action=error error=${msg}`);
    }
  }

  async #readJson(filePath, reason) {
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      return JSON.parse(raw);
    } catch (err) {
      if (reason === 'missing_health' || reason === 'missing_index') {
        return null;
      }
      const msg = err?.message || reason;
      console.error(`[RunSummaryBuilder] component=strategyd action=skipped reason=${reason} path=${filePath} error=${msg}`);
      return null;
    }
  }

  #computeDurationMs(startedAt, endedAt) {
    if (!startedAt || !endedAt) return null;
    const start = new Date(startedAt).getTime();
    const end = new Date(endedAt).getTime();
    if (Number.isNaN(start) || Number.isNaN(end) || end < start) return null;
    return end - start;
  }
}
