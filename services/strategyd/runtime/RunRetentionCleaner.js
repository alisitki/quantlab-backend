/**
 * RunRetentionCleaner — local runs retention cleanup (sidecar).
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { RUN_ARCHIVE_RETENTION_DAYS, CLEANER_INTERVAL_MS } from './constants.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');

const SAFE_ENDED_REASONS = new Set(['finished', 'stop', 'stop_at_index', 'kill']);

export class RunRetentionCleaner {
  #running = false;
  #timer = null;
  #onRebuildIndex = null;

  constructor({ onRebuildIndex } = {}) {
    this.#onRebuildIndex = onRebuildIndex || null;
  }

  start() {
    this.runOnce().catch(() => {});
    if (!this.#timer) {
      this.#timer = setInterval(() => {
        this.runOnce().catch(() => {});
      }, CLEANER_INTERVAL_MS);
      if (typeof this.#timer.unref === 'function') {
        this.#timer.unref();
      }
    }
  }

  async runOnce() {
    if (this.#running) return;
    this.#running = true;
    let deletedAny = false;
    try {
      await fs.mkdir(RUNS_DIR, { recursive: true });
      const files = await fs.readdir(RUNS_DIR);
      const manifests = files.filter((f) => f.endsWith('.json') && !f.endsWith('.json.gz'));
      for (const file of manifests) {
        const deleted = await this.#processManifest(path.join(RUNS_DIR, file));
        if (deleted) deletedAny = true;
      }
      if (deletedAny && this.#onRebuildIndex) {
        this.#onRebuildIndex().catch(() => {});
      }
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[RunRetentionCleaner] component=strategyd action=error error=${msg}`);
    } finally {
      this.#running = false;
    }
  }

  async #processManifest(filePath) {
    let manifest;
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      manifest = JSON.parse(raw);
    } catch (err) {
      const msg = err?.message || 'read_failed';
      console.error(`[RunRetentionCleaner] component=strategyd action=error path=${filePath} error=${msg}`);
      return false;
    }

    const runId = manifest?.run_id || path.basename(filePath, '.json');

    if (!manifest?.ended_at || !manifest?.ended_reason) {
      console.log(`[RunRetentionCleaner] run_id=${runId} action=skipped reason=active_run`);
      return false;
    }

    if (!SAFE_ENDED_REASONS.has(manifest.ended_reason)) {
      console.log(`[RunRetentionCleaner] run_id=${runId} action=skipped reason=ended_reason_blocked ended_reason=${manifest.ended_reason}`);
      return false;
    }

    const endedAt = new Date(manifest.ended_at);
    if (Number.isNaN(endedAt.getTime())) {
      console.log(`[RunRetentionCleaner] run_id=${runId} action=skipped reason=invalid_ended_at`);
      return false;
    }

    const ageDays = (Date.now() - endedAt.getTime()) / (24 * 60 * 60 * 1000);
    if (ageDays < RUN_ARCHIVE_RETENTION_DAYS) {
      console.log(`[RunRetentionCleaner] run_id=${runId} action=skipped reason=retention_not_met age_days=${ageDays.toFixed(2)}`);
      return false;
    }

    const archiveEnabled = process.env.RUN_ARCHIVE_ENABLED === '1';
    const gzPath = `${filePath}.gz`;
    let gzExists = false;
    try {
      await fs.access(gzPath);
      gzExists = true;
    } catch {
      gzExists = false;
    }

    if (archiveEnabled && !gzExists) {
      console.log(`[RunRetentionCleaner] run_id=${runId} action=skipped reason=not_archived age_days=${ageDays.toFixed(2)}`);
      return false;
    }

    try {
      await fs.unlink(filePath);
      if (gzExists) {
        await fs.unlink(gzPath);
      }
      console.log(`[RunRetentionCleaner] run_id=${runId} action=deleted age_days=${ageDays.toFixed(2)}`);
      return true;
    } catch (err) {
      const msg = err?.message || 'delete_failed';
      console.error(`[RunRetentionCleaner] run_id=${runId} action=error error=${msg}`);
      return false;
    }
  }
}
