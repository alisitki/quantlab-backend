/**
 * RunIndexBuilder — rebuilds runs index from local manifests.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const INDEX_PATH = path.join(RUNS_DIR, 'index.json');

export class RunIndexBuilder {
  #onRebuildSummary = null;
  #onIndexRebuilt = null;

  constructor({ onRebuildSummary, onIndexRebuilt } = {}) {
    this.#onRebuildSummary = onRebuildSummary || null;
    this.#onIndexRebuilt = onIndexRebuilt || null;
  }

  async rebuild() {
    try {
      await fs.mkdir(RUNS_DIR, { recursive: true });
      const files = await fs.readdir(RUNS_DIR);
      const manifests = files.filter((f) => f.endsWith('.json') && f !== 'index.json');

      const entries = [];
      for (const file of manifests) {
        const filePath = path.join(RUNS_DIR, file);
        const entry = await this.#readManifest(filePath);
        if (entry) entries.push(entry);
      }

      entries.sort((a, b) => a.run_id.localeCompare(b.run_id));

      const payload = entries.map((entry) => ({
        run_id: entry.run_id,
        started_at: entry.started_at,
        ended_at: entry.ended_at,
        ended_reason: entry.ended_reason,
        strategy_id: entry.strategy_id,
        stream: entry.stream,
        symbol: entry.symbol,
        state_hash: entry.state_hash
      }));

      await fs.writeFile(INDEX_PATH, JSON.stringify(payload, null, 2));
      console.log(`[RunIndexBuilder] component=strategyd action=rebuild count=${payload.length} path=${INDEX_PATH}`);
      if (this.#onIndexRebuilt) {
        this.#onIndexRebuilt().catch(() => {});
      }
      if (this.#onRebuildSummary) {
        this.#onRebuildSummary().catch(() => {});
      }
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[RunIndexBuilder] component=strategyd action=error error=${msg}`);
    }
  }

  async #readManifest(filePath) {
    let manifest;
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      manifest = JSON.parse(raw);
    } catch (err) {
      const msg = err?.message || 'invalid_manifest';
      console.error(`[RunIndexBuilder] component=strategyd action=skipped reason=invalid_manifest path=${filePath} error=${msg}`);
      return null;
    }

    const runId = manifest?.run_id;
    if (!runId) {
      console.error(`[RunIndexBuilder] component=strategyd action=skipped reason=missing_run_id path=${filePath}`);
      return null;
    }

    return {
      run_id: runId,
      started_at: manifest.started_at || null,
      ended_at: manifest.ended_at || null,
      ended_reason: manifest.ended_reason || null,
      strategy_id: manifest.strategy_id || manifest.strategy?.id || null,
      stream: manifest.input?.dataset || null,
      symbol: manifest.input?.symbol || null,
      state_hash: manifest.output?.state_hash || null
    };
  }
}
