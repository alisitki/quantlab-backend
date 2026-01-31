/**
 * TwinPairIndexer — derive deterministic ML twin pair index.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const PAIRS_DIR = path.join(RUNS_DIR, 'pairs');
const PAIRS_INDEX = path.join(PAIRS_DIR, 'index.json');

export class TwinPairIndexer {
  async rebuild() {
    try {
      await fs.mkdir(PAIRS_DIR, { recursive: true });
      const manifests = await this.#loadManifests();
      const pairs = this.#buildPairs(manifests);
      const updated = await this.#appendPairs(pairs);
      if (updated) {
        console.log(`[TwinPairIndexer] component=strategyd action=index_updated count=${updated.length} path=${PAIRS_INDEX}`);
      }
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[TwinPairIndexer] component=strategyd action=error error=${msg}`);
    }
  }

  async #loadManifests() {
    const files = await fs.readdir(RUNS_DIR);
    const manifests = [];
    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      if (file === 'index.json') continue;
      const filePath = path.join(RUNS_DIR, file);
      const manifest = await this.#readJson(filePath);
      if (!manifest?.ended_at || !manifest?.ended_reason) continue;
      if (!manifest?.output?.state_hash) continue;
      const runId = manifest?.run_id || path.basename(file, '.json');
      manifests.push({ runId, manifest });
    }
    return manifests;
  }

  #buildPairs(items) {
    const groups = new Map();
    for (const item of items) {
      const manifest = item.manifest;
      const strategyId = manifest?.strategy?.id || manifest?.strategy_id || null;
      const seed = manifest?.strategy?.seed || null;
      const stateHash = manifest?.output?.state_hash || null;
      if (!strategyId || !seed || !stateHash) continue;
      const mode = manifest?.extra?.ml?.mode || 'off';
      if (mode !== 'off' && mode !== 'shadow') continue;
      const key = `${strategyId}::${seed}::${stateHash}`;
      const entry = groups.get(key) || { strategyId, seed, stateHash, off: null, shadow: null };
      if (mode === 'off') entry.off = entry.off || item.runId;
      if (mode === 'shadow') entry.shadow = entry.shadow || item.runId;
      groups.set(key, entry);
    }

    const pairs = [];
    for (const entry of groups.values()) {
      if (!entry.off || !entry.shadow) continue;
      const pairId = `${entry.off}__${entry.shadow}`;
      pairs.push({
        pair_id: pairId,
        run_off: entry.off,
        run_shadow: entry.shadow,
        strategy_id: entry.strategyId,
        strategy_seed: entry.seed,
        state_hash: entry.stateHash
      });
    }
    pairs.sort((a, b) => a.pair_id.localeCompare(b.pair_id));
    return pairs;
  }

  async #appendPairs(pairs) {
    if (!pairs.length) return null;
    const existing = await this.#readJson(PAIRS_INDEX);
    const list = Array.isArray(existing) ? existing.slice() : [];
    const existingIds = new Set(list.map((e) => e?.pair_id));
    let appended = false;
    for (const pair of pairs) {
      if (existingIds.has(pair.pair_id)) continue;
      list.push(pair);
      existingIds.add(pair.pair_id);
      appended = true;
    }
    if (!appended) return list;
    list.sort((a, b) => a.pair_id.localeCompare(b.pair_id));
    await fs.writeFile(PAIRS_INDEX, JSON.stringify(list, null, 2));
    return list;
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
