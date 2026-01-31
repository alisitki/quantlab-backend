/**
 * ArchiveExporter — append-only exporter for derived artifacts.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const REPORT_DIR = path.join(RUNS_DIR, 'report');
const ARCHIVE_DIR = path.join(RUNS_DIR, 'archive');

export class ArchiveExporter {
  async exportFiles(filePaths = []) {
    const dateKey = this.#todayKey();
    const destDir = path.join(ARCHIVE_DIR, dateKey);
    await fs.mkdir(destDir, { recursive: true });

    const archived = [];
    for (const filePath of filePaths) {
      if (!filePath) continue;
      const base = path.basename(filePath);
      const destPath = path.join(destDir, base);
      try {
        await fs.access(destPath);
        continue;
      } catch {
        // proceed
      }
      try {
        await fs.copyFile(filePath, destPath);
        archived.push(destPath);
      } catch (err) {
        const msg = err?.message || 'copy_failed';
        console.error(`[ArchiveExporter] action=error file=${filePath} error=${msg}`);
      }
    }

    return archived;
  }

  async resolveArtifacts({ seed, strategyId }) {
    const files = await fs.readdir(REPORT_DIR);
    const matches = files.filter((f) => f.endsWith('.json'));
    matches.sort();

    const triad = this.#pickLatest(matches, 'triad_', seed, strategyId);
    const decision = this.#pickLatest(matches, 'decision_', seed, strategyId);
    const activeConfig = this.#pickLatest(matches, 'active_config_', seed, strategyId);

    return {
      triadPath: triad ? path.join(REPORT_DIR, triad) : null,
      decisionPath: decision ? path.join(REPORT_DIR, decision) : null,
      activeConfigPath: activeConfig ? path.join(REPORT_DIR, activeConfig) : null
    };
  }

  #pickLatest(files, prefix, seed, strategyId) {
    const filtered = files.filter((f) => f.startsWith(prefix));
    let subset = filtered;
    if (seed) {
      subset = subset.filter((f) => f.includes(`_${seed}.json`));
    } else if (strategyId) {
      subset = subset.filter((f) => f.includes(`_${strategyId}`));
    }
    if (!subset.length) return null;
    subset.sort();
    return subset[subset.length - 1];
  }

  #todayKey() {
    const now = new Date();
    const yyyy = now.getUTCFullYear();
    const mm = String(now.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(now.getUTCDate()).padStart(2, '0');
    return `${yyyy}${mm}${dd}`;
  }
}
