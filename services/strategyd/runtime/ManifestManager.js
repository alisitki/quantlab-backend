/**
 * ManifestManager — Handles Run Manifest I/O
 * 
 * Saves run details to services/strategyd/runs/<run_id>.json
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');

export class ManifestManager {
  /**
   * Ensure runs directory exists
   */
  async init() {
    try {
      await fs.mkdir(RUNS_DIR, { recursive: true });
    } catch (err) {
      console.error('[ManifestManager] Failed to create runs dir:', err.message);
    }
  }

  /**
   * Save a run manifest
   * @param {Object} manifest 
   */
  async save(manifest) {
    if (!manifest.run_id) return;
    
    const filePath = path.join(RUNS_DIR, `${manifest.run_id}.json`);
    try {
      await fs.writeFile(filePath, JSON.stringify(manifest, null, 2));
      return filePath;
    } catch (err) {
      console.error(`[ManifestManager] Failed to save ${manifest.run_id}:`, err.message);
    }
  }

  /**
   * Get a specific manifest
   * @param {string} runId 
   */
  async get(runId) {
    const filePath = path.join(RUNS_DIR, `${runId}.json`);
    try {
      const data = await fs.readFile(filePath, 'utf-8');
      return JSON.parse(data);
    } catch (err) {
      return null;
    }
  }

  /**
   * List recent runs
   * @param {number} limit 
   */
  async list(limit = 20) {
    try {
      const files = await fs.readdir(RUNS_DIR);
      // Filter for json files and sort descending by name (id usually contains date)
      const manifests = files
        .filter(f => f.endsWith('.json'))
        .sort()
        .reverse()
        .slice(0, limit);
        
      return manifests.map(f => f.replace('.json', ''));
    } catch (err) {
      return [];
    }
  }
}
