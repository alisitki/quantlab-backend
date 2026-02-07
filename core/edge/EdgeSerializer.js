/**
 * EdgeSerializer - Bidirectional serialization of Edge objects
 *
 * Problem: Edge objects contain closures (entryCondition, exitCondition) that cannot
 * be JSON serialized. We need to persist edges between pipeline steps.
 *
 * Solution: Store edge definitions (pattern + testResult) alongside edge metadata.
 * On deserialization, reconstruct closures using EdgeCandidateGenerator.
 */

import { EdgeCandidateGenerator } from './discovery/EdgeCandidateGenerator.js';
import { EdgeRegistry } from './EdgeRegistry.js';
import fs from 'node:fs/promises';
import path from 'node:path';

export class EdgeSerializer {
  constructor() {
    this.generator = new EdgeCandidateGenerator();
  }

  /**
   * Serialize EdgeRegistry to JSON-safe object
   * @param {EdgeRegistry} registry
   * @returns {Object} Serializable object
   */
  serialize(registry) {
    const registryData = registry.toJSON();

    return {
      version: 1,
      timestamp: Date.now(),
      edges: registryData.edges, // Already includes definitions
      stats: registryData.stats
    };
  }

  /**
   * Deserialize JSON to EdgeRegistry with reconstructed closures
   * @param {Object} serialized
   * @returns {EdgeRegistry} Registry with live Edge instances
   */
  deserialize(serialized) {
    if (serialized.version !== 1) {
      throw new Error(`EdgeSerializer: Unsupported version ${serialized.version}`);
    }

    const registry = new EdgeRegistry();

    for (const entry of serialized.edges) {
      if (!entry.definition) {
        console.warn(`EdgeSerializer: Edge ${entry.id} missing definition, skipping`);
        continue;
      }

      const { pattern, testResult } = entry.definition;

      // Reconstruct Edge with live closures
      const edge = this.generator.generate(pattern, testResult);

      // Override with serialized metadata (status, stats, confidence)
      edge.status = entry.status;
      edge.stats = entry.stats;
      edge.confidence = entry.confidence;
      edge.discovered = entry.discovered;

      // Register with original definition
      registry.register(edge, entry.definition);
    }

    return registry;
  }

  /**
   * Save EdgeRegistry to file (atomic write)
   * @param {string} filePath
   * @param {EdgeRegistry} registry
   */
  async saveToFile(filePath, registry) {
    const serialized = this.serialize(registry);
    const content = JSON.stringify(serialized, null, 2);

    // Ensure directory exists
    const dir = path.dirname(filePath);
    await fs.mkdir(dir, { recursive: true });

    // Atomic write: write to temp, fsync, rename
    const tmpPath = `${filePath}.tmp`;

    const fileHandle = await fs.open(tmpPath, 'w');
    await fileHandle.write(content);
    await fileHandle.sync(); // fsync
    await fileHandle.close();

    await fs.rename(tmpPath, filePath); // atomic

    console.log(`[EdgeSerializer] Saved ${serialized.edges.length} edges to ${filePath}`);
  }

  /**
   * Load EdgeRegistry from file
   * @param {string} filePath
   * @returns {Promise<EdgeRegistry>}
   */
  async loadFromFile(filePath) {
    const content = await fs.readFile(filePath, 'utf-8');
    const serialized = JSON.parse(content);

    console.log(`[EdgeSerializer] Loading ${serialized.edges.length} edges from ${filePath}`);

    const registry = this.deserialize(serialized);

    console.log(`[EdgeSerializer] Loaded ${registry.size()} edges`);

    return registry;
  }
}
