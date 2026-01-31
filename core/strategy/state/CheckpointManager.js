/**
 * QuantLab Strategy Runtime — Checkpoint Manager
 * 
 * PHASE 2: State & Snapshot
 * 
 * Manages persistence of runtime state snapshots for resume capability.
 * Supports local file and S3 storage.
 * 
 * Features:
 * - Save/load state snapshots
 * - Hash verification on load
 * - Atomic writes (write-then-rename)
 * - S3 support via aws-cli fallback
 * 
 * @module core/strategy/state/CheckpointManager
 */

import { promises as fs } from 'node:fs';
import { dirname, join } from 'node:path';
import { spawn } from 'node:child_process';
import { canonicalStringify, canonicalParse } from './StateSerializer.js';
import { computeHash, verifyHash } from '../safety/DeterminismValidator.js';

/**
 * @typedef {Object} CheckpointData
 * @property {Object} state - RuntimeStateSnapshot
 * @property {number} eventIndex - Event index at checkpoint
 * @property {string} checkpointId - Unique checkpoint ID
 * @property {string} stateHash - Hash of state for verification
 * @property {string} createdAt - ISO timestamp
 * @property {string} version - Checkpoint format version
 */

/**
 * Checkpoint Manager for state persistence.
 */
export class CheckpointManager {
  /** @type {string} */
  #baseDir;
  
  /** @type {string} */
  #runId;
  
  /**
   * Create a checkpoint manager.
   * 
   * @param {Object} options - Configuration
   * @param {string} [options.baseDir='/tmp/quantlab-checkpoints'] - Base directory
   * @param {string} [options.runId] - Run ID for namespacing
   */
  constructor({ baseDir = '/tmp/quantlab-checkpoints', runId } = {}) {
    this.#baseDir = baseDir;
    this.#runId = runId;
  }
  
  /**
   * Generate checkpoint path for a given ID.
   * 
   * @param {string} checkpointId - Checkpoint identifier
   * @returns {string} Full path to checkpoint file
   */
  #getPath(checkpointId) {
    const dir = this.#runId ? join(this.#baseDir, this.#runId) : this.#baseDir;
    return join(dir, `${checkpointId}.json`);
  }
  
  /**
   * Save a checkpoint to disk.
   * 
   * @param {Object} state - State snapshot to save
   * @param {string} checkpointId - Unique checkpoint ID
   * @param {number} [eventIndex=0] - Event index at checkpoint
   * @returns {Promise<{path: string, hash: string}>} Save result
   */
  async save(state, checkpointId, eventIndex = 0) {
    const stateHash = computeHash(state);
    
    /** @type {CheckpointData} */
    const checkpoint = {
      state,
      eventIndex,
      checkpointId,
      stateHash,
      createdAt: new Date().toISOString(),
      version: '1.0.0'
    };
    
    const path = this.#getPath(checkpointId);
    const tempPath = `${path}.tmp`;
    
    // Ensure directory exists
    await fs.mkdir(dirname(path), { recursive: true });
    
    // Atomic write: write to temp, then rename
    const content = canonicalStringify(checkpoint);
    await fs.writeFile(tempPath, content, 'utf8');
    await fs.rename(tempPath, path);
    
    return { path, hash: stateHash };
  }
  
  /**
   * Load a checkpoint from disk.
   * 
   * @param {string} checkpointId - Checkpoint ID to load
   * @param {Object} [options] - Load options
   * @param {boolean} [options.verifyHash=true] - Verify hash after load
   * @returns {Promise<CheckpointData>} Loaded checkpoint
   * @throws {Error} If checkpoint not found or hash mismatch
   */
  async load(checkpointId, { verifyHash: shouldVerify = true } = {}) {
    const path = this.#getPath(checkpointId);
    
    let content;
    try {
      content = await fs.readFile(path, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') {
        throw new Error(`CHECKPOINT_NOT_FOUND: ${checkpointId}`);
      }
      throw err;
    }
    
    const checkpoint = canonicalParse(content);
    
    // Verify hash if requested
    if (shouldVerify && checkpoint.stateHash) {
      const actualHash = computeHash(checkpoint.state);
      if (actualHash !== checkpoint.stateHash) {
        throw new Error(
          `CHECKPOINT_CORRUPT: Hash mismatch for ${checkpointId} - ` +
          `expected=${checkpoint.stateHash.substring(0, 8)} actual=${actualHash.substring(0, 8)}`
        );
      }
    }
    
    return checkpoint;
  }
  
  /**
   * Check if a checkpoint exists.
   * 
   * @param {string} checkpointId - Checkpoint ID
   * @returns {Promise<boolean>} True if exists
   */
  async exists(checkpointId) {
    const path = this.#getPath(checkpointId);
    try {
      await fs.access(path);
      return true;
    } catch {
      return false;
    }
  }
  
  /**
   * Delete a checkpoint.
   * 
   * @param {string} checkpointId - Checkpoint ID to delete
   * @returns {Promise<boolean>} True if deleted
   */
  async delete(checkpointId) {
    const path = this.#getPath(checkpointId);
    try {
      await fs.unlink(path);
      return true;
    } catch (err) {
      if (err.code === 'ENOENT') return false;
      throw err;
    }
  }
  
  /**
   * List all checkpoints for current run.
   * 
   * @returns {Promise<string[]>} Array of checkpoint IDs
   */
  async list() {
    const dir = this.#runId ? join(this.#baseDir, this.#runId) : this.#baseDir;
    
    try {
      const files = await fs.readdir(dir);
      return files
        .filter(f => f.endsWith('.json') && !f.endsWith('.tmp'))
        .map(f => f.replace('.json', ''));
    } catch (err) {
      if (err.code === 'ENOENT') return [];
      throw err;
    }
  }
  
  /**
   * Get the latest checkpoint based on eventIndex.
   * 
   * @returns {Promise<CheckpointData|null>} Latest checkpoint or null
   */
  async getLatest() {
    const ids = await this.list();
    if (ids.length === 0) return null;
    
    let latest = null;
    let latestIndex = -1;
    
    for (const id of ids) {
      try {
        const checkpoint = await this.load(id, { verifyHash: false });
        if (checkpoint.eventIndex > latestIndex) {
          latestIndex = checkpoint.eventIndex;
          latest = checkpoint;
        }
      } catch {
        // Skip invalid checkpoints
      }
    }
    
    // Verify hash of final result
    if (latest && latest.stateHash) {
      const actualHash = computeHash(latest.state);
      if (actualHash !== latest.stateHash) {
        throw new Error(`CHECKPOINT_CORRUPT: Latest checkpoint hash mismatch`);
      }
    }
    
    return latest;
  }
  
  /**
   * Clean up old checkpoints, keeping only the latest N.
   * 
   * @param {number} [keep=3] - Number of checkpoints to keep
   * @returns {Promise<number>} Number of checkpoints deleted
   */
  async cleanup(keep = 3) {
    const ids = await this.list();
    if (ids.length <= keep) return 0;
    
    // Load all to get eventIndex for sorting
    const checkpoints = [];
    for (const id of ids) {
      try {
        const cp = await this.load(id, { verifyHash: false });
        checkpoints.push({ id, eventIndex: cp.eventIndex });
      } catch {
        // Mark invalid for deletion
        checkpoints.push({ id, eventIndex: -1 });
      }
    }
    
    // Sort by eventIndex descending
    checkpoints.sort((a, b) => b.eventIndex - a.eventIndex);
    
    // Delete old ones
    const toDelete = checkpoints.slice(keep);
    let deleted = 0;
    for (const { id } of toDelete) {
      if (await this.delete(id)) deleted++;
    }
    
    return deleted;
  }
}

/**
 * Save checkpoint to S3.
 * Uses aws-cli for simplicity.
 * 
 * @param {Object} state - State to save
 * @param {string} s3Uri - S3 URI (s3://bucket/key)
 * @returns {Promise<{uri: string, hash: string}>} Save result
 */
export async function saveToS3(state, s3Uri) {
  const stateHash = computeHash(state);
  
  const checkpoint = {
    state,
    stateHash,
    createdAt: new Date().toISOString(),
    version: '1.0.0'
  };
  
  const content = canonicalStringify(checkpoint);
  
  return new Promise((resolve, reject) => {
    const proc = spawn('aws', ['s3', 'cp', '-', s3Uri], {
      stdio: ['pipe', 'pipe', 'pipe']
    });
    
    let stderr = '';
    proc.stderr.on('data', (data) => { stderr += data; });
    
    proc.on('close', (code) => {
      if (code === 0) {
        resolve({ uri: s3Uri, hash: stateHash });
      } else {
        reject(new Error(`S3_UPLOAD_FAILED: ${stderr}`));
      }
    });
    
    proc.stdin.write(content);
    proc.stdin.end();
  });
}

/**
 * Load checkpoint from S3.
 * 
 * @param {string} s3Uri - S3 URI
 * @returns {Promise<CheckpointData>} Loaded checkpoint
 */
export async function loadFromS3(s3Uri) {
  return new Promise((resolve, reject) => {
    const proc = spawn('aws', ['s3', 'cp', s3Uri, '-'], {
      stdio: ['pipe', 'pipe', 'pipe']
    });
    
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (data) => { stdout += data; });
    proc.stderr.on('data', (data) => { stderr += data; });
    
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`S3_DOWNLOAD_FAILED: ${stderr}`));
        return;
      }
      
      try {
        const checkpoint = canonicalParse(stdout);
        
        // Verify hash
        if (checkpoint.stateHash) {
          const actualHash = computeHash(checkpoint.state);
          if (actualHash !== checkpoint.stateHash) {
            reject(new Error(`CHECKPOINT_CORRUPT: S3 checkpoint hash mismatch`));
            return;
          }
        }
        
        resolve(checkpoint);
      } catch (err) {
        reject(new Error(`S3_PARSE_FAILED: ${err.message}`));
      }
    });
  });
}

export default CheckpointManager;
