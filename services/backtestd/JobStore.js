/**
 * JobStore — disk-based backtest job storage.
 */

import fs from 'node:fs/promises';
import path from 'node:path';

export class JobStore {
  constructor({ backtestsDir }) {
    this.backtestsDir = backtestsDir;
  }

  async init() {
    await fs.mkdir(this.backtestsDir, { recursive: true });
  }

  async get(jobId) {
    const filePath = path.join(this.backtestsDir, `${jobId}.json`);
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }

  async save(job) {
    if (!job?.job_id) return null;
    const filePath = path.join(this.backtestsDir, `${job.job_id}.json`);
    const tmpPath = `${filePath}.tmp`;
    const payload = JSON.stringify(job, null, 2);
    await fs.writeFile(tmpPath, payload);
    await fs.rename(tmpPath, filePath);
    return filePath;
  }
}
