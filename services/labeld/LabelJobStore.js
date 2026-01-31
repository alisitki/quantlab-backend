/**
 * LabelJobStore — disk-based job storage.
 */

import fs from 'node:fs/promises';
import path from 'node:path';

export class LabelJobStore {
  constructor({ jobsDir }) {
    this.jobsDir = jobsDir;
  }

  async init() {
    await fs.mkdir(this.jobsDir, { recursive: true });
  }

  async get(jobId) {
    const filePath = path.join(this.jobsDir, jobId, 'job.json');
    try {
      const raw = await fs.readFile(filePath, 'utf8');
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }

  async save(job) {
    if (!job?.job_id) return null;
    const dir = path.join(this.jobsDir, job.job_id);
    await fs.mkdir(dir, { recursive: true });
    const filePath = path.join(dir, 'job.json');
    const tmpPath = `${filePath}.tmp`;
    const payload = JSON.stringify(job, null, 2);
    await fs.writeFile(tmpPath, payload);
    await fs.rename(tmpPath, filePath);
    return filePath;
  }
}
