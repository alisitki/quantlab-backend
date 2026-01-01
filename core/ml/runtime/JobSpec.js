/**
 * JobSpec: Defines the contract for a training job.
 * Ensures reproducibility and deterministic hashing.
 */
import crypto from 'crypto';

export class JobSpec {
  /**
   * @param {Object} spec
   * @param {string} spec.jobId
   * @param {Object} spec.dataset
   * @param {Object} spec.model
   * @param {Object} spec.runtime
   * @param {Object} [spec.output]
   */
  constructor(spec) {
    this.jobId = spec.jobId || crypto.randomUUID();
    this.dataset = spec.dataset;
    this.model = spec.model;
    this.runtime = spec.runtime || { backend: 'cpu' };
    this.output = spec.output || {
      artifactPath: `./ml/artifacts/jobs/${this.jobId}/model.bin`,
      metricsPath: `./ml/artifacts/jobs/${this.jobId}/metrics.json`
    };

    this.validate();
  }

  /**
   * Validate schema
   */
  validate() {
    if (!this.dataset || !this.dataset.symbol) {
      throw new Error('JobSpec: dataset.symbol is required');
    }
    if (!this.dataset.featurePath) {
      throw new Error('JobSpec: dataset.featurePath is required');
    }
    if (!this.model || !this.model.type) {
      throw new Error('JobSpec: model.type is required');
    }
    if (!['cpu', 'gpu'].includes(this.runtime.backend)) {
      throw new Error('JobSpec: runtime.backend must be "cpu" or "gpu"');
    }
  }

  /**
   * Generate stable hash for the job identity
   */
  getHash() {
    const data = JSON.stringify({
      dataset: this.dataset,
      model: this.model,
      runtime: {
        backend: this.runtime.backend,
        gpuType: this.runtime.gpuType
      }
    });
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  /**
   * To plain JSON
   */
  toJSON() {
    return {
      jobId: this.jobId,
      dataset: this.dataset,
      model: this.model,
      runtime: this.runtime,
      output: this.output,
      hash: this.getHash()
    };
  }
}
