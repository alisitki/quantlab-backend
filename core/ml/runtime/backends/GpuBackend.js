/**
 * GpuBackend: v1 STUB.
 * Logs GPU requirements and delegates to CpuBackend.
 */
import { CpuBackend } from './CpuBackend.js';

export class GpuBackend {
  #cpuDelegate = new CpuBackend();

  async prepare(jobSpec) {
    console.log(`[GpuBackend] STUB: Prepare for GPU job ${jobSpec.jobId}`);
    console.log(`[GpuBackend] Requested GPU: ${jobSpec.runtime.gpuType || 'Default (T4)'}`);
    console.log(`[GpuBackend] Max Runtime: ${jobSpec.runtime.maxRuntimeMin || 'Unlimited'} min`);
    
    await this.#cpuDelegate.prepare(jobSpec);
  }

  async run(jobSpec) {
    console.log(`[GpuBackend] STUB: Executing on CPU for now...`);
    return await this.#cpuDelegate.run(jobSpec);
  }

  async cleanup(jobSpec) {
    console.log(`[GpuBackend] STUB: Cleanup for GPU job ${jobSpec.jobId}`);
    await this.#cpuDelegate.cleanup(jobSpec);
  }
}
