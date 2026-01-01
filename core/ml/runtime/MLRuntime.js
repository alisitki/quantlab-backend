/**
 * MLRuntime: Orchestrates job execution using backends.
 */
import fs from 'fs';
import path from 'path';
import { CpuBackend } from './backends/CpuBackend.js';
import { GpuBackend } from './backends/GpuBackend.js';

export class MLRuntime {
  /**
   * Run a training job according to its JobSpec.
   * @param {JobSpec} jobSpec 
   */
  static async run(jobSpec) {
    const startTime = Date.now();
    const backendType = jobSpec.runtime.backend;
    
    // 1. Select Backend
    let backend;
    if (backendType === 'gpu') {
      backend = new GpuBackend();
    } else {
      backend = new CpuBackend();
    }

    try {
      // 2. Prepare
      await backend.prepare(jobSpec);

      // 3. Execute
      const result = await backend.run(jobSpec);

      // 4. Persistence of Meta Artifacts
      const endTime = Date.now();
      const runtimeInfo = {
        backend: backendType,
        hostname: process.env.HOSTNAME || 'localhost',
        startTimestamp: new Date(startTime).toISOString(),
        endTimestamp: new Date(endTime).toISOString(),
        durationMs: endTime - startTime
      };

      const outDir = path.dirname(jobSpec.output.artifactPath);
      fs.writeFileSync(path.join(outDir, 'job.json'), JSON.stringify(jobSpec.toJSON(), null, 2));
      fs.writeFileSync(path.join(outDir, 'runtime.json'), JSON.stringify(runtimeInfo, null, 2));

      console.log(`[MLRuntime] Job ${jobSpec.jobId} completed in ${(endTime - startTime) / 1000}s`);
      
      return {
        jobId: jobSpec.jobId,
        backendUsed: backendType,
        artifactDir: outDir,
        metrics: result.metrics
      };
    } catch (err) {
      console.error(`[MLRuntime] Job ${jobSpec.jobId} failed:`, err);
      throw err;
    } finally {
      // 5. Cleanup
      await backend.cleanup(jobSpec);
    }
  }
}
