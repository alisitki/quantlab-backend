#!/usr/bin/env node
/**
 * run-job.js: CLI tool to execute ML training jobs.
 */
import fs from 'fs';
import path from 'path';
import { JobSpec } from './JobSpec.js';
import { MLRuntime } from './MLRuntime.js';

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.log('Usage: node run-job.js <job.json>');
    process.exit(1);
  }

  const jobPath = path.resolve(args[0]);
  if (!fs.existsSync(jobPath)) {
    console.error(`Error: Job file not found: ${jobPath}`);
    process.exit(1);
  }

  try {
    const raw = fs.readFileSync(jobPath, 'utf8');
    const specData = JSON.parse(raw);
    
    const jobSpec = new JobSpec(specData);
    console.log(`[CLI] Starting Job: ${jobSpec.jobId} (Hash: ${jobSpec.getHash()})`);
    
    const result = await MLRuntime.run(jobSpec);

    console.log('\n--- Job Summary ---');
    console.log(`ID: ${result.jobId}`);
    console.log(`Backend: ${result.backendUsed}`);
    console.log(`Artifacts: ${result.artifactDir}`);
    console.log(`Accuracy: ${(result.metrics.accuracy * 100).toFixed(2)}%`);
    console.log('-------------------\n');

  } catch (err) {
    console.error('[CLI] Failed to run job:', err);
    process.exit(1);
  }
}

main();
