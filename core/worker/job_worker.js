#!/usr/bin/env node
/**
 * Job Worker — polls ledger and executes queued jobs.
 *
 * Usage:
 *   node worker/job_worker.js [--poll_ms 2000] [--max_retries 3] [--once true]
 */

import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readLatestMap, updateJob } from '../research/job_ledger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

function execNode(args, cwd) {
  return new Promise((resolve, reject) => {
    execFile('node', args, { cwd, env: process.env }, (err, stdout, stderr) => {
      if (err) {
        reject(new Error(stderr || err.message));
        return;
      }
      resolve(stdout.trim());
    });
  });
}

function shouldRetry(job, maxRetries, backoffMs) {
  if (job.status !== 'failed') return false;
  if (job.attempt >= maxRetries) return false;
  const last = Date.parse(job.created_at);
  if (Number.isNaN(last)) return true;
  return Date.now() - last >= backoffMs * Math.max(1, job.attempt);
}

async function runSweep(job) {
  const specDir = path.resolve(__dirname, '../research/specs');
  await mkdir(specDir, { recursive: true });
  const specPath = path.join(specDir, `${job.job_id}.json`);
  await writeFile(specPath, JSON.stringify(job.payload.spec, null, 2));

  const repoRoot = path.resolve(__dirname, '../..');
  const expId = job.payload.exp_id;
  await execNode([
    path.resolve(__dirname, '../../services/strategyd/eval-sweep.js'),
    '--spec',
    specPath,
    '--exp_id',
    expId,
    '--concurrency',
    String(job.payload.concurrency || 1)
  ], repoRoot);

  return {
    exp_id: expId,
    leaderboard_path: path.resolve(__dirname, '../../services/strategyd/experiments', expId, 'leaderboard.json')
  };
}

async function runPromote(job) {
  const repoRoot = path.resolve(__dirname, '../..');
  const args = [
    path.resolve(__dirname, '../../services/strategyd/promote.js'),
    '--exp_id',
    job.payload.exp_id
  ];
  if (job.payload.dry_run) {
    args.push('--dry_run', 'true');
  }
  const output = await execNode(args, repoRoot);
  return { promote_output: output };
}

async function runPack(job) {
  const repoRoot = path.resolve(__dirname, '../..');
  const args = [
    path.resolve(__dirname, '../../services/strategyd/candidate-pack.js'),
    '--candidate_id',
    job.payload.candidate_id
  ];
  if (job.payload.force) {
    args.push('--force', 'true');
  }
  await execNode(args, repoRoot);
  return {
    candidate_dir: path.resolve(__dirname, '../../services/strategyd/candidates', job.payload.candidate_id)
  };
}

async function processJob(job, maxRetries) {
  const start = Date.now();
  const attempt = (job.attempt || 0) + 1;
  console.log(`[JOB] job_id=${job.job_id} type=${job.type} status=running attempt=${attempt}`);
  await updateJob(job, 'running', { attempt });

  try {
    let artifacts = {};
    if (job.type === 'sweep') artifacts = await runSweep(job);
    if (job.type === 'promote') artifacts = await runPromote(job);
    if (job.type === 'pack') artifacts = await runPack(job);
    const duration_ms = Date.now() - start;
    console.log(`[JOB] job_id=${job.job_id} type=${job.type} status=done attempt=${attempt} duration_ms=${duration_ms}`);
    await updateJob(job, 'done', { attempt, artifacts });
    return { status: 'done' };
  } catch (err) {
    const duration_ms = Date.now() - start;
    console.log(`[JOB] job_id=${job.job_id} type=${job.type} status=failed attempt=${attempt} duration_ms=${duration_ms} error=${err.message}`);
    await updateJob(job, 'failed', { attempt, last_error: err.message });
    if (attempt >= maxRetries) return { status: 'failed' };
    return { status: 'retry' };
  }
}

async function pollOnce(maxRetries, backoffMs) {
  const latestMap = await readLatestMap();
  const jobs = Array.from(latestMap.values());
  const queued = jobs.filter(j => j.status === 'queued');
  const retryable = jobs.filter(j => shouldRetry(j, maxRetries, backoffMs));

  for (const job of [...queued, ...retryable]) {
    await processJob(job, maxRetries);
  }

  return { queued: queued.length, retryable: retryable.length };
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const pollMs = Number(args.poll_ms || 2000);
  const maxRetries = Number(args.max_retries || 3);
  const once = args.once === 'true';
  const backoffMs = Number(args.backoff_ms || 5000);

  let jobs_done_total = 0;
  let jobs_failed_total = 0;
  let jobs_queued_total = 0;

  const loop = async () => {
    const latestMap = await readLatestMap();
    const jobs = Array.from(latestMap.values());
    const queued = jobs.filter(j => j.status === 'queued');
    const retryable = jobs.filter(j => shouldRetry(j, maxRetries, backoffMs));

    jobs_queued_total += queued.length;
    for (const job of [...queued, ...retryable]) {
      const result = await processJob(job, maxRetries);
      if (result.status === 'done') jobs_done_total += 1;
      if (result.status === 'failed' || result.status === 'retry') jobs_failed_total += 1;
    }
    if (queued.length + retryable.length > 0) {
      console.log(`[METRIC] jobs_queued_total=${jobs_queued_total} jobs_done_total=${jobs_done_total} jobs_failed_total=${jobs_failed_total}`);
    }
  };

  if (once) {
    await loop();
    return;
  }

  setInterval(loop, pollMs);
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
