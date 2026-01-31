/**
 * Job Scheduler — enqueue sweep/promote/pack jobs into ledger.
 */

import { enqueueJob, hashSha256, stableStringify } from '../research/job_ledger.js';

export async function enqueueSweep(spec, options = {}) {
  const inputFingerprint = hashSha256(stableStringify(spec));
  const exp_id = options.exp_id || inputFingerprint.slice(0, 12);
  const payload = {
    spec,
    exp_id,
    concurrency: options.concurrency || 1,
    input_fingerprint: inputFingerprint
  };
  const result = await enqueueJob('sweep', payload);
  console.log(`[SCHED] job_id=${result.job_id} type=sweep status=${result.enqueued ? 'queued' : 'skip'}`);
  return result;
}

export async function enqueuePromote(exp_id, options = {}) {
  const payload = {
    exp_id,
    dry_run: options.dry_run || false,
    input_fingerprint: hashSha256(exp_id)
  };
  const result = await enqueueJob('promote', payload);
  console.log(`[SCHED] job_id=${result.job_id} type=promote status=${result.enqueued ? 'queued' : 'skip'}`);
  return result;
}

export async function enqueuePack(candidate_id, options = {}) {
  const payload = {
    candidate_id,
    force: options.force || false,
    input_fingerprint: hashSha256(candidate_id)
  };
  const result = await enqueueJob('pack', payload);
  console.log(`[SCHED] job_id=${result.job_id} type=pack status=${result.enqueued ? 'queued' : 'skip'}`);
  return result;
}
