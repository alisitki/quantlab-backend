/**
 * Job Ledger — append-only JSONL store for research jobs.
 */

import { readFile, appendFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const LEDGER_PATH = path.join(__dirname, 'jobs.jsonl');

export function stableStringify(obj) {
  if (obj === null || typeof obj !== 'object') return JSON.stringify(obj);
  if (Array.isArray(obj)) return '[' + obj.map(stableStringify).join(',') + ']';
  const keys = Object.keys(obj).sort();
  return '{' + keys.map(k => `"${k}":${stableStringify(obj[k])}`).join(',') + '}';
}

export function hashSha256(input) {
  return createHash('sha256').update(input).digest('hex');
}

export function computeJobId(type, payload, inputFingerprint) {
  const base = `${type}:${stableStringify(payload)}:${inputFingerprint || ''}`;
  return hashSha256(base);
}

export async function ensureLedgerDir() {
  await mkdir(path.dirname(LEDGER_PATH), { recursive: true });
}

export async function appendEntry(entry) {
  await ensureLedgerDir();
  await appendFile(LEDGER_PATH, JSON.stringify(entry) + '\n');
}

export async function readAllEntries() {
  try {
    const raw = await readFile(LEDGER_PATH, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  } catch {
    return [];
  }
}

export async function readLatestMap() {
  const entries = await readAllEntries();
  const map = new Map();
  for (const e of entries) {
    map.set(e.job_id, e);
  }
  return map;
}

export async function enqueueJob(type, payload) {
  const inputFingerprint = payload.input_fingerprint || hashSha256(stableStringify(payload));
  const jobId = computeJobId(type, payload, inputFingerprint);
  const latestMap = await readLatestMap();
  if (latestMap.has(jobId)) {
    return { job_id: jobId, enqueued: false, existing: latestMap.get(jobId) };
  }
  const entry = {
    job_id: jobId,
    created_at: new Date().toISOString(),
    type,
    payload: { ...payload, input_fingerprint: inputFingerprint },
    status: 'queued',
    attempt: 0,
    last_error: null,
    artifacts: {}
  };
  await appendEntry(entry);
  return { job_id: jobId, enqueued: true, entry };
}

export async function updateJob(job, status, updates = {}) {
  const entry = {
    job_id: job.job_id,
    created_at: new Date().toISOString(),
    type: job.type,
    payload: job.payload,
    status,
    attempt: updates.attempt ?? job.attempt ?? 0,
    last_error: updates.last_error ?? null,
    artifacts: updates.artifacts ?? job.artifacts ?? {}
  };
  await appendEntry(entry);
  return entry;
}
