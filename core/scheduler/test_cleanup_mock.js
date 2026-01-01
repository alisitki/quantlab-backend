#!/usr/bin/env node
/**
 * test_cleanup_mock.js: Mock test for cleanup_retention logic.
 * Simulates S3 scan and audit logs to generate a plan JSON with S3 candidates.
 */
import fs from 'fs';
import path from 'path';

// --- MOCK DATA ---
const mockAuditLog = `
{"timestamp":"2023-12-20T00:00:00Z","symbol":"btcusdt","date":"20231220","job_id":"job-failed-123","training_status":"FAILED","hash_changed":false}
{"timestamp":"2023-11-20T00:00:00Z","symbol":"btcusdt","date":"20231120","job_id":"job-rejected-456","training_status":"SUCCESS","hash_changed":false}
{"timestamp":"2023-12-25T00:00:00Z","symbol":"btcusdt","date":"20231225","job_id":"job-promoted-789","training_status":"SUCCESS","hash_changed":true}
`.trim();

const mockS3Artifacts = [
  { Key: 'ml-artifacts/job-failed-123/model.bin', LastModified: '2023-12-20T00:00:00Z' },
  { Key: 'ml-artifacts/job-rejected-456/model.bin', LastModified: '2023-11-20T00:00:00Z' },
  { Key: 'ml-artifacts/job-promoted-789/model.bin', LastModified: '2023-12-25T00:00:00Z' },
  { Key: 'ml-artifacts/stale-untracked/model.bin', LastModified: '2023-11-01T00:00:00Z' }
];

const mockS3Leases = [
  { Key: 'ml-leases/lease-old.json', LastModified: '2023-12-01T00:00:00Z' }
];

// --- MOCK LOGIC (based on cleanup_retention.js) ---
const CATEGORIES = {
  S3_PROMOTED: { ttlDays: Infinity, minTTL: Infinity, label: 'Promoted ML Jobs (S3)' },
  S3_REJECTED: { ttlDays: 30, minTTL: 7, label: 'Rejected ML Jobs (S3)' },
  S3_FAILED:   { ttlDays: 7, minTTL: 7, label: 'Failed ML Jobs (S3)' },
  S3_STALE_LEASE: { ttlDays: 1, minTTL: 0, label: 'Stale Leases (S3)' }
};

function isOlderThan(lastModified, days) {
  const diff = Date.now() - new Date(lastModified).getTime();
  return diff > (days * 24 * 60 * 60 * 1000);
}

const jobMap = new Map();
mockAuditLog.split('\n').forEach(line => {
  const entry = JSON.parse(line);
  jobMap.set(entry.job_id, {
    status: entry.training_status,
    promoted: entry.training_status === 'SUCCESS' && entry.hash_changed === true,
    timestamp: new Date(entry.timestamp).getTime()
  });
});

const candidates = [];

// 1. Process ml-artifacts
const jobDirs = new Map();
mockS3Artifacts.forEach(item => {
  const jobId = item.Key.split('/')[1];
  if (!jobDirs.has(jobId)) jobDirs.set(jobId, { keys: [], lastModified: item.LastModified });
  jobDirs.get(jobId).keys.push(item.Key);
});

for (const [jobId, info] of jobDirs) {
  const status = jobMap.get(jobId);
  let category = status ? (status.promoted ? 'S3_PROMOTED' : (status.status === 'SUCCESS' ? 'S3_REJECTED' : 'S3_FAILED')) : 'S3_FAILED';
  
  const config = CATEGORIES[category];
  if (!config || config.ttlDays === Infinity) continue;
  
  if (isOlderThan(info.lastModified, config.ttlDays)) {
    candidates.push({ type: 'S3', category, label: config.label, keys: info.keys, bucket: 'quantlab-artifacts', job_id: jobId });
  }
}

// 2. Process ml-leases
mockS3Leases.forEach(lease => {
  if (isOlderThan(lease.LastModified, CATEGORIES.S3_STALE_LEASE.ttlDays)) {
    candidates.push({ type: 'S3_KEY', category: 'S3_STALE_LEASE', label: CATEGORIES.S3_STALE_LEASE.label, keys: [lease.Key], bucket: 'quantlab-artifacts' });
  }
});

const plan = {
  timestamp: new Date().toISOString(),
  mode: 'DRY-RUN',
  candidates
};

console.log(JSON.stringify(plan, null, 2));
