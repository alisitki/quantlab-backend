/**
 * Release Startup Self-Test
 */

import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { createObserverApp } from '../observer/index.js';
import { mkdir, open, unlink } from 'node:fs/promises';
import { join } from 'node:path';

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function pad2(n) {
  return n.toString().padStart(2, '0');
}

function dateKeyFromMs(ms) {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}${pad2(d.getUTCDate())}`;
}

async function streamToString(body) {
  const chunks = [];
  for await (const chunk of body) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString('utf-8');
}

async function checkS3Reachable() {
  const enabled = envBool(process.env.RUN_ARCHIVE_ENABLED || '0');
  if (!enabled) {
    return { skipped: true };
  }

  const bucket = process.env.RUN_ARCHIVE_S3_BUCKET;
  const endpoint = process.env.RUN_ARCHIVE_S3_ENDPOINT;
  const accessKey = process.env.RUN_ARCHIVE_S3_ACCESS_KEY;
  const secretKey = process.env.RUN_ARCHIVE_S3_SECRET_KEY;

  const s3 = new S3Client({
    endpoint,
    region: 'auto',
    credentials: {
      accessKeyId: accessKey,
      secretAccessKey: secretKey
    },
    forcePathStyle: true
  });

  await s3.send(new ListObjectsV2Command({
    Bucket: bucket,
    Prefix: 'replay_runs/',
    MaxKeys: 1
  }));

  return { skipped: false, s3 };
}

async function checkIndexReadable(s3) {
  if (!s3) return { skipped: true };

  try {
    const res = await s3.send(new GetObjectCommand({
      Bucket: process.env.RUN_ARCHIVE_S3_BUCKET,
      Key: 'replay_runs/_index.json'
    }));
    const text = await streamToString(res.Body);
    JSON.parse(text);
    return { skipped: false, present: true };
  } catch (err) {
    const code = err?.$metadata?.httpStatusCode;
    if (code === 404 || err?.name === 'NoSuchKey') {
      return { skipped: false, present: false };
    }
    throw err;
  }
}

async function checkAuditSpoolWritable() {
  const spoolDir = process.env.AUDIT_SPOOL_DIR || '/tmp/quantlab-audit';
  const dateKey = dateKeyFromMs(Date.now());
  const dir = join(spoolDir, `date=${dateKey}`);
  await mkdir(dir, { recursive: true });
  const fileName = `selftest-${Date.now()}.tmp`;
  const filePath = join(dir, fileName);

  const fh = await open(filePath, 'w');
  try {
    await fh.writeFile('selftest\n');
    await fh.sync();
  } finally {
    await fh.close();
  }

  await unlink(filePath);
}

async function checkObserverAuth() {
  const token = process.env.OBSERVER_TOKEN || '';
  if (!token) {
    throw new Error('OBSERVER_TOKEN missing');
  }

  const app = createObserverApp();
  const server = await new Promise((resolve) => {
    const srv = app.listen(0, () => resolve(srv));
  });

  const address = server.address();
  const port = typeof address === 'string' ? address : address.port;
  const base = `http://127.0.0.1:${port}/observer/health`;

  try {
    const resUnauthorized = await fetch(base, { method: 'GET' });
    if (resUnauthorized.status !== 401) {
      throw new Error(`observer auth expected 401, got ${resUnauthorized.status}`);
    }

    const resAuthorized = await fetch(base, {
      method: 'GET',
      headers: { Authorization: `Bearer ${token}` }
    });
    if (resAuthorized.status !== 200) {
      throw new Error(`observer auth expected 200, got ${resAuthorized.status}`);
    }
  } finally {
    await new Promise((resolve) => server.close(() => resolve()));
  }
}

export async function runSelfTest() {
  const start = Date.now();
  try {
    const s3Result = await checkS3Reachable();
    console.log(JSON.stringify({
      event: 'self_test',
      step: 's3_reachable',
      status: 'ok',
      skipped: s3Result.skipped
    }));

    const indexResult = await checkIndexReadable(s3Result.s3);
    console.log(JSON.stringify({
      event: 'self_test',
      step: 'index_readable',
      status: 'ok',
      skipped: indexResult.skipped,
      present: indexResult.present
    }));

    await checkAuditSpoolWritable();
    console.log(JSON.stringify({
      event: 'self_test',
      step: 'audit_spool',
      status: 'ok'
    }));

    await checkObserverAuth();
    console.log(JSON.stringify({
      event: 'self_test',
      step: 'observer_auth',
      status: 'ok'
    }));

    console.log(JSON.stringify({
      event: 'self_test',
      status: 'ok',
      duration_ms: Date.now() - start
    }));
  } catch (err) {
    console.error(JSON.stringify({
      event: 'self_test',
      status: 'error',
      error: err.message || String(err),
      duration_ms: Date.now() - start
    }));
    process.exit(1);
  }
}

if (process.argv[1] && process.argv[1].endsWith('SelfTest.js')) {
  runSelfTest();
}
