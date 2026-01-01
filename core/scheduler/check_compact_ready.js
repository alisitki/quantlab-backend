#!/usr/bin/env node
/**
 * check_compact_ready.js: Check if compact state is ready for a target date.
 * 
 * Reads s3://quantlab-compact/compacted/_state.json
 * Returns: READY | NOT_READY | FAILURE
 * 
 * Exit codes:
 *   0 - READY or NOT_READY (check stdout)
 *   1 - FAILURE (state file missing/invalid)
 */
import 'dotenv/config';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

const STATE_KEY = 'compacted/_state.json';

function parseArgs(args) {
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--date') result.date = args[++i];
    if (args[i] === '--json') result.json = true;
  }
  return result;
}

function getYesterday() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().split('T')[0].replace(/-/g, '');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const targetDate = args.date || getYesterday();
  
  const s3Client = new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'us-east-1',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    },
    forcePathStyle: true
  });

  const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
  
  let state;
  try {
    const res = await s3Client.send(new GetObjectCommand({ 
      Bucket: bucket, 
      Key: STATE_KEY 
    }));
    const body = await res.Body.transformToString();
    state = JSON.parse(body);
  } catch (err) {
    if (err.name === 'NoSuchKey' || err.Code === 'NoSuchKey') {
      outputResult('FAILURE', targetDate, 'State file not found', args.json);
      process.exit(1);
    }
    outputResult('FAILURE', targetDate, `S3 error: ${err.message}`, args.json);
    process.exit(1);
  }

  // Validate state
  if (!state || typeof state !== 'object') {
    outputResult('FAILURE', targetDate, 'State file invalid JSON', args.json);
    process.exit(1);
  }

  const lastCompactedDate = state.last_compacted_date;
  if (!lastCompactedDate) {
    outputResult('FAILURE', targetDate, 'last_compacted_date missing', args.json);
    process.exit(1);
  }

  // Compare dates (string comparison works for YYYYMMDD)
  if (lastCompactedDate >= targetDate) {
    outputResult('READY', targetDate, null, args.json, lastCompactedDate);
    process.exit(0);
  } else {
    outputResult('NOT_READY', targetDate, `Compact at ${lastCompactedDate}, need ${targetDate}`, args.json, lastCompactedDate);
    process.exit(0);
  }
}

function outputResult(status, targetDate, reason, json, lastCompactedDate = null) {
  if (json) {
    console.log(JSON.stringify({
      status,
      target_date: targetDate,
      last_compacted_date: lastCompactedDate,
      reason
    }));
  } else {
    console.log(status);
    if (reason) {
      console.error(`[check_compact_ready] ${reason}`);
    }
  }
}

main().catch(err => {
  console.log('FAILURE');
  console.error(`[check_compact_ready] Fatal: ${err.message}`);
  process.exit(1);
});
