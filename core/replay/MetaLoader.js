/**
 * QuantLab Replay Engine v1 — Meta Loader
 * Loads and validates meta.json files from compact datasets.
 */

import { readFile } from 'node:fs/promises';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import dotenv from 'dotenv';

dotenv.config();

/** Required fields in meta.json */
const REQUIRED_FIELDS = ['schema_version', 'rows', 'ts_event_min', 'ts_event_max'];

/**
 * Load and parse meta.json from disk or S3
 * @param {string} metaPath - Absolute path or s3:// URI to meta.json
 * @returns {Promise<import('./types.js').MetaData>}
 * @throws {Error} If file missing, parse fails, or required fields missing
 */
export async function loadMeta(metaPath) {
  let content;

  if (metaPath.startsWith('s3://')) {
    const s3 = new S3Client({
      endpoint: process.env.S3_COMPACT_ENDPOINT || process.env.S3_ENDPOINT,
      region: process.env.S3_COMPACT_REGION || 'us-east-1',
      credentials: {
        accessKeyId: process.env.S3_COMPACT_ACCESS_KEY || process.env.S3_ACCESS_KEY,
        secretAccessKey: process.env.S3_COMPACT_SECRET_KEY || process.env.S3_SECRET_KEY
      },
      forcePathStyle: true
    });

    const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
    const key = metaPath.replace('s3://' + bucket + '/', '').replace('s3://', '');
    
    try {
      const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
      const res = await s3.send(cmd);
      content = await res.Body.transformToString();
    } catch (err) {
      console.error('[MetaLoader] S3 Error Details:', err);
      throw new Error(`META_S3_LOAD_FAILED: Cannot read ${metaPath}: ${err.message}`);
    }
  } else {
    try {
      content = await readFile(metaPath, 'utf-8');
    } catch (err) {
      throw new Error(`META_LOAD_FAILED: Cannot read ${metaPath}: ${err.message}`);
    }
  }

  let meta;
  try {
    meta = JSON.parse(content);
  } catch (err) {
    throw new Error(`META_PARSE_FAILED: Invalid JSON in ${metaPath}: ${err.message}`);
  }

  // Validate required fields
  const missing = REQUIRED_FIELDS.filter(field => meta[field] === undefined);
  if (missing.length > 0) {
    throw new Error(`META_INVALID: Missing required fields: ${missing.join(', ')}`);
  }

  // Type coercion for numeric fields
  return {
    schema_version: Number(meta.schema_version),
    rows: Number(meta.rows),
    ts_event_min: Number(meta.ts_event_min),
    ts_event_max: Number(meta.ts_event_max),
    source_files: meta.source_files !== undefined ? Number(meta.source_files) : undefined
  };
}
