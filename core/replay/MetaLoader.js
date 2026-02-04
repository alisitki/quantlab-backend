import { readFile } from 'node:fs/promises';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import dotenv from 'dotenv';
import { metaCache, pageCache } from './ReplayCache.js';
import crypto from 'node:crypto';
import { replayMetrics } from '../../services/replayd/metrics.js';

dotenv.config();

/** Required fields in meta.json */
const REQUIRED_FIELDS = ['schema_version', 'rows', 'ts_event_min', 'ts_event_max'];

/**
 * Generate a stable cache key for a path
 */
function getPathHash(path) {
  return crypto.createHash('md5').update(path).digest('hex');
}

/**
 * Load and parse meta.json from disk or S3
 * @param {string} metaPath - Absolute path or s3:// URI to meta.json
 * @param {Object} [identity] - Optional identity: { stream, date, symbol }
 * @returns {Promise<import('./types.js').MetaData>}
 */
export async function loadMeta(metaPath, identity = {}) {
  const { stream = 'unknown', date = 'unknown', symbol = 'unknown' } = identity;
  
  // Preliminary lookup with path only (to avoid S3/FS hit if possible)
  const pathHash = getPathHash(metaPath);
  const quickCached = metaCache.get(`meta:path:${pathHash}`);

  let content;
  // ... reading logic ...
  if (metaPath.startsWith('s3://')) {
    const endpoint = process.env.S3_COMPACT_ENDPOINT;
    const accessKey = process.env.S3_COMPACT_ACCESS_KEY;
    const secretKey = process.env.S3_COMPACT_SECRET_KEY;

    if (!endpoint || !accessKey || !secretKey) {
      throw new Error(
        `CREDENTIAL_ERROR: Missing required S3_COMPACT_* variables for S3 access. ` +
        `Required: [S3_COMPACT_ENDPOINT, S3_COMPACT_ACCESS_KEY, S3_COMPACT_SECRET_KEY]`
      );
    }

    const s3 = new S3Client({
      endpoint,
      region: process.env.S3_COMPACT_REGION || 'us-east-1',
      credentials: {
        accessKeyId: accessKey,
        secretAccessKey: secretKey
      },
      forcePathStyle: true
    });

    const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
    const key = metaPath.replace('s3://' + bucket + '/', '').replace('s3://', '');
    
    try {
      const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
      const res = await s3.send(cmd);
      replayMetrics.s3GetOpsTotal++;
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
  const manifest_id = crypto.createHash('md5').update(content).digest('hex').substring(0, 12);
  const result = {
    schema_version: Number(meta.schema_version),
    rows: Number(meta.rows),
    ts_event_min: Number(meta.ts_event_min),
    ts_event_max: Number(meta.ts_event_max),
    source_files: meta.source_files !== undefined ? Number(meta.source_files) : undefined,
    stream_type: meta.stream_type,
    ordering_columns: meta.ordering_columns,
    manifest_id // Add internal fingerprint
  };

  if (quickCached && quickCached.manifest_id && quickCached.manifest_id !== result.manifest_id) {
    metaCache.invalidateAll();
    pageCache.invalidateAll();
  }

  // Full production key: meta:{stream}:{date}:{symbol}:{schema_version}:{manifest_id}
  const fullKey = `meta:${stream}:${date}:${symbol}:${result.schema_version}:${manifest_id}`;
  metaCache.set(fullKey, result);
  metaCache.set(`meta:path:${pathHash}`, result); // Keep quick lookup
  return result;
}

/**
 * Validate consistency across multiple meta files
 * @param {import('./types.js').MetaData[]} metas
 * @throws {Error} If inconsistent
 */
function validateMultiMetaConsistency(metas) {
  if (metas.length === 0) return;

  const first = metas[0];
  const fieldsToMatch = ['schema_version', 'stream_type'];
  
  for (let i = 1; i < metas.length; i++) {
    const current = metas[i];
    
    for (const field of fieldsToMatch) {
      if (first[field] !== undefined && current[field] !== undefined && first[field] !== current[field]) {
        throw new Error(`MULTI_META_INCONSISTENT: Field '${field}' mismatch. Host: ${metas[0].schema_version} vs ${current.schema_version}`);
      }
    }

    // Check ordering columns if present in meta
    if (first.ordering_columns && current.ordering_columns) {
      const fCols = JSON.stringify(first.ordering_columns);
      const cCols = JSON.stringify(current.ordering_columns);
      if (fCols !== cCols) {
        throw new Error(`MULTI_META_INCONSISTENT: ordering_columns mismatch: ${fCols} vs ${cCols}`);
      }
    }
  }
}

/**
 * Load multiple meta files and return a unified metadata object
 * @param {string[]} metaPaths
 * @returns {Promise<import('./types.js').MetaData>}
 */
export async function loadMultiMeta(metaPaths, identity = {}) {
  if (!Array.isArray(metaPaths) || metaPaths.length === 0) {
    throw new Error('MULTI_META_INVALID: metaPaths must be a non-empty array');
  }

  const multiHash = getPathHash(metaPaths.sort().join('|'));
  const cached = metaCache.get(`multiMeta:${multiHash}`);
  if (cached) {
    return cached;
  }

  const metas = await Promise.all(metaPaths.map(path => loadMeta(path, identity)));
  
  // Validate consistency
  validateMultiMetaConsistency(metas);

  // Return merged meta
  const result = {
    schema_version: metas[0].schema_version,
    stream_type: metas[0].stream_type,
    ordering_columns: metas[0].ordering_columns,
    rows: metas.reduce((sum, m) => sum + m.rows, 0),
    ts_event_min: Math.min(...metas.map(m => m.ts_event_min)),
    ts_event_max: Math.max(...metas.map(m => m.ts_event_max)),
    partition_count: metas.length
  };

  metaCache.set(`multiMeta:${multiHash}`, result);
  return result;
}
