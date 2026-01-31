/**
 * Replayd Service Configuration
 * 
 * DATASET_MAP: Single source of truth for dataset → S3 path mapping.
 */

export const DATASET_MAP = Object.freeze({
  bbo: 'compact',
  trades: 'compact-trades'
});

export const SERVICE_PORT = Number(process.env.REPLAYD_PORT) || 3030;
export const REPLAY_VERSION = '1.2';

export function buildDatasetPaths(dataset, symbol, date) {
  const folder = DATASET_MAP[dataset];
  if (!folder) {
    throw new Error(`UNKNOWN_DATASET: '${dataset}' not in DATASET_MAP`);
  }
  
  // Normalize date: 2024-01-15 -> 20240115 (for Hive)
  const normalizedDate = date.replace(/-/g, '');
  
  // Local test bypass
  const localDir = process.env.LOCAL_DATA_DIR;
  if (localDir) {
    const base = `${localDir}/${folder}/${symbol}/${date}`;
    return {
      parquet: `${base}/data.parquet`,
      meta: `${base}/meta.json`
    };
  }
  
  const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
  
  // Try Hive-style first if it's S3
  const hiveBase = `s3://${bucket}/exchange=binance/stream=${dataset}/symbol=${symbol.toLowerCase()}/date=${normalizedDate}`;
  const legacyBase = `s3://${bucket}/${folder}/${symbol}/${date}`;

  // Default to Hive style for the new compaction layout
  return {
    parquet: `${hiveBase}/data.parquet`,
    meta: `${hiveBase}/meta.json`,
    legacy_meta: `${legacyBase}/meta.json` // Fallback hint
  };
}

