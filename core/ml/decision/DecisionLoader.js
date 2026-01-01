/**
 * DecisionLoader: Loads decision.json from S3 with caching and fallback.
 * Used by live/signal pipelines to get threshold configuration.
 */
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from '../../scheduler/config.js';

// In-memory cache
const cache = new Map();
const CACHE_TTL_MS = 60000; // 60 seconds

// Default fallback config
const DEFAULT_DECISION = {
  symbol: null,
  featuresetVersion: 'v1',
  labelHorizonSec: 10,
  primaryMetric: 'f1_pos',
  bestThreshold: 0.5,
  thresholdGrid: [0.5, 0.55, 0.6, 0.65, 0.7],
  probaSource: 'none',
  jobId: null,
  createdAt: null,
  configHash: null,
  _fallback: true
};

/**
 * @typedef {Object} DecisionConfig
 * @property {string} symbol
 * @property {string} featuresetVersion
 * @property {number} labelHorizonSec
 * @property {string} primaryMetric
 * @property {number} bestThreshold
 * @property {number[]} thresholdGrid
 * @property {string} probaSource
 * @property {string} jobId
 * @property {string} createdAt
 * @property {string} configHash
 * @property {boolean} [_fallback]
 */

/**
 * Load decision config from S3 with caching.
 * @param {string} symbol - Trading symbol (e.g., 'btcusdt')
 * @returns {Promise<DecisionConfig>}
 */
export async function loadDecision(symbol) {
  const cacheKey = `decision:${symbol}`;
  
  // Check cache
  const cached = cache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
    console.log(`[DecisionLoader] Cache hit for ${symbol}`);
    return cached.config;
  }
  
  // Load from S3 (use same credentials as Promoter)
  const accessKeyId = process.env.S3_ARTIFACTS_ACCESS_KEY || process.env.S3_COMPACT_ACCESS_KEY;
  const secretAccessKey = process.env.S3_ARTIFACTS_SECRET_KEY || process.env.S3_COMPACT_SECRET_KEY;
  const endpoint = process.env.S3_ARTIFACTS_ENDPOINT || process.env.S3_COMPACT_ENDPOINT || SCHEDULER_CONFIG.s3.artifactEndpoint;
  const region = process.env.S3_ARTIFACTS_REGION || process.env.S3_COMPACT_REGION || 'us-east-1';

  const s3Client = new S3Client({
    endpoint,
    region,
    credentials: {
      accessKeyId,
      secretAccessKey
    },
    forcePathStyle: true
  });
  
  const bucket = process.env.S3_ARTIFACTS_BUCKET || SCHEDULER_CONFIG.s3.artifactBucket || 'quantlab-artifacts';
  const productionPrefix = process.env.S3_PRODUCTION_PREFIX || SCHEDULER_CONFIG.s3.productionPrefix || 'models/production';
  const key = `${productionPrefix}/${symbol}/decision.json`;
  
  try {
    console.log(`[DecisionLoader] Loading from s3://${bucket}/${key}`);
    
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: bucket,
      Key: key
    }));
    
    const body = await response.Body.transformToString();
    const config = JSON.parse(body);
    
    // Validate
    const validationError = validateDecisionConfig(config);
    if (validationError) {
      console.warn(`[DecisionLoader] Invalid config for ${symbol}: ${validationError}`);
      return useFallback(symbol);
    }
    
    // Update cache
    cache.set(cacheKey, { config, timestamp: Date.now() });
    
    console.log(`[DecisionLoader] Loaded decision for ${symbol}: threshold=${config.bestThreshold}, probaSource=${config.probaSource}`);
    return config;
    
  } catch (err) {
    if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
      console.warn(`[DecisionLoader] No decision.json found for ${symbol}, using fallback`);
      return useFallback(symbol);
    }
    
    console.error(`[DecisionLoader] Error loading decision for ${symbol}:`, err.message);
    return useFallback(symbol);
  }
}

/**
 * Use fallback default config.
 */
function useFallback(symbol) {
  const fallback = { ...DEFAULT_DECISION, symbol, _fallback: true };
  console.warn(`[DecisionLoader] Using fallback config: threshold=${fallback.bestThreshold}`);
  return fallback;
}

/**
 * Validate decision config structure.
 * @param {Object} config
 * @returns {string|null} Error message or null if valid
 */
export function validateDecisionConfig(config) {
  if (!config) return 'Config is null';
  if (typeof config.bestThreshold !== 'number') return 'bestThreshold must be a number';
  if (config.bestThreshold < 0 || config.bestThreshold > 1) return 'bestThreshold must be 0-1';
  if (!config.symbol) return 'symbol is required';
  if (!config.probaSource) return 'probaSource is required';
  if (!Array.isArray(config.thresholdGrid)) return 'thresholdGrid must be an array';
  return null;
}

/**
 * Clear cache (for testing).
 */
export function clearCache() {
  cache.clear();
}

/**
 * Get cache stats (for debugging).
 */
export function getCacheStats() {
  return {
    size: cache.size,
    keys: Array.from(cache.keys())
  };
}
