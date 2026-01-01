/**
 * JobSpecGenerator: Generates deterministic JobSpec objects for ML training.
 * No randomness or wall-clock dependency in job definition.
 */
import crypto from 'crypto';
import { SCHEDULER_CONFIG } from './config.js';

export class JobSpecGenerator {
  /**
   * Generate a JobSpec for a given symbol and date.
   * @param {Object} params
   * @param {string} params.symbol - Trading symbol (e.g., 'btcusdt')
   * @param {string} params.date - Date in YYYYMMDD format
   * @param {Object} [params.modelOverrides] - Optional model config overrides
   * @returns {Object} JobSpec-compatible object
   */
  static generate({ symbol, date, modelOverrides = {} }) {
    const config = SCHEDULER_CONFIG;
    
    // Merge model config with overrides
    const modelConfig = {
      type: modelOverrides.type || config.model.type,
      params: { ...config.model.params, ...modelOverrides.params },
      featureParams: { ...config.model.featureParams, ...modelOverrides.featureParams }
    };
    
    // Build dataset spec
    const datasetSpec = {
      symbol,
      exchange: modelOverrides.exchange || config.dataset?.exchange || 'binance',
      stream: modelOverrides.stream || config.dataset?.stream || 'bbo',
      dateRange: {
        date,
        // Optional: support multi-day windows
        windowSize: config.dateRange.windowSize
      },
      featuresetVersion: modelOverrides.featureset || config.dataset?.featuresetVersion || 'v1',
      labelHorizonSec: modelOverrides.labelHorizonSec || config.dataset?.labelHorizonSec || 10
    };

    // Feature Path (Hive structure on S3)
    // s3://quantlab-compact/features/featureset=v1/exchange=binance/stream=bbo/symbol=btcusdt/date=20251229/data.parquet
    const featureset = datasetSpec.featuresetVersion;
    const featurePartition = `featureset=${featureset}/exchange=${datasetSpec.exchange}/stream=${datasetSpec.stream}/symbol=${symbol}/date=${date}`;
    const featureBucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
    
    datasetSpec.featurePath = `s3://${featureBucket}/features/${featurePartition}/data.parquet`;
    datasetSpec.metaPath = `s3://${featureBucket}/features/${featurePartition}/meta.json`;
    
    // Runtime spec for GPU execution
    const runtimeSpec = {
      backend: 'gpu',
      gpuType: config.gpu.preferredTypes[0],
      maxRuntimeMin: config.gpu.maxRuntimeMin
    };
    
    // Generate deterministic job ID from config hash
    const jobId = this.generateJobId({ symbol, date, modelConfig });
    
    // Output paths (will be on GPU instance, then uploaded to S3)
    const outputSpec = {
      artifactPath: `./ml/artifacts/jobs/${jobId}/model.bin`,
      metricsPath: `./ml/artifacts/jobs/${jobId}/metrics.json`
    };
    
    return {
      jobId,
      dataset: datasetSpec,
      model: modelConfig,
      runtime: runtimeSpec,
      output: outputSpec,
      // Store hash for verification
      configHash: this.hashConfig({ symbol, date, modelConfig })
    };
  }
  
  /**
   * Generate deterministic job ID from config.
   * Same config always produces same ID.
   */
  static generateJobId({ symbol, date, modelConfig }) {
    const hash = this.hashConfig({ symbol, date, modelConfig });
    // Use first 16 chars of hash for readability
    return `job-${symbol}-${date}-${hash.substring(0, 16)}`;
  }
  
  /**
   * Hash configuration for deterministic identity.
   */
  static hashConfig(config) {
    const normalized = JSON.stringify(config, Object.keys(config).sort());
    return crypto.createHash('sha256').update(normalized).digest('hex');
  }
  
  /**
   * Generate JobSpecs for multiple symbols for a given date.
   */
  static generateBatch({ symbols, date, modelOverrides = {} }) {
    return symbols.map(symbol => this.generate({ symbol, date, modelOverrides }));
  }
  
  /**
   * Get yesterday's date in YYYYMMDD format.
   * Note: This uses UTC to ensure determinism across timezones.
   */
  static getYesterdayDate() {
    const now = new Date();
    const yesterday = new Date(now);
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);
    return this.formatDate(yesterday);
  }
  
  /**
   * Format Date to YYYYMMDD string.
   */
  static formatDate(date) {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    return `${year}${month}${day}`;
  }
}
