/**
 * Promoter: Compares new model metrics against production and promotes if better.
 * Deterministic decision logic - no randomness.
 */
import crypto from 'crypto';
import { S3Client, GetObjectCommand, PutObjectCommand, CopyObjectCommand } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from '../scheduler/config.js';

export class Promoter {
  #s3Client;
  #bucket;
  
  constructor() {
    this.#s3Client = new S3Client({
      endpoint: SCHEDULER_CONFIG.s3.artifactEndpoint,
      region: process.env.S3_ARTIFACTS_REGION || process.env.S3_COMPACT_REGION || 'us-east-1',
      credentials: {
        accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
        secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
      },
      forcePathStyle: true
    });
    this.#bucket = SCHEDULER_CONFIG.s3.artifactBucket;
  }
  
  /**
   * Evaluate whether a new model should be promoted.
   * @param {string} symbol - Trading symbol
   * @param {Object} newMetrics - Metrics from the new model
   * @param {string} jobId - Job ID of the new model
   * @param {Object} options - Promotion options { mode: 'off'|'dry'|'auto', canary: boolean }
   * @param {Object} jobSpec - Full job specification for decision config
   * @returns {Promise<Object>} Promotion decision with reasoning
   */
  async evaluate(symbol, newMetrics, jobId, options = {}, jobSpec = null) {
    let { mode = 'off', canary = false } = options;
    
    if (mode === 'off') {
      console.log(`[Promoter] [${jobId}] Promotion disabled (off) for ${symbol}`);
      return { symbol, jobId, decision: 'off', reason: 'Promotion mode is off' };
    }
    
    // Canary Guard: Downgrade auto to dry
    const originalMode = mode;
    if (canary && mode === 'auto') {
      console.log(`[PROMOTE] canary run: auto->dry (blocked) for ${jobId}`);
      mode = 'dry';
    }
    
    console.log(`[Promoter] Evaluating model for ${symbol} (jobId: ${jobId}, mode: ${mode})...`);
    
    // Load current production metrics (if exists)
    const currentMetrics = await this.#getProductionMetrics(symbol);
    
    // Decision logic
    const decision = this.#compare(newMetrics, currentMetrics);
    
    console.log(`[Promoter] [${jobId}] Decision for ${symbol}: ${decision.promote ? 'PASS' : 'REJECT'}`);
    console.log(`[Promoter] [${jobId}] Reason: ${decision.reason}`);
    
    let promotionStatus = decision.promote ? 'passed' : 'rejected';
    
    // If promoting, check if we should actually write
    if (decision.promote) {
      if (mode === 'auto') {
        await this.#promoteModelS3(symbol, jobId, newMetrics);
        promotionStatus = 'promoted';
        
        // Write decision config (only in non-canary auto mode with best_threshold)
        if (!canary && newMetrics.best_threshold && jobSpec) {
          await this.#writeDecisionConfigS3(symbol, newMetrics, jobId, jobSpec);
        } else {
          const skipReason = canary ? 'canary mode' : 
                            !newMetrics.best_threshold ? 'no best_threshold' : 'no jobSpec';
          console.log(`[Promoter] [${jobId}] Decision config write skipped (${skipReason})`);
        }
      } else {
        console.log(`[Promoter] [${jobId}] Dry decision: would promote ${symbol} but mode is ${mode}`);
        promotionStatus = 'dry_pass';
      }
    }
    
    return {
      symbol,
      jobId,
      mode,
      decision: promotionStatus,
      reason: decision.reason,
      comparison: {
        new: newMetrics,
        current: currentMetrics
      }
    };
  }
  
  /**
   * Compare metrics and return promotion decision.
   * Rules v1:
   * - Primary: higher directionalHitRate
   * - Secondary: lower maxDrawdown (tie-breaker)
   */
  #compare(newMetrics, currentMetrics) {
    // No production model - always promote first
    if (!currentMetrics) {
      return {
        promote: true,
        reason: 'No production model exists - first model promotion'
      };
    }
    
    // Configurable primary metric
    const primaryMetricKey = process.env.PROMOTION_PRIMARY_METRIC || 'directionalHitRate';
    
    // Extract metrics with safe defaults
    // Note: balancedAccuracy, f1_pos, etc might be null if not computed
    const newPrimary = newMetrics[primaryMetricKey] ?? 0;
    const curPrimary = currentMetrics[primaryMetricKey] ?? 0;
    
    const newDrawdown = newMetrics.maxDrawdown ?? Infinity;
    const curDrawdown = currentMetrics.maxDrawdown ?? Infinity;
    
    // Primary rule: configurable metric (higher is better)
    if (newPrimary > curPrimary) {
      return {
        promote: true,
        reason: `Higher ${primaryMetricKey}: ${(newPrimary * 100).toFixed(2)}% vs ${(curPrimary * 100).toFixed(2)}%`
      };
    }
    
    if (newPrimary < curPrimary) {
      return {
        promote: false,
        reason: `Lower ${primaryMetricKey}: ${(newPrimary * 100).toFixed(2)}% vs ${(curPrimary * 100).toFixed(2)}%`
      };
    }
    
    // Secondary rule: maxDrawdown (tie-breaker)
    if (newDrawdown < curDrawdown) {
      return {
        promote: true,
        reason: `Same ${primaryMetricKey} but lower max drawdown: ${(newDrawdown * 100).toFixed(2)}% vs ${(curDrawdown * 100).toFixed(2)}%`
      };
    }
    
    // Default: don't promote if not clearly better
    return {
      promote: false,
      reason: `Not better than current: ${primaryMetricKey} ${(newPrimary * 100).toFixed(2)}%, drawdown ${(newDrawdown * 100).toFixed(2)}%`
    };
  }
  
  /**
   * Get current production model metrics from S3.
   */
  async #getProductionMetrics(symbol) {
    const key = `${SCHEDULER_CONFIG.s3.productionPrefix}/${symbol}/metrics.json`;
    
    try {
      const response = await this.#s3Client.send(new GetObjectCommand({
        Bucket: this.#bucket,
        Key: key
      }));
      
      const body = await response.Body.transformToString();
      return JSON.parse(body);
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
        console.log(`[Promoter] No production model found for ${symbol}`);
        return null;
      }
      console.error(`[Promoter] Error fetching production metrics for ${symbol}:`, err);
      throw err;
    }
  }
  
  /**
   * Promote model artifacts to production location in S3.
   * Only called if mode is 'auto'.
   */
  async #promoteModelS3(symbol, jobId, metrics) {
    console.log(`[Promoter] [${jobId}] Writing artifacts to production for ${symbol}...`);
    
    const sourcePrefix = `${SCHEDULER_CONFIG.s3.artifactPrefix}/${jobId}`;
    const destPrefix = `${SCHEDULER_CONFIG.s3.productionPrefix}/${symbol}`;
    
    // 1. Copy model.bin
    await this.#copyS3Object(`${sourcePrefix}/model.bin`, `${destPrefix}/model.bin`);
    
    // 2. Write metrics.json with promotion metadata
    const promotedMetrics = {
      ...metrics,
      promotedFrom: jobId,
      promotedAt: new Date().toISOString()
    };
    
    await this.#s3Client.send(new PutObjectCommand({
      Bucket: this.#bucket,
      Key: `${destPrefix}/metrics.json`,
      Body: JSON.stringify(promotedMetrics, null, 2),
      ContentType: 'application/json'
    }));
    
    // 3. Write promotion log
    const logEntry = {
      jobId,
      symbol,
      promotedAt: new Date().toISOString(),
      metrics
    };
    
    await this.#s3Client.send(new PutObjectCommand({
      Bucket: this.#bucket,
      Key: `${destPrefix}/promotion_log/${jobId}.json`,
      Body: JSON.stringify(logEntry, null, 2),
      ContentType: 'application/json'
    }));
    
    console.log(`[Promoter] [${jobId}] Model promoted successfully to production`);
  }
  
  /**
   * Write decision config artifact to S3.
   * Contains threshold and proba configuration for production inference.
   */
  async #writeDecisionConfigS3(symbol, metrics, jobId, jobSpec) {
    const primaryMetric = process.env.PROMOTION_PRIMARY_METRIC || 'f1_pos';
    const thresholdGrid = [0.5, 0.55, 0.6, 0.65, 0.7];
    
    // Build decision config
    const decisionConfig = {
      symbol,
      featuresetVersion: jobSpec.dataset?.featuresetVersion || 'v1',
      labelHorizonSec: jobSpec.dataset?.labelHorizonSec || 10,
      primaryMetric,
      bestThreshold: metrics.best_threshold?.value ?? 0.5,
      thresholdGrid,
      probaSource: metrics.proba_source || 'model',
      jobId,
      createdAt: new Date().toISOString()
    };
    
    // Generate deterministic config hash
    const hashInput = {
      symbol: decisionConfig.symbol,
      featuresetVersion: decisionConfig.featuresetVersion,
      labelHorizonSec: decisionConfig.labelHorizonSec,
      primaryMetric: decisionConfig.primaryMetric,
      bestThreshold: decisionConfig.bestThreshold,
      thresholdGrid: decisionConfig.thresholdGrid,
      probaSource: decisionConfig.probaSource
    };
    decisionConfig.configHash = crypto
      .createHash('sha256')
      .update(JSON.stringify(hashInput))
      .digest('hex');
    
    // Write to S3: models/production/{symbol}/decision.json
    const key = `${SCHEDULER_CONFIG.s3.productionPrefix}/${symbol}/decision.json`;
    
    await this.#s3Client.send(new PutObjectCommand({
      Bucket: this.#bucket,
      Key: key,
      Body: JSON.stringify(decisionConfig, null, 2),
      ContentType: 'application/json'
    }));
    
    console.log(`[Promoter] [${jobId}] Decision config written to s3://${this.#bucket}/${key}`);
  }
  
  /**
   * Copy S3 object.
   */
  async #copyS3Object(sourceKey, destKey) {
    await this.#s3Client.send(new CopyObjectCommand({
      Bucket: this.#bucket,
      CopySource: `${this.#bucket}/${sourceKey}`,
      Key: destKey
    }));
  }
  
  /**
   * Get promotion history for a symbol.
   */
  async getPromotionHistory(symbol) {
    // Would list promotion_log/ prefix and return history
    // Simplified for v1
    return [];
  }
}
