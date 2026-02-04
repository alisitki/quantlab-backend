/**
 * CostWriter: Write cost.json artifact for ML jobs.
 *
 * Creates a cost record for each job containing:
 * - Job metadata (id, symbol, date)
 * - Instance info (id, GPU type)
 * - Cost calculation (hourly rate, runtime, total cost)
 */
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import fs from 'fs/promises';
import path from 'path';
import { SCHEDULER_CONFIG } from '../scheduler/config.js';
import { CostCalculator } from './CostCalculator.js';

export class CostWriter {
  #s3Client;
  #s3Bucket;
  #artifactPrefix;
  #calculator;
  #localLogPath;

  constructor(options = {}) {
    this.#s3Client = new S3Client({
      endpoint: SCHEDULER_CONFIG.s3.artifactEndpoint,
      region: process.env.S3_ARTIFACTS_REGION || process.env.S3_COMPACT_REGION || 'us-east-1',
      credentials: {
        accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
        secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
      },
      forcePathStyle: true
    });
    this.#s3Bucket = SCHEDULER_CONFIG.s3.artifactBucket;
    this.#artifactPrefix = SCHEDULER_CONFIG.s3.artifactPrefix || 'ml-jobs';
    this.#calculator = new CostCalculator();
    this.#localLogPath = options.logPath || path.resolve(process.cwd(), 'logs/gpu-costs.jsonl');
  }

  /**
   * Write cost record for a completed job.
   * @param {Object} params - Cost parameters
   * @param {string} params.jobId - Job identifier
   * @param {string} params.symbol - Trading symbol (e.g., 'btcusdt')
   * @param {string} params.date - Job date (YYYYMMDD format)
   * @param {string} params.instanceId - Vast.ai instance ID
   * @param {string} params.gpuType - GPU type (e.g., 'RTX_3090')
   * @param {number} params.hourlyCost - Hourly cost in USD
   * @param {number} params.runtimeMs - Job runtime in milliseconds
   * @param {number} [params.retryCount=0] - Number of retries
   * @param {Object} [params.metadata] - Additional metadata
   * @returns {Promise<Object>} Written cost record
   */
  async writeCost(params) {
    const {
      jobId,
      symbol,
      date,
      instanceId,
      gpuType,
      hourlyCost,
      runtimeMs,
      retryCount = 0,
      metadata = {}
    } = params;

    // Calculate cost
    const calculatedCost = this.#calculator.calculateJobCost(runtimeMs, hourlyCost);

    const costRecord = {
      jobId,
      symbol: symbol?.toLowerCase(),
      date,
      instanceId,
      gpuType,
      hourlyCost,
      runtimeMs,
      calculatedCost,
      retryCount,
      recordedAt: new Date().toISOString(),
      ...metadata
    };

    console.log(`[CostWriter] Job ${jobId}: ${runtimeMs}ms @ $${hourlyCost}/hr = $${calculatedCost}`);

    // Write to S3
    await this.#writeToS3(jobId, costRecord);

    // Write to local log (append)
    await this.#appendToLocalLog(costRecord);

    return costRecord;
  }

  /**
   * Write cost record to S3.
   */
  async #writeToS3(jobId, costRecord) {
    const key = `${this.#artifactPrefix}/${jobId}/cost.json`;

    try {
      await this.#s3Client.send(new PutObjectCommand({
        Bucket: this.#s3Bucket,
        Key: key,
        Body: JSON.stringify(costRecord, null, 2),
        ContentType: 'application/json'
      }));
      console.log(`[CostWriter] Uploaded cost.json to s3://${this.#s3Bucket}/${key}`);
    } catch (err) {
      console.error(`[CostWriter] Failed to upload cost.json: ${err.message}`);
      throw err;
    }
  }

  /**
   * Append cost record to local JSONL log.
   */
  async #appendToLocalLog(costRecord) {
    try {
      const logDir = path.dirname(this.#localLogPath);
      await fs.mkdir(logDir, { recursive: true });
      await fs.appendFile(
        this.#localLogPath,
        JSON.stringify(costRecord) + '\n'
      );
    } catch (err) {
      console.warn(`[CostWriter] Failed to append to local log: ${err.message}`);
      // Non-fatal - S3 is the primary store
    }
  }

  /**
   * Read local cost log.
   * @param {Object} options - Filter options
   * @param {number} [options.limit=100] - Max records to return
   * @param {string} [options.symbol] - Filter by symbol
   * @returns {Promise<Array>} Cost records
   */
  async readLocalLog(options = {}) {
    const { limit = 100, symbol } = options;

    try {
      const content = await fs.readFile(this.#localLogPath, 'utf8');
      const lines = content.trim().split('\n').filter(Boolean);

      let records = lines.map(line => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      }).filter(Boolean);

      // Filter by symbol
      if (symbol) {
        records = records.filter(r => r.symbol === symbol.toLowerCase());
      }

      // Return most recent first
      return records.reverse().slice(0, limit);
    } catch (err) {
      if (err.code === 'ENOENT') {
        return [];
      }
      throw err;
    }
  }

  /**
   * Get quick summary from local log.
   * @param {string} period - '24h', '7d', '30d'
   * @returns {Promise<Object>} Summary stats
   */
  async getLocalSummary(period = '7d') {
    const records = await this.readLocalLog({ limit: 1000 });

    const now = new Date();
    let cutoffMs;

    switch (period) {
      case '24h':
        cutoffMs = 24 * 60 * 60 * 1000;
        break;
      case '7d':
        cutoffMs = 7 * 24 * 60 * 60 * 1000;
        break;
      case '30d':
        cutoffMs = 30 * 24 * 60 * 60 * 1000;
        break;
      default:
        cutoffMs = 7 * 24 * 60 * 60 * 1000;
    }

    const cutoff = now.getTime() - cutoffMs;
    const filtered = records.filter(r => {
      const recordTime = new Date(r.recordedAt).getTime();
      return recordTime >= cutoff;
    });

    let totalCost = 0;
    let totalRuntimeMs = 0;
    const bySymbol = {};

    for (const r of filtered) {
      totalCost += r.calculatedCost || 0;
      totalRuntimeMs += r.runtimeMs || 0;

      const sym = r.symbol || 'unknown';
      bySymbol[sym] = (bySymbol[sym] || 0) + (r.calculatedCost || 0);
    }

    return {
      period,
      totalJobs: filtered.length,
      totalCost: Number(totalCost.toFixed(4)),
      avgCostPerJob: filtered.length > 0 ? Number((totalCost / filtered.length).toFixed(4)) : 0,
      totalRuntimeMs,
      bySymbol: Object.fromEntries(
        Object.entries(bySymbol).map(([k, v]) => [k, Number(v.toFixed(4))])
      )
    };
  }
}

/**
 * Create singleton instance.
 */
let instance = null;
export function getCostWriter() {
  if (!instance) {
    instance = new CostWriter();
  }
  return instance;
}
