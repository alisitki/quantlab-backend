/**
 * CostCalculator: GPU cost calculation and aggregation for Vast.ai jobs.
 *
 * Calculates: cost = (runtimeMs / 3600000) * hourlyRate
 */
import fs from 'fs/promises';
import path from 'path';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { SCHEDULER_CONFIG } from '../scheduler/config.js';

export class CostCalculator {
  #s3Client;
  #s3Bucket;
  #artifactPrefix;

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
    this.#s3Bucket = SCHEDULER_CONFIG.s3.artifactBucket;
    this.#artifactPrefix = SCHEDULER_CONFIG.s3.artifactPrefix || 'ml-jobs';
  }

  /**
   * Calculate job cost from runtime and hourly rate.
   * @param {number} runtimeMs - Job runtime in milliseconds
   * @param {number} hourlyRate - Cost per hour ($/hour)
   * @returns {number} Calculated cost in USD
   */
  calculateJobCost(runtimeMs, hourlyRate) {
    if (!runtimeMs || runtimeMs <= 0) return 0;
    if (!hourlyRate || hourlyRate <= 0) return 0;

    const hours = runtimeMs / 3600000; // ms to hours
    return Number((hours * hourlyRate).toFixed(6));
  }

  /**
   * Aggregate costs from all cost.json files in S3.
   * @param {Object} options - Aggregation options
   * @param {string} [options.startDate] - Start date (YYYY-MM-DD)
   * @param {string} [options.endDate] - End date (YYYY-MM-DD)
   * @param {string} [options.symbol] - Filter by symbol
   * @returns {Promise<Object>} Aggregated cost data
   */
  async aggregateCosts(options = {}) {
    const { startDate, endDate, symbol } = options;

    console.log('[CostCalculator] Aggregating costs from S3...');

    // List all job directories
    const listCmd = new ListObjectsV2Command({
      Bucket: this.#s3Bucket,
      Prefix: this.#artifactPrefix + '/',
      Delimiter: '/'
    });

    const listResult = await this.#s3Client.send(listCmd);
    const jobPrefixes = listResult.CommonPrefixes || [];

    const costs = [];
    let totalCost = 0;
    let totalJobs = 0;
    let totalRuntimeMs = 0;
    const bySymbol = {};
    const byGpuType = {};
    const byDay = {};

    for (const prefix of jobPrefixes) {
      try {
        // Get cost.json from each job
        const costKey = `${prefix.Prefix}cost.json`;
        const getCmd = new GetObjectCommand({
          Bucket: this.#s3Bucket,
          Key: costKey
        });

        const response = await this.#s3Client.send(getCmd);
        const body = await response.Body.transformToString();
        const costData = JSON.parse(body);

        // Apply filters
        if (startDate && costData.date < startDate.replace(/-/g, '')) continue;
        if (endDate && costData.date > endDate.replace(/-/g, '')) continue;
        if (symbol && costData.symbol !== symbol.toLowerCase()) continue;

        costs.push(costData);
        totalCost += costData.calculatedCost || 0;
        totalJobs++;
        totalRuntimeMs += costData.runtimeMs || 0;

        // By symbol
        const sym = costData.symbol || 'unknown';
        bySymbol[sym] = (bySymbol[sym] || 0) + (costData.calculatedCost || 0);

        // By GPU type
        const gpu = costData.gpuType || 'unknown';
        byGpuType[gpu] = (byGpuType[gpu] || 0) + (costData.calculatedCost || 0);

        // By day
        const day = costData.date || 'unknown';
        byDay[day] = (byDay[day] || 0) + (costData.calculatedCost || 0);

      } catch (err) {
        // cost.json may not exist for old jobs
        if (!err.name?.includes('NoSuchKey')) {
          console.warn(`[CostCalculator] Error reading cost for ${prefix.Prefix}: ${err.message}`);
        }
      }
    }

    const avgCostPerJob = totalJobs > 0 ? totalCost / totalJobs : 0;
    const avgRuntimeMs = totalJobs > 0 ? totalRuntimeMs / totalJobs : 0;

    return {
      period: {
        start: startDate || 'all',
        end: endDate || 'all',
        symbol: symbol || 'all'
      },
      summary: {
        totalCost: Number(totalCost.toFixed(4)),
        totalJobs,
        avgCostPerJob: Number(avgCostPerJob.toFixed(4)),
        totalRuntimeMs,
        avgRuntimeMs: Math.round(avgRuntimeMs)
      },
      bySymbol: this.#roundValues(bySymbol),
      byGpuType: this.#roundValues(byGpuType),
      byDay: this.#roundValues(byDay),
      costs
    };
  }

  /**
   * Calculate costs for a specific time period.
   * @param {string} period - '24h', '7d', '30d', etc.
   * @returns {Promise<Object>} Aggregated costs for period
   */
  async aggregateByPeriod(period) {
    const now = new Date();
    let startDate;

    switch (period) {
      case '24h':
        startDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        break;
      case '7d':
        startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        break;
      case '30d':
        startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
        break;
      default:
        startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    }

    return this.aggregateCosts({
      startDate: this.#formatDate(startDate),
      endDate: this.#formatDate(now)
    });
  }

  /**
   * Check if spending is within budget.
   * @param {number} spent - Amount spent
   * @param {number} budget - Budget limit
   * @returns {Object} Budget status
   */
  checkBudget(spent, budget) {
    const remaining = budget - spent;
    const percentUsed = budget > 0 ? (spent / budget) * 100 : 0;

    let status;
    if (percentUsed >= 100) {
      status = 'exceeded';
    } else if (percentUsed >= 90) {
      status = 'critical';
    } else if (percentUsed >= 75) {
      status = 'warning';
    } else {
      status = 'ok';
    }

    return {
      spent: Number(spent.toFixed(4)),
      budget: Number(budget.toFixed(4)),
      remaining: Number(remaining.toFixed(4)),
      percentUsed: Number(percentUsed.toFixed(2)),
      status
    };
  }

  /**
   * Get quick cost summary from local cache (if available).
   * @param {string} cacheFile - Path to local cache file
   * @returns {Promise<Object|null>} Cached summary or null
   */
  async getCachedSummary(cacheFile) {
    try {
      const data = await fs.readFile(cacheFile, 'utf8');
      const cache = JSON.parse(data);

      // Check if cache is fresh (less than 1 hour old)
      const cacheAge = Date.now() - new Date(cache.updatedAt).getTime();
      if (cacheAge < 60 * 60 * 1000) {
        return cache;
      }
      return null;
    } catch {
      return null;
    }
  }

  /**
   * Save cost summary to local cache.
   * @param {string} cacheFile - Path to cache file
   * @param {Object} summary - Summary to cache
   */
  async saveCacheFile(cacheFile, summary) {
    const cacheDir = path.dirname(cacheFile);
    await fs.mkdir(cacheDir, { recursive: true });
    await fs.writeFile(cacheFile, JSON.stringify({
      ...summary,
      updatedAt: new Date().toISOString()
    }, null, 2));
  }

  #formatDate(date) {
    return date.toISOString().split('T')[0];
  }

  #roundValues(obj) {
    const result = {};
    for (const [k, v] of Object.entries(obj)) {
      result[k] = Number(v.toFixed(4));
    }
    return result;
  }
}

/**
 * Create singleton instance.
 */
let instance = null;
export function getCostCalculator() {
  if (!instance) {
    instance = new CostCalculator();
  }
  return instance;
}
