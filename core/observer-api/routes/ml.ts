/**
 * ML Dashboard API Routes
 * Provides ML job metrics, aggregations, and promotion history
 */
import { Router, Request, Response } from 'express';
import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const router = Router();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ARTIFACTS_PATH = path.resolve(__dirname, '../../ml/artifacts/jobs');

interface JobMetrics {
  accuracy?: number;
  directionalHitRate?: number;
  sampleSize?: number;
  directionalSampleSize?: number;
  f1_pos?: number;
  balancedAccuracy?: number;
  maxDrawdown?: number;
  best_threshold?: { value: number; metric: string };
}

interface JobInfo {
  jobId: string;
  dataset?: {
    symbol?: string;
    exchange?: string;
    stream?: string;
    dateRange?: { date?: string; windowSize?: number };
  };
  model?: {
    type?: string;
    params?: Record<string, any>;
    featureParams?: { enabledFeatures?: string[] };
  };
  runtime?: {
    backend?: string;
    gpuType?: string;
    maxRuntimeMin?: number;
  };
  hash?: string;
}

interface EnrichedJob {
  job_id: string;
  symbol: string | null;
  model_type: string | null;
  status: 'completed' | 'pending' | 'failed';
  created_at: string | null;
  metrics: JobMetrics | null;
  hash: string | null;
}

async function readJsonFile<T>(filePath: string): Promise<T | null> {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function getJobDirectories(): Promise<string[]> {
  try {
    const entries = await readdir(ARTIFACTS_PATH, { withFileTypes: true });
    return entries
      .filter(e => e.isDirectory())
      .map(e => e.name);
  } catch {
    return [];
  }
}

async function loadJobWithMetrics(jobDir: string): Promise<EnrichedJob | null> {
  const jobPath = path.join(ARTIFACTS_PATH, jobDir);

  const jobInfo = await readJsonFile<JobInfo>(path.join(jobPath, 'job.json'));
  const metrics = await readJsonFile<JobMetrics>(path.join(jobPath, 'metrics.json'));

  if (!jobInfo) return null;

  let createdAt: string | null = null;
  try {
    const stats = await stat(path.join(jobPath, 'job.json'));
    createdAt = stats.mtime.toISOString();
  } catch { /* ignore */ }

  return {
    job_id: jobInfo.jobId || jobDir,
    symbol: jobInfo.dataset?.symbol || null,
    model_type: jobInfo.model?.type || null,
    status: metrics ? 'completed' : 'pending',
    created_at: createdAt,
    metrics: metrics || null,
    hash: jobInfo.hash || null
  };
}

/**
 * GET /v1/ml/jobs
 * Returns list of ML jobs with enriched metrics
 */
router.get('/ml/jobs', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(Number(req.query.limit || 100), 500);
    const status = req.query.status as string | undefined;
    const symbol = req.query.symbol as string | undefined;

    const jobDirs = await getJobDirectories();
    const jobs: EnrichedJob[] = [];

    for (const dir of jobDirs) {
      const job = await loadJobWithMetrics(dir);
      if (!job) continue;

      // Filter by status
      if (status && job.status !== status) continue;

      // Filter by symbol
      if (symbol && job.symbol?.toLowerCase() !== symbol.toLowerCase()) continue;

      jobs.push(job);
    }

    // Sort by created_at descending (newest first)
    jobs.sort((a, b) => {
      if (!a.created_at && !b.created_at) return 0;
      if (!a.created_at) return 1;
      if (!b.created_at) return -1;
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
    });

    const sliced = jobs.slice(0, limit);
    (res.locals as any).resultCount = sliced.length;

    return res.json({
      count: sliced.length,
      total: jobs.length,
      jobs: sliced
    });
  } catch (err: any) {
    console.error(`[ML_ROUTES] code=LIST_JOBS_ERROR error=${err.message}`);
    return res.status(500).json({ error: 'LIST_JOBS_ERROR', message: err.message });
  }
});

/**
 * GET /v1/ml/jobs/:jobId
 * Returns detailed job info with full metrics and timeline
 */
router.get('/ml/jobs/:jobId', async (req: Request, res: Response) => {
  try {
    const { jobId } = req.params;
    const jobPath = path.join(ARTIFACTS_PATH, jobId);

    const jobInfo = await readJsonFile<JobInfo>(path.join(jobPath, 'job.json'));
    if (!jobInfo) {
      return res.status(404).json({ error: 'JOB_NOT_FOUND', message: `Job ${jobId} not found` });
    }

    const metrics = await readJsonFile<JobMetrics>(path.join(jobPath, 'metrics.json'));
    const runtime = await readJsonFile<any>(path.join(jobPath, 'runtime.json'));

    let createdAt: string | null = null;
    try {
      const stats = await stat(path.join(jobPath, 'job.json'));
      createdAt = stats.mtime.toISOString();
    } catch { /* ignore */ }

    (res.locals as any).resultCount = 1;

    return res.json({
      job_id: jobId,
      created_at: createdAt,
      status: metrics ? 'completed' : 'pending',
      job_spec: jobInfo,
      metrics: metrics || null,
      runtime: runtime || null
    });
  } catch (err: any) {
    console.error(`[ML_ROUTES] code=GET_JOB_ERROR error=${err.message}`);
    return res.status(500).json({ error: 'GET_JOB_ERROR', message: err.message });
  }
});

/**
 * GET /v1/ml/metrics
 * Returns aggregated ML metrics summary
 */
router.get('/ml/metrics', async (req: Request, res: Response) => {
  try {
    const jobDirs = await getJobDirectories();

    let totalJobs = 0;
    let completedJobs = 0;
    let failedJobs = 0;
    let totalAccuracy = 0;
    let totalDirectional = 0;
    let accuracyCount = 0;
    let directionalCount = 0;

    const symbolStats: Record<string, { count: number; avgAccuracy: number; avgDirectional: number }> = {};

    for (const dir of jobDirs) {
      totalJobs++;
      const job = await loadJobWithMetrics(dir);
      if (!job) continue;

      if (job.status === 'completed') {
        completedJobs++;

        if (job.metrics?.accuracy != null) {
          totalAccuracy += job.metrics.accuracy;
          accuracyCount++;
        }

        if (job.metrics?.directionalHitRate != null) {
          totalDirectional += job.metrics.directionalHitRate;
          directionalCount++;
        }

        // Per-symbol stats
        const sym = job.symbol || 'unknown';
        if (!symbolStats[sym]) {
          symbolStats[sym] = { count: 0, avgAccuracy: 0, avgDirectional: 0 };
        }
        symbolStats[sym].count++;
        if (job.metrics?.accuracy != null) {
          symbolStats[sym].avgAccuracy += job.metrics.accuracy;
        }
        if (job.metrics?.directionalHitRate != null) {
          symbolStats[sym].avgDirectional += job.metrics.directionalHitRate;
        }
      } else {
        failedJobs++;
      }
    }

    // Compute averages
    const avgAccuracy = accuracyCount > 0 ? totalAccuracy / accuracyCount : null;
    const avgDirectional = directionalCount > 0 ? totalDirectional / directionalCount : null;

    // Compute per-symbol averages
    for (const sym of Object.keys(symbolStats)) {
      const s = symbolStats[sym];
      if (s.count > 0) {
        s.avgAccuracy = s.avgAccuracy / s.count;
        s.avgDirectional = s.avgDirectional / s.count;
      }
    }

    (res.locals as any).resultCount = 1;

    return res.json({
      summary: {
        total_jobs: totalJobs,
        completed_jobs: completedJobs,
        failed_jobs: failedJobs,
        avg_accuracy: avgAccuracy,
        avg_directional_hit_rate: avgDirectional
      },
      by_symbol: symbolStats,
      updated_at: new Date().toISOString()
    });
  } catch (err: any) {
    console.error(`[ML_ROUTES] code=METRICS_ERROR error=${err.message}`);
    return res.status(500).json({ error: 'METRICS_ERROR', message: err.message });
  }
});

/**
 * GET /v1/ml/promotions
 * Returns promotion history (placeholder - reads from artifacts)
 */
router.get('/ml/promotions', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(Number(req.query.limit || 50), 200);

    // For now, return empty array - would read from S3 promotion_log in full implementation
    // This is a placeholder for Phase 3 ML Advisory
    const promotions: any[] = [];

    (res.locals as any).resultCount = promotions.length;

    return res.json({
      count: promotions.length,
      promotions,
      note: 'Promotion history is stored in S3. This endpoint shows local cache only.'
    });
  } catch (err: any) {
    console.error(`[ML_ROUTES] code=PROMOTIONS_ERROR error=${err.message}`);
    return res.status(500).json({ error: 'PROMOTIONS_ERROR', message: err.message });
  }
});

/**
 * GET /v1/ml/compare
 * Compare two jobs side by side
 */
router.get('/ml/compare', async (req: Request, res: Response) => {
  try {
    const jobA = req.query.jobA as string;
    const jobB = req.query.jobB as string;

    if (!jobA || !jobB) {
      return res.status(400).json({ error: 'MISSING_PARAMS', message: 'jobA and jobB query params required' });
    }

    const [a, b] = await Promise.all([
      loadJobWithMetrics(jobA),
      loadJobWithMetrics(jobB)
    ]);

    if (!a) {
      return res.status(404).json({ error: 'JOB_NOT_FOUND', message: `Job ${jobA} not found` });
    }
    if (!b) {
      return res.status(404).json({ error: 'JOB_NOT_FOUND', message: `Job ${jobB} not found` });
    }

    // Compute differences
    const diff: Record<string, { a: number | null; b: number | null; delta: number | null }> = {};

    const metricsToCompare = ['accuracy', 'directionalHitRate', 'sampleSize', 'f1_pos', 'maxDrawdown'];
    for (const key of metricsToCompare) {
      const aVal = (a.metrics as any)?.[key] ?? null;
      const bVal = (b.metrics as any)?.[key] ?? null;
      diff[key] = {
        a: aVal,
        b: bVal,
        delta: (aVal != null && bVal != null) ? bVal - aVal : null
      };
    }

    (res.locals as any).resultCount = 2;

    return res.json({
      jobA: a,
      jobB: b,
      comparison: diff
    });
  } catch (err: any) {
    console.error(`[ML_ROUTES] code=COMPARE_ERROR error=${err.message}`);
    return res.status(500).json({ error: 'COMPARE_ERROR', message: err.message });
  }
});

export default router;
