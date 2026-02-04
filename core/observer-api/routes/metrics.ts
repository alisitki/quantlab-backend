/**
 * Observer-API Metrics Routes
 * Prometheus format metrics for observer and collector
 *
 * GET /metrics         - Observer metrics (jobs, decisions, promotions)
 * GET /metrics/collector - Collector metrics (proxied from external VPS)
 */
import { Router, Request, Response } from 'express';
import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const router = Router();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ARTIFACTS_PATH = path.resolve(__dirname, '../../ml/artifacts/jobs');

// Collector API URL from env (external VPS)
const COLLECTOR_API_URL = process.env.COLLECTOR_API_URL || '';

interface JobMetrics {
  accuracy?: number;
  directionalHitRate?: number;
}

/**
 * Count jobs by status from artifacts directory
 */
async function countJobsByStatus(): Promise<{
  total: number;
  completed: number;
  pending: number;
  failed: number;
  lastCompletedAt: string | null;
}> {
  let total = 0;
  let completed = 0;
  let pending = 0;
  let failed = 0;
  let lastCompletedAt: string | null = null;

  try {
    const entries = await readdir(ARTIFACTS_PATH, { withFileTypes: true });
    const jobDirs = entries.filter(e => e.isDirectory()).map(e => e.name);

    for (const dir of jobDirs) {
      total++;
      const jobPath = path.join(ARTIFACTS_PATH, dir);

      try {
        const metricsPath = path.join(jobPath, 'metrics.json');
        const metricsRaw = await readFile(metricsPath, 'utf8');
        const metrics: JobMetrics = JSON.parse(metricsRaw);

        if (metrics) {
          completed++;
          // Get file modification time as completion time
          const stats = await stat(metricsPath);
          const completedAt = stats.mtime.toISOString();
          if (!lastCompletedAt || completedAt > lastCompletedAt) {
            lastCompletedAt = completedAt;
          }
        }
      } catch {
        // No metrics.json means pending or failed
        try {
          // Check if job.json exists
          await readFile(path.join(jobPath, 'job.json'), 'utf8');
          pending++;
        } catch {
          failed++;
        }
      }
    }
  } catch {
    // Artifacts path not accessible
  }

  return { total, completed, pending, failed, lastCompletedAt };
}

/**
 * Count decisions from decisions directory
 */
async function countDecisions(): Promise<number> {
  try {
    const decisionsPath = path.resolve(__dirname, '../../ml/artifacts/decisions');
    const entries = await readdir(decisionsPath);
    return entries.length;
  } catch {
    return 0;
  }
}

/**
 * Render observer metrics in Prometheus format
 */
async function renderObserverMetrics(): Promise<string> {
  const jobStats = await countJobsByStatus();
  const decisionsCount = await countDecisions();

  const lines: string[] = [];

  // Jobs Total
  lines.push('# HELP observer_jobs_total Total ML jobs by status');
  lines.push('# TYPE observer_jobs_total gauge');
  lines.push(`observer_jobs_total{status="completed"} ${jobStats.completed}`);
  lines.push(`observer_jobs_total{status="pending"} ${jobStats.pending}`);
  lines.push(`observer_jobs_total{status="failed"} ${jobStats.failed}`);
  lines.push('');

  // Jobs Total (aggregate)
  lines.push('# HELP observer_jobs_count Total number of ML jobs');
  lines.push('# TYPE observer_jobs_count gauge');
  lines.push(`observer_jobs_count ${jobStats.total}`);
  lines.push('');

  // Last Completed
  if (jobStats.lastCompletedAt) {
    const ts = new Date(jobStats.lastCompletedAt).getTime() / 1000;
    lines.push('# HELP observer_jobs_last_completed_timestamp Unix timestamp of last completed job');
    lines.push('# TYPE observer_jobs_last_completed_timestamp gauge');
    lines.push(`observer_jobs_last_completed_timestamp ${ts}`);
    lines.push('');
  }

  // Decisions
  lines.push('# HELP observer_decisions_total Total ML decision artifacts');
  lines.push('# TYPE observer_decisions_total gauge');
  lines.push(`observer_decisions_total ${decisionsCount}`);
  lines.push('');

  // Service info
  lines.push('# HELP observer_info Observer API info');
  lines.push('# TYPE observer_info gauge');
  lines.push(`observer_info{version="1.0.0"} 1`);

  return lines.join('\n');
}

/**
 * Convert collector JSON metrics to Prometheus format
 */
function collectorJsonToPrometheus(json: any): string {
  const lines: string[] = [];

  // Queue size
  if (json.queue_size != null) {
    lines.push('# HELP collector_queue_size Current write queue size');
    lines.push('# TYPE collector_queue_size gauge');
    lines.push(`collector_queue_size ${json.queue_size}`);
    lines.push('');
  }

  // Writer stats
  if (json.writer) {
    lines.push('# HELP collector_writer_bytes_total Total bytes written');
    lines.push('# TYPE collector_writer_bytes_total counter');
    lines.push(`collector_writer_bytes_total ${json.writer.bytes_total || 0}`);
    lines.push('');

    lines.push('# HELP collector_writer_files_total Total files written');
    lines.push('# TYPE collector_writer_files_total counter');
    lines.push(`collector_writer_files_total ${json.writer.files_total || 0}`);
    lines.push('');

    lines.push('# HELP collector_writer_errors_total Total write errors');
    lines.push('# TYPE collector_writer_errors_total counter');
    lines.push(`collector_writer_errors_total ${json.writer.errors_total || 0}`);
    lines.push('');
  }

  // Backpressure
  if (json.backpressure_state) {
    lines.push('# HELP collector_backpressure_state Current backpressure state');
    lines.push('# TYPE collector_backpressure_state gauge');
    lines.push(`collector_backpressure_state{level="normal"} ${json.backpressure_state === 'normal' ? 1 : 0}`);
    lines.push(`collector_backpressure_state{level="high"} ${json.backpressure_state === 'high' ? 1 : 0}`);
    lines.push(`collector_backpressure_state{level="critical"} ${json.backpressure_state === 'critical' ? 1 : 0}`);
    lines.push('');
  }

  // Uptime
  if (json.uptime_seconds != null) {
    lines.push('# HELP collector_uptime_seconds Collector uptime in seconds');
    lines.push('# TYPE collector_uptime_seconds gauge');
    lines.push(`collector_uptime_seconds ${json.uptime_seconds}`);
    lines.push('');
  }

  // Connected exchanges
  if (json.exchanges) {
    lines.push('# HELP collector_exchanges_connected Number of connected exchanges');
    lines.push('# TYPE collector_exchanges_connected gauge');
    const connected = Object.values(json.exchanges).filter((e: any) => e.connected).length;
    lines.push(`collector_exchanges_connected ${connected}`);
    lines.push('');

    // Per-exchange
    lines.push('# HELP collector_exchange_messages_total Messages received per exchange');
    lines.push('# TYPE collector_exchange_messages_total counter');
    for (const [name, data] of Object.entries(json.exchanges)) {
      const exch = data as any;
      lines.push(`collector_exchange_messages_total{exchange="${name}"} ${exch.messages_total || 0}`);
    }
  }

  return lines.join('\n');
}

/**
 * GET /metrics - Observer metrics in Prometheus format
 */
router.get('/metrics', async (req: Request, res: Response) => {
  try {
    const metrics = await renderObserverMetrics();
    res.type('text/plain; version=0.0.4; charset=utf-8');
    return res.send(metrics);
  } catch (err: any) {
    console.error(`[METRICS] code=RENDER_ERROR error=${err.message}`);
    return res.status(500).send(`# ERROR: ${err.message}`);
  }
});

/**
 * GET /metrics/collector - Proxy collector metrics from external VPS
 */
router.get('/metrics/collector', async (req: Request, res: Response) => {
  if (!COLLECTOR_API_URL) {
    res.type('text/plain; version=0.0.4; charset=utf-8');
    return res.send('# collector_configured 0\n# COLLECTOR_API_URL not set');
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);

    const response = await fetch(`${COLLECTOR_API_URL}/metrics`, {
      signal: controller.signal
    });

    clearTimeout(timeout);

    if (!response.ok) {
      res.type('text/plain; version=0.0.4; charset=utf-8');
      return res.send(`# collector_reachable 0\n# HTTP ${response.status}`);
    }

    const json = await response.json();
    const prometheusMetrics = collectorJsonToPrometheus(json);

    res.type('text/plain; version=0.0.4; charset=utf-8');
    return res.send(`# collector_reachable 1\n${prometheusMetrics}`);
  } catch (err: any) {
    console.error(`[METRICS] code=COLLECTOR_PROXY_ERROR error=${err.message}`);
    res.type('text/plain; version=0.0.4; charset=utf-8');
    return res.send(`# collector_reachable 0\n# ERROR: ${err.message}`);
  }
});

export default router;
