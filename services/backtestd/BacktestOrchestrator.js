/**
 * BacktestOrchestrator — deterministic orchestration using strategyd + replayd.
 */

import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { canonicalStringify } from '../../core/strategy/state/StateSerializer.js';

export class BacktestOrchestrator {
  constructor({ jobStore, config }) {
    this.jobStore = jobStore;
    this.config = config;
    this.runningJobs = new Set();
  }

  normalizeJob(input) {
    const streams = Array.isArray(input.streams) ? [...input.streams] : [];
    const symbols = Array.isArray(input.symbols) ? [...input.symbols] : [];
    streams.sort();
    symbols.sort();

    return {
      strategy_id: input.strategy_id,
      strategy_version: input.strategy_version || null,
      date_range: {
        start: input.date_range?.[0],
        end: input.date_range?.[1]
      },
      streams,
      symbols,
      seed: input.seed || null,
      concurrency: Number.isFinite(input.concurrency) ? Number(input.concurrency) : this.config.concurrency
    };
  }

  computeJobId(normalizedJob) {
    const payload = canonicalStringify(normalizedJob);
    return createHash('sha256').update(payload).digest('hex');
  }

  buildRunPlan(job) {
    const dates = this.#dateRange(job.date_range.start, job.date_range.end);
    const runs = [];
    for (const date of dates) {
      for (const stream of job.streams) {
        for (const symbol of job.symbols) {
          const runId = this.#buildRunId(job.job_id, stream, symbol, date);
          runs.push({
            run_id: runId,
            date,
            stream,
            symbol,
            health_class: null,
            total_events: null,
            total_signals: null,
            total_fills: null,
            ended_reason: null
          });
        }
      }
    }
    return runs;
  }

  async submit(jobInput) {
    const normalized = this.normalizeJob(jobInput);
    const jobId = this.computeJobId(normalized);

    const existing = await this.jobStore.get(jobId);
    if (existing) {
      return existing;
    }

    const job = {
      job_id: jobId,
      state: 'pending',
      strategy_id: normalized.strategy_id,
      strategy_version: normalized.strategy_version,
      date_range: normalized.date_range,
      streams: normalized.streams,
      symbols: normalized.symbols,
      seed: normalized.seed,
      concurrency: normalized.concurrency,
      runs: [],
      aggregate: {
        total_runs: 0,
        healthy_runs: 0,
        failed_runs: 0,
        degraded_runs: 0
      }
    };

    job.runs = this.buildRunPlan(job);
    job.aggregate.total_runs = job.runs.length;

    await this.jobStore.save(job);

    this.run(jobId).catch(() => {});

    return job;
  }

  async run(jobId) {
    if (this.runningJobs.has(jobId)) return;
    this.runningJobs.add(jobId);

    try {
      const job = await this.jobStore.get(jobId);
      if (!job) return;

      job.state = 'running';
      await this.jobStore.save(job);
      console.log(`[BacktestOrchestrator] job_id=${jobId} component=backtestd action=job_start runs=${job.runs.length}`);

      const runs = job.runs;
      const concurrency = Math.max(1, Number(job.concurrency) || 1);
      let index = 0;

      const workers = Array.from({ length: Math.min(concurrency, runs.length) }, (_, slot) =>
        this.#workerLoop(job, runs, () => index++, slot)
      );

      await Promise.all(workers);

      this.#recomputeAggregate(job);
      job.state = 'completed';
      await this.jobStore.save(job);

      console.log(`[BacktestOrchestrator] job_id=${jobId} component=backtestd action=job_complete total_runs=${job.aggregate.total_runs} healthy_runs=${job.aggregate.healthy_runs} failed_runs=${job.aggregate.failed_runs} degraded_runs=${job.aggregate.degraded_runs}`);
    } catch (err) {
      const msg = err?.message || 'unknown_error';
      console.error(`[BacktestOrchestrator] job_id=${jobId} component=backtestd action=job_failed error=${msg}`);
      const job = await this.jobStore.get(jobId);
      if (job) {
        job.state = 'failed';
        await this.jobStore.save(job);
      }
    } finally {
      this.runningJobs.delete(jobId);
    }
  }

  async #workerLoop(job, runs, nextIndex, slot) {
    while (true) {
      const idx = nextIndex();
      if (idx >= runs.length) return;

      const runSpec = runs[idx];
      const result = await this.#runSingle(job, runSpec, slot);

      runs[idx] = {
        ...runSpec,
        ...result
      };

      this.#recomputeAggregate(job);
      await this.jobStore.save(job);
    }
  }

  async #runSingle(job, runSpec, slot) {
    const { run_id: runId, date, stream, symbol } = runSpec;
    console.log(`[BacktestOrchestrator] job_id=${job.job_id} component=backtestd action=run_start run_id=${runId} date=${date} stream=${stream} symbol=${symbol} slot=${slot}`);

    if (!this.config.strategydToken) {
      console.error(`[BacktestOrchestrator] job_id=${job.job_id} component=backtestd action=run_error reason=missing_strategyd_token run_id=${runId}`);
      return {
        health_class: 'failed',
        total_events: null,
        total_signals: null,
        total_fills: null,
        ended_reason: 'missing_strategyd_token'
      };
    }

    const port = this.config.strategydPortBase + slot;
    const child = await this.#spawnStrategyd(job, runSpec, port);

    const manifest = await this.#waitForManifest(runId, port, child);

    await this.#stopStrategyd(child);

    const { summary, health } = await this.#readRunArtifacts(runId);
    const runResult = this.#buildRunResult(manifest, summary, health);

    console.log(`[BacktestOrchestrator] job_id=${job.job_id} component=backtestd action=run_complete run_id=${runId} ended_reason=${runResult.ended_reason} health_class=${runResult.health_class}`);

    return runResult;
  }

  async #spawnStrategyd(job, runSpec, port) {
    const logPath = `/tmp/backtestd_${runSpec.run_id}.log`;
    const logStream = fs.createWriteStream(logPath, { flags: 'w' });

    const env = {
      ...process.env,
      BACKTEST_JOB_ID: job.job_id,
      STRATEGYD_PORT: String(port),
      RUN_ID: runSpec.run_id,
      DATASET: runSpec.stream,
      SYMBOL: runSpec.symbol,
      DATE: runSpec.date,
      SPEED: 'asap',
      STRATEGY_RUNTIME_V2: '1',
      AUTH_REQUIRED: 'true',
      STRATEGYD_TOKEN: this.config.strategydToken || '',
      REPLAYD_URL: this.config.replaydUrl,
      REPLAYD_TOKEN: this.config.replaydToken || ''
    };

    const child = spawn('node', ['server.js'], {
      cwd: this.config.strategydDir,
      env,
      stdio: ['ignore', 'pipe', 'pipe']
    });

    child.stdout.on('data', (chunk) => logStream.write(chunk));
    child.stderr.on('data', (chunk) => logStream.write(chunk));

    child.on('exit', () => {
      logStream.end();
    });

    return child;
  }

  async #waitForManifest(runId, port, child) {
    const start = Date.now();
    const timeoutMs = this.config.runTimeoutMs;

    while (Date.now() - start < timeoutMs) {
      if (child.exitCode !== null && child.exitCode !== undefined) {
        return null;
      }
      const manifest = await this.#fetchManifest(runId, port);
      if (manifest) return manifest;
      await this.#sleep(this.config.runPollIntervalMs);
    }

    return null;
  }

  async #fetchManifest(runId, port) {
    const url = `http://127.0.0.1:${port}/run/${runId}`;
    const headers = {};
    if (this.config.strategydToken) {
      headers.Authorization = `Bearer ${this.config.strategydToken}`;
    }

    try {
      const res = await fetch(url, { headers });
      if (res.status === 200) {
        return res.json();
      }
    } catch {
      // ignore
    }
    return null;
  }

  async #stopStrategyd(child) {
    if (!child || child.killed) return;
    child.kill('SIGTERM');

    const exited = await Promise.race([
      new Promise((resolve) => child.on('exit', () => resolve(true))),
      this.#sleep(5000).then(() => false)
    ]);

    if (!exited) {
      child.kill('SIGKILL');
    }
  }

  async #readRunArtifacts(runId) {
    const summaryPath = path.join(this.config.runsDir, 'summary', `${runId}.json`);
    const healthPath = path.join(this.config.runsDir, 'health', `${runId}.json`);

    const summary = await this.#readJson(summaryPath);
    const health = await this.#readJson(healthPath);

    return { summary, health };
  }

  #buildRunResult(manifest, summary, health) {
    const endedReason = manifest?.ended_reason || summary?.ended_reason || 'missing_manifest';
    const totalEvents = summary?.total_events ?? manifest?.output?.event_count ?? null;
    const totalSignals = summary?.total_signals ?? manifest?.output?.signal_count ?? null;
    const totalFills = summary?.total_fills ?? manifest?.output?.fills_count ?? null;

    let healthClass = health?.class || summary?.health_class || null;
    if (!healthClass) {
      if (endedReason === 'queue_overflow') healthClass = 'failed';
      else if (endedReason !== 'finished') healthClass = 'degraded';
      else healthClass = 'healthy';
    }

    return {
      health_class: healthClass,
      total_events: totalEvents,
      total_signals: totalSignals,
      total_fills: totalFills,
      ended_reason: endedReason
    };
  }

  #recomputeAggregate(job) {
    const aggregate = {
      total_runs: job.runs.length,
      healthy_runs: 0,
      failed_runs: 0,
      degraded_runs: 0
    };

    for (const run of job.runs) {
      if (run.health_class === 'healthy') aggregate.healthy_runs += 1;
      else if (run.health_class === 'failed') aggregate.failed_runs += 1;
      else if (run.health_class === 'degraded') aggregate.degraded_runs += 1;
    }

    job.aggregate = aggregate;
  }

  #buildRunId(jobId, stream, symbol, date) {
    const base = `${jobId}|${stream}|${symbol}|${date}`;
    const hash = createHash('sha256').update(base).digest('hex').slice(0, 12);
    const safe = (value) => String(value).replace(/[^a-zA-Z0-9_-]/g, '_');
    return `bt_${safe(stream)}_${safe(symbol)}_${safe(date)}_${hash}`;
  }

  #dateRange(start, end) {
    const dates = [];
    const startDate = this.#parseDate(start);
    const endDate = this.#parseDate(end);
    if (!startDate || !endDate) return dates;

    let current = new Date(Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth(), startDate.getUTCDate()));
    const last = new Date(Date.UTC(endDate.getUTCFullYear(), endDate.getUTCMonth(), endDate.getUTCDate()));

    while (current <= last) {
      dates.push(current.toISOString().slice(0, 10));
      current.setUTCDate(current.getUTCDate() + 1);
    }
    return dates;
  }

  #parseDate(value) {
    if (!value || typeof value !== 'string') return null;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
    const date = new Date(value + 'T00:00:00Z');
    if (Number.isNaN(date.getTime())) return null;
    return date;
  }

  async #readJson(filePath) {
    try {
      const raw = await fsPromises.readFile(filePath, 'utf8');
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }

  async #sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
