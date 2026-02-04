import { createHash } from 'node:crypto';
import { ExecutionEngine } from '../../../core/execution/engine.js';
import { StrategyRuntime } from '../../../core/strategy/runtime/StrategyRuntime.js';
import { OrderingGuard } from '../../../core/strategy/safety/OrderingGuard.js';
import { ErrorContainment } from '../../../core/strategy/safety/ErrorContainment.js';
import { MetricsRegistry } from '../../../core/strategy/metrics/MetricsRegistry.js';
import { RiskManager } from '../../../core/risk/RiskManager.js';
import { ErrorPolicy, OrderingMode } from '../../../core/strategy/interface/types.js';
import { SignalEngine } from './SignalEngine.js';
import { ManifestManager } from './ManifestManager.js';
import { ManifestArchiver } from './ManifestArchiver.js';
import { RunHealthEvaluator } from './RunHealthEvaluator.js';
import { MLDecisionAdapter } from './MLDecisionAdapter.js';
import { MLActiveGate } from './MLActiveGate.js';
import { loadRuntimeConfig } from './RuntimeConfig.js';
import { ActiveAudit } from './ActiveAudit.js';
import { getValidationMetrics } from '../validationMetrics.js';
import { attachMlExtra, ensureLastCursor } from './manifestWriter.js';
import {
  ML_ACTIVE_ENABLED,
  ML_ACTIVE_KILL,
  ML_ACTIVE_MAX_DAILY_IMPACT_PCT
} from './constants.js';

const MAX_QUEUE_CAPACITY = Number(process.env.STRATEGYD_MAX_QUEUE_CAPACITY || 2000);

export class RuntimeAdapterV2 {
  #config;
  #executionEngine;
  #signalEngine;
  #manifestManager;
  #manifestArchiver;
  #healthEvaluator;
  #mlDecisionAdapter;
  #mlActiveGate;
  #runtimeConfig;
  #activeAudit;
  #lastMlResult = null;
  #lastMlAppliedWeight = null;
  #lastActiveApplied = false;
  #lastActiveReason = null;
  #activeDisabled = false;
  #dailyBaseSize = 0;
  #dailyAdjustedSize = 0;
  #runtime;
  #metrics;
  #orderingGuard;
  #runtimeOrderingGuard;
  #errorContainment;
  #eventQueue = [];
  #pendingResolve = null;
  #processingPromise = null;
  #pendingPostProcess = null;
  #processingStarted = false;
  #stopAtReached = false;
  #backpressureActive = false;
  #backpressureEndLogged = false;
  #resumeInProgress = false;
  #highWatermark = 1500;
  #lowWatermark = 500;
  #hardOverflowed = false;
  #eventsSinceYield = 0;
  #yieldEvery = 1000;
  #running = false;
  #paused = false;
  #status = 'RUNNING';
  #eventCount = 0;
  #signalCount = 0;
  #localEventIdx = 0;
  #errorsTotal = 0;
  #queueOverflowDisconnectsTotal = 0;
  #lastTs = null;
  #lastSeq = null;
  #lastProcessedCursor = null;
  #fillsForHash = [];
  #endedReason = 'finished';
  #lastEventTs = Date.now();
  #startedAt = null;
  #startedAtMs = null;
  #manifestFinalized = false;
  #stopped = false;
  #streamClient = null;
  #onAbort = null;

  constructor(config, { onAbort } = {}) {
    this.#config = config;
    this.#lastProcessedCursor = config.cursor || null;
    this.#onAbort = onAbort || null;

    this.#executionEngine = new ExecutionEngine({
      initialCapital: config.executionConfig?.initialCapital || 10000,
      feeRate: config.executionConfig?.feeRate || 0.0004,
      recordEquityCurve: true,
      requiresBbo: true
    });

    this.#signalEngine = new SignalEngine(config.strategyConfig || {});
    this.#manifestManager = new ManifestManager();
    this.#manifestArchiver = new ManifestArchiver();
    this.#healthEvaluator = new RunHealthEvaluator();
    this.#mlDecisionAdapter = new MLDecisionAdapter({
      enabled: process.env.ML_SHADOW_ENABLED === '1',
      decisionPath: process.env.ML_DECISION_PATH || null,
      modelPath: process.env.ML_MODEL_PATH || null
    });
    this.#runtimeConfig = loadRuntimeConfig({
      strategyId: config.strategyId || config.strategy_id || config.strategyConfig?.strategy_id || null,
      seed: config.seed || config.strategySeed || config.strategyConfig?.seed || config.runId || null
    });
    this.#mlActiveGate = new MLActiveGate(this.#runtimeConfig);
    this.#activeAudit = new ActiveAudit({
      runId: config.runId,
      maxWeight: this.#runtimeConfig.maxWeight
    });
    this.#yieldEvery = Number(process.env.STRATEGYD_YIELD_EVERY || 1000);
    this.#highWatermark = Number(process.env.STRATEGYD_BACKPRESSURE_HIGH || 1500);
    this.#lowWatermark = Number(process.env.STRATEGYD_BACKPRESSURE_LOW || 500);

    this.#metrics = new MetricsRegistry({ runId: config.runId });
    this.#orderingGuard = new OrderingGuard({ mode: OrderingMode.STRICT });
    this.#runtimeOrderingGuard = new OrderingGuard({ mode: OrderingMode.WARN });
    this.#errorContainment = new ErrorContainment({ policy: ErrorPolicy.FAIL_FAST });

    const strategy = this.#createStrategy();

    this.#runtime = new StrategyRuntime({
      dataset: {
        parquet: `sse:${config.dataset}`,
        meta: `sse:${config.dataset}`,
        stream: config.dataset,
        date: config.date,
        symbol: config.symbol
      },
      strategy,
      strategyConfig: config.strategyConfig || {},
      executionConfig: config.executionConfig || {},
      errorPolicy: ErrorPolicy.FAIL_FAST,
      orderingMode: OrderingMode.WARN,
      enableMetrics: true,
      seed: config.runId
    });

    this.#runtime
      .attachExecutionEngine(this.#executionEngine)
      .attachMetrics(this.#metrics)
      .attachOrderingGuard(this.#runtimeOrderingGuard)
      .attachErrorContainment(this.#errorContainment);

    // Risk management (Phase 2 Safety Guards)
    if (config.riskConfig?.enabled !== false) {
      const initialCapital = config.executionConfig?.initialCapital || 10000;
      const riskManager = new RiskManager(config.riskConfig || {}, initialCapital);
      this.#runtime.attachRiskManager(riskManager);
    }
  }

  setStreamClient(client) {
    this.#streamClient = client;
  }

  onStreamEnd() {
    if (this.#hardOverflowed) {
      return true;
    }
    if (this.#backpressureActive) {
      if (!this.#backpressureEndLogged) {
        this.#backpressureEndLogged = true;
        console.warn(
          `[RuntimeAdapterV2] run_id=${this.#config.runId} action=backpressure_stream_end`
        );
      }
      return false;
    }
    return true;
  }

  getManifestManager() {
    return this.#manifestManager;
  }

  async start() {
    if (this.#running) return;

    await this.#manifestManager.init();
    this.#startedAt = new Date().toISOString();
    this.#startedAtMs = Date.now();
    this.#running = true;
    this.#stopped = false;
    this.#endedReason = 'finished';
    this.#manifestFinalized = false;

    await this.#runtime.init();

    this.#processingPromise = this.#processQueue();

    this.#processingPromise.catch((err) => {
      if (err?.message?.includes('ORDERING_VIOLATION')) {
        this.#endedReason = 'ordering_error';
      } else if (this.#endedReason === 'finished') {
        this.#endedReason = 'error';
      }

      this.#errorsTotal++;

      console.error(
        `[RuntimeAdapterV2] run_id=${this.#config.runId} action=runtime_error error=${err.message}`
      );
    });

    console.log(
      `[RuntimeAdapterV2] run_id=${this.#config.runId} component=strategyd action=start dataset=${this.#config.dataset} symbol=${this.#config.symbol} date=${this.#config.date}`
    );
  }

  onSseEvent(event) {
    if (!this.#running || this.#stopped) return;
    if (this.#stopAtReached) return;

    if (this.#eventQueue.length >= MAX_QUEUE_CAPACITY) {
      console.error(
        `[RuntimeAdapterV2] run_id=${this.#config.runId} action=queue_overflow size=${this.#eventQueue.length} lastCursor=${this.#lastProcessedCursor}`
      );
      this.#queueOverflowDisconnectsTotal++;
      this.#hardOverflowed = true;
      this.#endedReason = 'queue_overflow';
      this.stop('queue_overflow');
      if (this.#onAbort) {
        this.#onAbort('queue_overflow');
      }
      return;
    }

    this.#eventQueue.push(event);
    this.#metrics.set('queue_size', this.#eventQueue.length);
    if (this.#eventQueue.length >= this.#highWatermark) {
      this.#enterBackpressure();
    }

    if (this.#pendingResolve) {
      const resolve = this.#pendingResolve;
      this.#pendingResolve = null;
      resolve();
    }
  }

  async #processQueue() {
    if (this.#processingStarted) return this.#processingPromise;
    this.#processingStarted = true;

    const iterator = {
      [Symbol.asyncIterator]: () => ({
        next: async () => this.#nextEvent()
      })
    };

    return this.#runtime.processStream(iterator);
  }

  async #nextEvent() {
    if (this.#pendingPostProcess) {
      const finalize = this.#pendingPostProcess;
      this.#pendingPostProcess = null;
      if (!this.#stopAtReached) {
        finalize();
      }
    }

    while (true) {
      if (this.#stopAtReached) {
        return { done: true, value: undefined };
      }
      if (this.#stopped && this.#eventQueue.length === 0) {
        return { done: true, value: undefined };
      }

      if (this.#eventQueue.length === 0) {
        this.#maybeExitBackpressure();
        await new Promise((resolve) => {
          this.#pendingResolve = resolve;
        });
        continue;
      }

      const event = this.#eventQueue.shift();
      this.#metrics.set('queue_size', this.#eventQueue.length);

      const currentCursor = event?.cursor ?? event?.payload?.cursor ?? null;
      if (this.#lastProcessedCursor && currentCursor === this.#lastProcessedCursor) {
        continue;
      }

      const normalizedEvent = this.#normalizeEvent(event);

      try {
        this.#orderingGuard.validate(normalizedEvent);
      } catch (err) {
        console.error(
          `[RuntimeAdapterV2] run_id=${this.#config.runId} action=ordering_error error=${err.message}`
        );
        this.#endedReason = 'ordering_error';
        this.#errorsTotal++;
        this.#eventQueue = [];
        this.stop('ordering_error');
        if (this.#onAbort) {
          this.#onAbort('ordering_error');
        }
        return { done: true, value: undefined };
      }

      this.#pendingPostProcess = () => {
        this.#localEventIdx++;
        this.#eventCount++;
        this.#eventsSinceYield++;
        this.#lastEventTs = Date.now();
        this.#lastProcessedCursor = currentCursor;
        this.#lastTs = normalizedEvent.ts_event ?? null;
        this.#lastSeq = normalizedEvent.seq ?? null;
        this.#maybeExitBackpressure();

        if (this.#config.stopAtEventIndex && this.#localEventIdx >= this.#config.stopAtEventIndex) {
          this.#stopAtReached = true;
          this.#eventQueue = [];
          console.log(
            `[RuntimeAdapterV2] run_id=${this.#config.runId} action=stop_at_index idx=${this.#localEventIdx}`
          );
          this.stop('stop_at_index');
          if (this.#onAbort) {
            this.#onAbort('stop_at_index');
          }
          if (this.#pendingResolve) {
            const resolve = this.#pendingResolve;
            this.#pendingResolve = null;
            resolve();
          }
        }
      };

      const effectiveYieldEvery = this.#backpressureActive ? 1 : this.#yieldEvery;
      if (effectiveYieldEvery > 0 && this.#eventsSinceYield >= effectiveYieldEvery) {
        this.#eventsSinceYield = 0;
        if (process.env.DEBUG_EVENTLOOP_YIELD === '1') {
          console.debug(`[RuntimeAdapterV2] run_id=${this.#config.runId} action=yield`);
        }
        await new Promise((resolve) => setImmediate(resolve));
      }

      return { done: false, value: normalizedEvent };
    }
  }

  #enterBackpressure() {
    if (this.#backpressureActive) return;
    this.#backpressureActive = true;
    this.#backpressureEndLogged = false;
    console.warn(
      `[RuntimeAdapterV2] run_id=${this.#config.runId} action=backpressure_enter queue=${this.#eventQueue.length}`
    );
    if (this.#streamClient?.running) {
      this.#streamClient.stop();
    }
  }

  #maybeExitBackpressure() {
    if (!this.#backpressureActive) return;
    if (this.#eventQueue.length > this.#lowWatermark) return;
    this.#backpressureActive = false;
    console.warn(
      `[RuntimeAdapterV2] run_id=${this.#config.runId} action=backpressure_exit queue=${this.#eventQueue.length}`
    );
    this.#resumeStream();
  }

  #resumeStream() {
    if (this.#hardOverflowed) return;
    if (!this.#streamClient || this.#streamClient.running) return;
    if (this.#resumeInProgress) return;
    this.#resumeInProgress = true;
    this.#streamClient
      .start()
      .catch((err) => {
        console.error(
          `[RuntimeAdapterV2] run_id=${this.#config.runId} action=backpressure_resume_error error=${err.message}`
        );
      })
      .finally(() => {
        this.#resumeInProgress = false;
      });
  }

  #createStrategy() {
    const adapter = this;

    return {
      async onEvent(event, context) {
        const payload = event?.payload || event;

        adapter.#mlDecisionAdapter.observeEvent(event);

        try {
          adapter.#executionEngine.onEvent(payload);
        } catch (err) {
          adapter.#errorsTotal++;
          return;
        }

        if (adapter.#paused) return;

        const signal = adapter.#signalEngine.onEvent(event);
        if (!signal) return;

        adapter.#signalCount++;
        adapter.#metrics.increment('signals_total');

        adapter.#mlDecisionAdapter.computeShadow(event);
        adapter.#lastMlResult = adapter.#mlDecisionAdapter.getLastResult();
        const appliedWeight = adapter.#applyMlWeight(signal.qty, signal.side, signal.ts_event, signal.seq);
        adapter.#lastMlAppliedWeight = appliedWeight;

        try {
          const fill = context.placeOrder({
            symbol: signal.symbol,
            side: signal.side,
            qty: signal.qty * appliedWeight,
            ts_event: signal.ts_event
          });

          adapter.#fillsForHash.push({
            id: fill.id,
            side: fill.side,
            price: fill.fillPrice,
            qty: fill.qty,
            ts: fill.ts_event.toString()
          });
        } catch (err) {
          console.error(`[EXECUTION_ERROR] ${err.message}`);
        }
      },

      getState() {
        return adapter.#signalEngine.snapshot();
      }
    };
  }

  #normalizeEvent(event) {
    if (event && typeof event === 'object' && event.payload && typeof event.payload === 'object') {
      return { ...event, ...event.payload };
    }
    return event;
  }

  pause() {
    this.#paused = true;
    this.#status = 'PAUSED';
  }

  resume() {
    this.#paused = false;
    this.#status = 'RUNNING';
  }

  setEndedReason(reason) {
    if (reason) {
      this.#endedReason = reason;
    }
  }

  async kill() {
    this.#endedReason = 'kill';
    const snapshot = this.#executionEngine.snapshot();

    for (const [symbol, pos] of Object.entries(snapshot.positions)) {
      if (pos.size === 0) continue;
      const side = pos.size > 0 ? 'SELL' : 'BUY';
      const qty = Math.abs(pos.size);
      try {
        const fill = this.#executionEngine.onOrder({
          symbol,
          side,
          qty,
          ts_event: this.#lastTs
        });
        this.#fillsForHash.push({
          id: fill.id,
          side: fill.side,
          price: fill.fillPrice,
          qty: fill.qty,
          ts: fill.ts_event.toString()
        });
      } catch (err) {
        console.error(`[KILL_ERROR] ${err.message}`);
      }
    }

    this.stop('kill');
    if (this.#onAbort) {
      this.#onAbort('kill');
    }
    await this.finalizeManifest();
  }

  stop(reason = null) {
    if (reason && this.#endedReason === 'finished') {
      this.#endedReason = reason;
    }

    this.#running = false;
    this.#stopped = true;
    if (reason === 'stop_at_index') {
      this.#stopAtReached = true;
      this.#eventQueue = [];
    }

    if (this.#pendingResolve) {
      const resolve = this.#pendingResolve;
      this.#pendingResolve = null;
      resolve();
    }
  }

  getSnapshot() {
    return this.#executionEngine.snapshot();
  }

  getStats() {
    return {
      runId: this.#config.runId,
      status: this.#status,
      eventCount: this.#eventCount,
      signalCount: this.#signalCount,
      reconnectCount: this.#streamClient?.reconnectCount || 0,
      cursorProgress: this.#lastProcessedCursor,
      queueSize: this.#eventQueue.length
    };
  }

  getRunSnapshot() {
    const snapshot = this.#executionEngine.snapshot();
    return {
      run_id: this.#config.runId,
      event_count: this.#eventCount,
      local_event_idx: this.#localEventIdx,
      state_hash: this.#computeStateHash(),
      fills_hash: this.#computeFillsHash(),
      equity_end: snapshot.equity,
      last_cursor: this.#lastProcessedCursor,
      positions: snapshot.positions
    };
  }

  async finalizeManifest() {
    if (this.#manifestFinalized) return;
    this.#manifestFinalized = true;

    if (this.#processingPromise) {
      try {
        await this.#processingPromise;
      } catch {
        // Error already recorded in adapter state
      }
    }

    const runSnap = this.getRunSnapshot();
    const manifest = {
      run_id: runSnap.run_id,
      started_at: this.#startedAt,
      ended_at: new Date().toISOString(),
      ended_reason: this.#endedReason,
      strategy: {
        id: this.#config.strategyId
          || this.#config.strategy_id
          || this.#config.strategyConfig?.strategy_id
          || process.env.STRATEGY_ID
          || null,
        seed: this.#config.seed
          || this.#config.strategySeed
          || this.#config.strategyConfig?.seed
          || this.#config.runId
          || null
      },
      input: {
        dataset: this.#config.dataset,
        symbol: this.#config.symbol,
        date: this.#config.date
      },
      output: {
        last_cursor: runSnap.last_cursor,
        fills: runSnap.local_event_idx,
        event_count: runSnap.event_count,
        fills_count: Object.keys(runSnap.positions).length,
        equity_end: runSnap.equity_end,
        state_hash: runSnap.state_hash,
        fills_hash: runSnap.fills_hash,
        reconnects: this.#streamClient?.reconnectCount || 0
      }
    };

    manifest.output.fills_count = this.#executionEngine.snapshot().fills.length;
    const mlMode = ML_ACTIVE_ENABLED && !ML_ACTIVE_KILL
      ? 'active'
      : (process.env.ML_SHADOW_ENABLED === '1' ? 'shadow' : 'off');
    attachMlExtra(manifest, this.#lastMlResult, {
      applied_weight: this.#lastMlAppliedWeight,
      active_mode: ML_ACTIVE_ENABLED && !ML_ACTIVE_KILL,
      active_applied: this.#lastActiveApplied,
      active_reason: this.#lastActiveReason,
      mode: mlMode
    });

    ensureLastCursor(
      manifest,
      runSnap.last_cursor
        || this.#lastProcessedCursor
        || this.#streamClient?.lastCursor
        || this.#config.cursor
    );

    const manifestPath = await this.#manifestManager.save(manifest);

    const durationMs = this.#startedAtMs ? Date.now() - this.#startedAtMs : null;
    console.log(
      `[RuntimeAdapterV2] run_id=${manifest.run_id} component=strategyd action=manifest_saved events=${runSnap.event_count} signals=${this.#signalCount} duration_ms=${durationMs ?? 'unknown'} state=${runSnap.state_hash.substring(0, 8)} fills=${runSnap.fills_hash.substring(0, 8)}`
    );

    if (manifestPath && process.env.RUN_ARCHIVE_ENABLED === '1') {
      this.#manifestArchiver.archive(manifestPath, manifest).catch(() => {});
    }

    if (manifestPath) {
      this.#healthEvaluator.evaluateManifest(manifestPath, manifest).catch(() => {});
    }
  }

  renderMetrics() {
    const snapshot = this.getSnapshot();
    const stats = this.getStats();
    const validationMetrics = getValidationMetrics();
    return [
      '# HELP strategyd_replay_events_total Total events received',
      '# TYPE strategyd_replay_events_total counter',
      `strategyd_replay_events_total ${stats.eventCount}`,
      '',
      '# HELP strategyd_reconnects_total Total reconnections',
      '# TYPE strategyd_reconnects_total counter',
      `strategyd_reconnects_total ${stats.reconnectCount}`,
      '',
      '# HELP strategyd_queue_overflow_disconnects_total Total queue overflows',
      '# TYPE strategyd_queue_overflow_disconnects_total counter',
      `strategyd_queue_overflow_disconnects_total ${this.#queueOverflowDisconnectsTotal}`,
      '',
      '# HELP strategyd_equity Current portfolio equity',
      '# TYPE strategyd_equity gauge',
      `strategyd_equity ${snapshot.equity || 0}`,
      '',
      '# HELP strategyd_stream_connects_total Total stream connection attempts',
      '# TYPE strategyd_stream_connects_total counter',
      `strategyd_stream_connects_total ${this.#streamClient?.metrics?.connectsTotal || 0}`,
      '',
      '# HELP strategyd_stream_timeouts_total Total stream timeouts (5s/30s)',
      '# TYPE strategyd_stream_timeouts_total counter',
      `strategyd_stream_timeouts_total ${this.#streamClient?.metrics?.timeoutsTotal || 0}`,
      '',
      '# HELP strategyd_stream_first_event_latency_ms Latency of the first event after connection',
      '# TYPE strategyd_stream_first_event_latency_ms gauge',
      `strategyd_stream_first_event_latency_ms ${this.#streamClient?.metrics?.firstEventLatencyMs || 0}`,
      '',
      '# HELP strategyd_validation_triggered_total Snapshot validations triggered',
      '# TYPE strategyd_validation_triggered_total counter',
      `strategyd_validation_triggered_total ${validationMetrics.strategyd_validation_triggered_total}`,
      '',
      '# HELP strategyd_validation_diverged_total Snapshot validations diverged',
      '# TYPE strategyd_validation_diverged_total counter',
      `strategyd_validation_diverged_total ${validationMetrics.strategyd_validation_diverged_total}`
    ].join('\n');
  }

  #applyMlWeight(baseQty, direction, tsEvent, seq) {
    if (this.#activeDisabled) {
      this.#lastActiveApplied = false;
      this.#lastActiveReason = this.#lastActiveReason || 'safety_disabled';
      return 1.0;
    }
    const gate = this.#mlActiveGate.isActiveAllowed();
    if (!gate.allowed) {
      this.#lastActiveApplied = false;
      this.#lastActiveReason = gate.reason;
      return 1.0;
    }
    if (!Number.isFinite(baseQty) || baseQty <= 0) return 1.0;

    const mlResult = this.#mlDecisionAdapter.getLastResult();
    if (!mlResult) {
      this.#lastActiveApplied = false;
      this.#lastActiveReason = 'missing_ml_result';
      return 1.0;
    }

    let weight;
    try {
      weight = this.#mlDecisionAdapter.computeWeight(mlResult.confidence);
    } catch {
      return 1.0;
    }

    if (!Number.isFinite(weight) || weight <= 0) return 1.0;
    const rawWeight = weight;
    if (Number.isFinite(this.#runtimeConfig?.maxWeight)) {
      weight = Math.min(weight, this.#runtimeConfig.maxWeight);
    }
    const adjustedQty = baseQty * weight;
    if (!Number.isFinite(adjustedQty) || adjustedQty <= 0) return 1.0;

    const audit = this.#activeAudit.checkAndRecord({
      baseQty,
      mlWeight: rawWeight,
      appliedQty: adjustedQty,
      direction,
      tsEvent,
      seq
    });
    if (!audit.ok) {
      this.#activeDisabled = true;
      this.#lastActiveApplied = false;
      this.#lastActiveReason = audit.reason;
      return 1.0;
    }

    const baseTotal = this.#dailyBaseSize + baseQty;
    const adjustedTotal = this.#dailyAdjustedSize + adjustedQty;
    const ratio = baseTotal > 0 ? adjustedTotal / baseTotal : 1.0;

    const dailyCap = Number.isFinite(this.#runtimeConfig?.dailyCap)
      ? this.#runtimeConfig.dailyCap
      : ML_ACTIVE_MAX_DAILY_IMPACT_PCT;
    if (Math.abs(ratio - 1.0) > dailyCap) {
      this.#lastActiveApplied = false;
      this.#lastActiveReason = 'daily_cap';
      return 1.0;
    }

    this.#dailyBaseSize = baseTotal;
    this.#dailyAdjustedSize = adjustedTotal;
    if (weight === 1.0) {
      this.#lastActiveApplied = false;
      this.#lastActiveReason = 'weight_neutral';
      return 1.0;
    }
    this.#lastActiveApplied = true;
    this.#lastActiveReason = gate.reason;
    return weight;
  }

  getLastEventTs() {
    return this.#lastEventTs;
  }

  getLastCursor() {
    return this.#lastProcessedCursor;
  }

  #canonicalStringify(obj) {
    if (typeof obj === 'bigint') return JSON.stringify(obj.toString());
    if (obj === null || typeof obj !== 'object') return JSON.stringify(obj);
    if (Array.isArray(obj)) return '[' + obj.map(item => this.#canonicalStringify(item)).join(',') + ']';

    const keys = Object.keys(obj).sort();
    return '{' + keys.map(k => `"${k}":${this.#canonicalStringify(obj[k])}`).join(',') + '}';
  }

  #computeStateHash() {
    const execSnapshot = this.#executionEngine.snapshot();
    const signalState = this.#signalEngine.snapshot();
    const combined = { exec: execSnapshot, signal: signalState };
    return createHash('sha256').update(this.#canonicalStringify(combined)).digest('hex');
  }

  #computeFillsHash() {
    return createHash('sha256').update(this.#canonicalStringify(this.#fillsForHash)).digest('hex');
  }
}

export default RuntimeAdapterV2;
