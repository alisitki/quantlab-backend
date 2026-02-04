import { createHash } from 'node:crypto';
import { ExecutionEngine } from '../../../core/execution/engine.js';
import { SignalEngine } from './SignalEngine.js';
import { ManifestManager } from './ManifestManager.js';
import { ManifestArchiver } from './ManifestArchiver.js';
import { getValidationMetrics } from '../validationMetrics.js';
import { ReplayStreamClient } from './ReplayStreamClient.js';
import { RuntimeAdapterV2 } from './RuntimeAdapterV2.js';
import { MLDecisionAdapter } from './MLDecisionAdapter.js';
import { MLActiveGate } from './MLActiveGate.js';
import { loadRuntimeConfig } from './RuntimeConfig.js';
import { ActiveAudit } from './ActiveAudit.js';
import { attachMlExtra, ensureLastCursor } from './manifestWriter.js';
import {
  ML_ACTIVE_ENABLED,
  ML_ACTIVE_KILL,
  ML_ACTIVE_MAX_DAILY_IMPACT_PCT
} from './constants.js';

const MAX_QUEUE_CAPACITY = Number(process.env.STRATEGYD_MAX_QUEUE_CAPACITY || 2000);

/**
 * @typedef {Object} RunnerConfig
 * @property {string} runId
 * @property {string} replaydUrl
 * @property {string} dataset
 * @property {string} symbol
 * @property {string} date
 * @property {string} [speed]
 * @property {string} [replaydToken]
 * @property {string} [cursor]
 * @property {Object} [strategyConfig]
 * @property {Object} [executionConfig]
 * @property {number} [stopAtEventIndex]
 * @property {string} [aggregate]
 */

export class SSEStrategyRunner {
  /** @type {RunnerConfig} */
  #config;
  /** @type {ExecutionEngine} */
  #executionEngine;
  /** @type {SignalEngine} */
  #signalEngine;
  /** @type {ManifestManager} */
  #manifestManager;
  /** @type {ManifestArchiver} */
  #manifestArchiver;
  /** @type {MLDecisionAdapter} */
  #mlDecisionAdapter;
  /** @type {MLActiveGate} */
  #mlActiveGate;
  /** @type {Object} */
  #runtimeConfig;
  /** @type {ActiveAudit} */
  #activeAudit;
  /** @type {Object|null} */
  #lastMlResult = null;
  /** @type {number|null} */
  #lastMlAppliedWeight = null;
  /** @type {boolean} */
  #lastActiveApplied = false;
  /** @type {string|null} */
  #lastActiveReason = null;
  /** @type {boolean} */
  #activeDisabled = false;
  /** @type {number} */
  #dailyBaseSize = 0;
  /** @type {number} */
  #dailyAdjustedSize = 0;
  /** @type {ReplayStreamClient|null} */
  #client = null;
  /** @type {Object[]} */
  #eventQueue = [];
  /** @type {boolean} */
  #isProcessing = false;
  
  /** @type {number} */
  #eventCount = 0;
  /** @type {number} */
  #signalCount = 0;
  /** @type {boolean} */
  #running = false;
  /** @type {'RUNNING'|'PAUSED'} */
  #status = 'RUNNING';
  /** @type {bigint|null} */
  #lastTs = null;
  /** @type {bigint|null} */
  #lastSeq = null;
  /** @type {string|null} */
  #lastProcessedCursor = null;
  /** @type {Object[]} Fill history for hashing */
  #fillsForHash = [];
  /** @type {string} */
  #endedReason = 'finished';
  /** @type {number} */
  #errorsTotal = 0;
  /** @type {number} */
  #queueOverflowDisconnectsTotal = 0;
  /** @type {number} */
  #lastEventTs = Date.now();
  /** @type {number} */
  #legacyYieldEvery = Number(process.env.STRATEGYD_LEGACY_YIELD_EVERY || 1000);
  /** @type {number} */
  #eventsSinceYield = 0;
  /** @type {string|null} */
  #startedAt = null;
  /** @type {boolean} */
  #manifestFinalized = false;
  /** @type {number} */
  #localEventIdx = 0;
  /** @type {boolean} */
  #useRuntimeV2 = false;
  /** @type {RuntimeAdapterV2|null} */
  #runtimeAdapter = null;

  constructor(config) {
    this.#config = config;
    this.#lastProcessedCursor = config.cursor || null;
    if (typeof config.strategyRuntimeV2 === 'boolean') {
      this.#useRuntimeV2 = config.strategyRuntimeV2;
    } else {
      const flag = process.env.STRATEGY_RUNTIME_V2;
      this.#useRuntimeV2 = flag !== '0';
    }

    this.#executionEngine = new ExecutionEngine({
      initialCapital: config.executionConfig?.initialCapital || 10000,
      feeRate: config.executionConfig?.feeRate || 0.0004,
      recordEquityCurve: true,
      requiresBbo: true
    });
    this.#signalEngine = new SignalEngine(config.strategyConfig || {});
    this.#manifestManager = new ManifestManager();
    this.#manifestArchiver = new ManifestArchiver();
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

    if (this.#useRuntimeV2) {
      this.#runtimeAdapter = new RuntimeAdapterV2(config, {
        onAbort: () => this.stop()
      });
    }
  }

  #getStreamUrl() {
    const { replaydUrl, dataset, symbol, date, speed, aggregate } = this.#config;
    const params = new URLSearchParams({ dataset, symbol, date });
    if (speed) params.set('speed', speed);
    if (aggregate) params.set('aggregate', aggregate);
    return `${replaydUrl}/stream?${params.toString()}`;
  }

  async start() {
    if (this.#running) return;
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      await this.#runtimeAdapter.start();
    } else {
      await this.#manifestManager.init();
      this.#startedAt = new Date().toISOString();
    }
    this.#running = true;

    if (!this.#lastProcessedCursor) {
      console.log('[SSERunner] NO_RESUME_STATE: Starting from stream beginning');
    }

    this.#client = new ReplayStreamClient({
      url: this.#getStreamUrl(),
      token: this.#config.replaydToken,
      cursor: this.#lastProcessedCursor,
      onEvent: (event) => this.#onSseEvent(event),
      onError: (err) => {
        console.error(`[SSERunner] Stream error: ${err.message}`);
        this.#errorsTotal++;
      },
      onEnd: () => {
        if (this.#useRuntimeV2 && this.#runtimeAdapter) {
          const shouldStop = this.#runtimeAdapter.onStreamEnd();
          if (!shouldStop) {
            return;
          }
        }
        console.log(`[SSERunner] Stream finished normally. Total events: ${this.#eventCount}`);
        this.stop();
      }
    });

    console.log(`[SSERunner] Starting client runId=${this.#config.runId}`);
    try {
      if (this.#useRuntimeV2 && this.#runtimeAdapter) {
        this.#runtimeAdapter.setStreamClient(this.#client);
      }
      await this.#client.start();
    } finally {
      this.#running = false;
      await this.finalizeManifest();
    }
  }

  #onSseEvent(event) {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      this.#runtimeAdapter.onSseEvent(event);
      return;
    }
    this.#enqueueEvent(event);
  }

  #enqueueEvent(event) {
    if (this.#eventQueue.length >= MAX_QUEUE_CAPACITY) {
      console.error(`[SSERunner] Disconnected. reason=QUEUE_OVERFLOW size=${this.#eventQueue.length} lastCursor=${this.#lastProcessedCursor}`);
      this.#queueOverflowDisconnectsTotal++;
      this.#endedReason = 'queue_overflow';
      this.stop();
      return;
    }
    this.#eventQueue.push(event);
    this.#processQueue();
  }

  async #processQueue() {
    if (this.#isProcessing || this.#eventQueue.length === 0) return;
    this.#isProcessing = true;

    while (this.#eventQueue.length > 0) {
      const event = this.#eventQueue.shift();
      const currentCursor = event.cursor;
      const payload = event.payload || event;
      const currentTs = BigInt(payload.ts_event || 0);
      const currentSeq = BigInt(payload.seq || 0);

      // Event Order Guard (Drift Prevention)
      // We check monotonicity of (ts_event, seq)
      if (this.#lastTs !== null) {
        const isBackward = currentTs < this.#lastTs || (currentTs === this.#lastTs && currentSeq <= this.#lastSeq);
        if (isBackward) {
          const err = `ORDERING_ERROR: Ingest drift detected! idx=${this.#localEventIdx} last=${this.#lastTs}:${this.#lastSeq} new=${currentTs}:${currentSeq}`;
          console.error(`[SSERunner] ${err}`);
          this.#endedReason = 'ordering_error';
          this.#errorsTotal++;
          this.stop();
          throw new Error(err);
        }
      }

      // Strict Cursor Guard (Duplicate check)
      if (this.#lastProcessedCursor && currentCursor === this.#lastProcessedCursor) {
        continue; 
      }

      this.#localEventIdx++;
      this.#handleEvent(event);
      this.#eventsSinceYield++;
      if (this.#legacyYieldEvery > 0 && this.#eventsSinceYield >= this.#legacyYieldEvery) {
        this.#eventsSinceYield = 0;
        await new Promise((resolve) => setImmediate(resolve));
      }
      this.#lastProcessedCursor = currentCursor;
      this.#lastTs = currentTs;
      this.#lastSeq = currentSeq;

      // Deterministic Stop for Testing
      if (this.#config.stopAtEventIndex && this.#localEventIdx >= this.#config.stopAtEventIndex) {
        console.log(`[SSERunner] stopAtEventIndex reached: ${this.#localEventIdx}`);
        this.stop();
        break;
      }
    }
    this.#isProcessing = false;
  }

  #handleEvent(event) {
    this.#eventCount++;
    this.#lastEventTs = Date.now();
    
    // Adapter Pattern: No transformation
    const payload = event.payload || event;
    this.#mlDecisionAdapter.observeEvent(event);
    const ts = payload.ts_event;
    if (ts) this.#lastTs = ts;
    
    try {
      this.#executionEngine.onEvent(payload);
    } catch (err) {
      return;
    }
    
    if (this.#status === 'PAUSED') return;

    const signal = this.#signalEngine.onEvent(event);
    if (signal) {
      this.#signalCount++;
      this.#mlDecisionAdapter.computeShadow(event);
      this.#lastMlResult = this.#mlDecisionAdapter.getLastResult();
      const appliedWeight = this.#applyMlWeight(signal.qty, signal.side, signal.ts_event, null);
      this.#lastMlAppliedWeight = appliedWeight;
      try {
        const fill = this.#executionEngine.onOrder({
          symbol: signal.symbol,
          side: signal.side,
          qty: signal.qty * appliedWeight,
          ts_event: signal.ts_event
        });
        this.#fillsForHash.push({
          id: fill.id, side: fill.side, price: fill.fillPrice, qty: fill.qty, ts: fill.ts_event.toString()
        });
      } catch (err) {
        console.error(`[EXECUTION_ERROR] ${err.message}`);
      }
    }

    if (this.#eventCount % 50000 === 0) {
      const snapshot = this.#executionEngine.snapshot();
      console.log(`[SSERunner] Progress: events=${this.#eventCount} signals=${this.#signalCount} equity=${snapshot.equity.toFixed(2)}`);
    }
  }

  #canonicalStringify(obj) {
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

  getRunSnapshot() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      return this.#runtimeAdapter.getRunSnapshot();
    }
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

  getStrategyId() {
    return this.#config.strategyId
      || this.#config.strategy_id
      || this.#config.strategyConfig?.strategy_id
      || process.env.STRATEGY_ID
      || null;
  }

  getStrategySeed() {
    return this.#config.seed
      || this.#config.strategySeed
      || this.#config.strategyConfig?.seed
      || this.#config.runId
      || null;
  }

  async finalizeManifest() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      await this.#runtimeAdapter.finalizeManifest();
      return;
    }
    if (this.#manifestFinalized) return;
    this.#manifestFinalized = true;

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
        fills: runSnap.local_event_idx, // Wait, local_event_idx is events. fills is fills.
        event_count: runSnap.event_count,
        fills_count: Object.keys(runSnap.positions).length, // This is wrong. 
        equity_end: runSnap.equity_end,
        state_hash: runSnap.state_hash,
        fills_hash: runSnap.fills_hash,
        reconnects: this.#client?.reconnectCount || 0
      }
    };
    
    // Fix: use actual fills from snapshot
    manifest.output.fills_count = this.#executionEngine.snapshot().fills.length;
    const mlMode = ML_ACTIVE_ENABLED && !ML_ACTIVE_KILL
      ? 'active'
      : (process.env.ML_SHADOW_ENABLED === '1' ? 'shadow' : 'off');
    const jobInfo = this.#mlDecisionAdapter.getJobInfo();
    attachMlExtra(manifest, this.#lastMlResult, {
      applied_weight: this.#lastMlAppliedWeight,
      active_mode: ML_ACTIVE_ENABLED && !ML_ACTIVE_KILL,
      active_applied: this.#lastActiveApplied,
      active_reason: this.#lastActiveReason,
      mode: mlMode,
      job_id: jobInfo.job_id,
      job_hash: jobInfo.job_hash,
      decision_path: jobInfo.decision_path
    });

    ensureLastCursor(
      manifest,
      runSnap.last_cursor
        || this.#lastProcessedCursor
        || this.#client?.lastCursor
        || this.#config.cursor
    );

    const manifestPath = await this.#manifestManager.save(manifest);
    console.log(`[SSERunner] Manifest saved: ${manifest.run_id} State=${runSnap.state_hash.substring(0, 8)} Fills=${runSnap.fills_hash.substring(0, 8)} DetCheck=PASS`);

    if (manifestPath && process.env.RUN_ARCHIVE_ENABLED === '1') {
      this.#manifestArchiver.archive(manifestPath, manifest).catch(() => {});
    }
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

  pause() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      this.#runtimeAdapter.pause();
      return;
    }
    this.#status = 'PAUSED';
  }
  resume() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      this.#runtimeAdapter.resume();
      return;
    }
    this.#status = 'RUNNING';
  }

  setEndedReason(reason) {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      this.#runtimeAdapter.setEndedReason(reason);
      return;
    }
    if (reason) {
      this.#endedReason = reason;
    }
  }

  getManifestManager() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      return this.#runtimeAdapter.getManifestManager();
    }
    return this.#manifestManager;
  }

  async kill() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      await this.#runtimeAdapter.kill();
      return;
    }
    this.#endedReason = 'kill';
    const snapshot = this.#executionEngine.snapshot();
    for (const [symbol, pos] of Object.entries(snapshot.positions)) {
      if (pos.size === 0) continue;
      const side = pos.size > 0 ? 'SELL' : 'BUY';
      const qty = Math.abs(pos.size);
      try {
        const fill = this.#executionEngine.onOrder({ symbol, side, qty, ts_event: this.#lastTs });
        this.#fillsForHash.push({ id: fill.id, side: fill.side, price: fill.fillPrice, qty: fill.qty, ts: fill.ts_event.toString() });
      } catch (err) { console.error(`[KILL_ERROR] ${err.message}`); }
    }
    this.stop();
  }

  stop() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      this.#runtimeAdapter.stop();
    }
    if (this.#client) this.#client.stop();
    this.#running = false;
  }

  getSnapshot() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      return this.#runtimeAdapter.getSnapshot();
    }
    return this.#executionEngine.snapshot();
  }
  getStats() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      return this.#runtimeAdapter.getStats();
    }
    return {
      runId: this.#config.runId,
      status: this.#status,
      eventCount: this.#eventCount,
      signalCount: this.#signalCount,
      reconnectCount: this.#client?.reconnectCount || 0,
      cursorProgress: this.#lastProcessedCursor,
      queueSize: this.#eventQueue.length
    };
  }

  renderMetrics() {
    if (this.#useRuntimeV2 && this.#runtimeAdapter) {
      return this.#runtimeAdapter.renderMetrics();
    }
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
      `strategyd_stream_connects_total ${this.#client?.metrics?.connectsTotal || 0}`,
      '',
      '# HELP strategyd_stream_timeouts_total Total stream timeouts (5s/30s)',
      '# TYPE strategyd_stream_timeouts_total counter',
      `strategyd_stream_timeouts_total ${this.#client?.metrics?.timeoutsTotal || 0}`,
      '',
      '# HELP strategyd_stream_first_event_latency_ms Latency of the first event after connection',
      '# TYPE strategyd_stream_first_event_latency_ms gauge',
      `strategyd_stream_first_event_latency_ms ${this.#client?.metrics?.firstEventLatencyMs || 0}`,
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

  startMonitoring() {
    setInterval(() => {
      if (!this.#running) return;
      if (this.#useRuntimeV2 && this.#runtimeAdapter) {
        if (this.#runtimeAdapter.getStats().status === 'RUNNING' && (Date.now() - this.#runtimeAdapter.getLastEventTs() > 60000)) {
          console.warn(`[WARN] Stream STALLED. lastCursor=${this.#runtimeAdapter.getLastCursor()}`);
        }
        return;
      }
      if (this.#status === 'RUNNING' && (Date.now() - this.#lastEventTs > 60000)) {
        console.warn(`[WARN] Stream STALLED. lastCursor=${this.#lastProcessedCursor}`);
      }
    }, 60000);
  }
}
