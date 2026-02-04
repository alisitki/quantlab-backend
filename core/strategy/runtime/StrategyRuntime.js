/**
 * QuantLab Strategy Runtime — Main Orchestrator
 * 
 * PHASE 3: Lifecycle & Runtime
 * 
 * Production-grade runtime for executing strategies with determinism guarantees.
 * Orchestrates ReplayEngine, ExecutionEngine, and Strategy with proper lifecycle.
 * 
 * Features:
 * - Deterministic run ID generation
 * - Lifecycle state machine
 * - Cursor tracking for resume
 * - Error containment
 * - Metrics collection
 * - Checkpoint support
 * 
 * @module core/strategy/runtime/StrategyRuntime
 */

import { EventEmitter } from 'node:events';
import { RuntimeConfig } from './RuntimeConfig.js';
import { RuntimeContext, createRuntimeContext } from './RuntimeContext.js';
import { RuntimeLifecycle } from './RuntimeLifecycle.js';
import { RuntimeState, createRuntimeState } from './RuntimeState.js';
import { computeRunId, computeHash } from '../safety/DeterminismValidator.js';
import { RunLifecycleStatus } from '../interface/types.js';
import { emitAudit } from '../../audit/AuditWriter.js';
import { canonicalStringify } from '../state/StateSerializer.js';
import crypto from 'node:crypto';
import { RunArchiveWriter } from '../../run-archive/RunArchiveWriter.js';

/**
 * @typedef {import('../interface/types.js').StrategyV2} StrategyV2
 * @typedef {import('../interface/types.js').RunManifest} RunManifest
 */

/**
 * Deterministic Strategy Runtime.
 * 
 * @extends EventEmitter
 */
export class StrategyRuntime extends EventEmitter {
  /** @type {RuntimeConfig} */
  #config;
  
  /** @type {RuntimeLifecycle} */
  #lifecycle;
  
  /** @type {RuntimeState} */
  #state;
  
  /** @type {RuntimeContext} */
  #context;
  
  /** @type {string} */
  #runId;
  
  /** @type {Object|null} */
  #replayEngine;
  
  /** @type {Object|null} */
  #executionEngine;
  
  /** @type {StrategyV2} */
  #strategy;
  
  /** @type {Object|null} */
  #metrics;
  
  /** @type {Object|null} */
  #orderingGuard;
  
  /** @type {Object|null} */
  #errorContainment;
  
  /** @type {Object|null} */
  #checkpointManager;

  /** @type {Object|null} */
  #riskManager;

  /** @type {Object|null} */
  #mlAdapter;

  /** @type {Object|null} */
  #lastEvent;
  
  /** @type {string} */
  #endedReason;
  
  /** @type {string|null} */
  #replayRunId;

  /** @type {function|null} */
  #eventObserver;
  
  /** @type {number|null} */
  #replayStartedAt;
  
  /** @type {number|null} */
  #replayFinishedAt;
  
  /** @type {bigint|null} */
  #replayFirstEventTs;
  
  /** @type {bigint|null} */
  #replayLastEventTs;
  
  /** @type {string|null} */
  #replayManifestId;
  
  /** @type {string|null} */
  #replayStopReason;
  
  /** @type {number|null} */
  #replayEmittedEventCount;
  
  /** @type {Array<Object>} */
  #decisions;
  
  /**
   * Create a new StrategyRuntime.
   * 
   * @param {Object} options - Runtime options
   */
  constructor(options) {
    super();
    
    // Create and validate config
    this.#config = options instanceof RuntimeConfig 
      ? options 
      : new RuntimeConfig(options);
    
    // Generate deterministic run ID
    this.#runId = computeRunId({
      dataset: this.#config.dataset,
      config: this.#config.strategyConfig,
      seed: this.#config.seed
    });
    
    // Initialize lifecycle
    this.#lifecycle = new RuntimeLifecycle();
    this.#lifecycle.on('transition', (event) => {
      this.emit('lifecycle', event);
    });
    
    // Initialize state
    this.#state = createRuntimeState({ runId: this.#runId });
    
    // Store strategy reference
    this.#strategy = this.#config.strategy;
    
    // Initialize optional components
    this.#replayEngine = null;
    this.#executionEngine = null;
    this.#metrics = null;
    this.#orderingGuard = null;
    this.#errorContainment = null;
    this.#checkpointManager = null;
    this.#riskManager = null;
    this.#mlAdapter = null;
    this.#lastEvent = null;
    this.#endedReason = 'finished';
    this.#replayRunId = null;
    this.#replayStartedAt = null;
    this.#replayFinishedAt = null;
    this.#replayStopReason = null;
    this.#replayEmittedEventCount = null;
    this.#decisions = [];
    this.#replayFirstEventTs = null;
    this.#replayLastEventTs = null;
    this.#replayManifestId = null;
    this.#eventObserver = null;
    
    // Create context (will be updated with placeOrder after execution engine attachment)
    this.#context = null;
  }
  
  // ============================================================================
  // GETTERS
  // ============================================================================
  
  /** @returns {string} */
  get runId() { return this.#runId; }
  
  /** @returns {string} */
  get status() { return this.#lifecycle.status; }
  
  /** @returns {RuntimeConfig} */
  get config() { return this.#config; }
  
  /** @returns {RuntimeLifecycle} */
  get lifecycle() { return this.#lifecycle; }
  
  /** @returns {RuntimeState} */
  get state() { return this.#state; }
  
  /** @returns {RuntimeContext} */
  get context() { return this.#context; }

  /** @returns {string|null} */
  get replayRunId() { return this.#replayRunId; }

  /** @returns {Array<Object>} */
  get decisions() { return this.#decisions.map(d => ({ ...d })); }

  /** @returns {string} */
  get decisionHash() {
    return computeHash(this.#decisions);
  }

  /**
   * Attach an observer called after each event is processed.
   * @param {function} fn
   */
  setEventObserver(fn) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.READY);
    this.#eventObserver = fn;
  }

  /**
   * Set replay_run_id for external (live) runs.
   * Must be called before start/processStream.
   * @param {string} replayRunId
   */
  setReplayRunId(replayRunId) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.READY);
    this.#replayRunId = replayRunId;
  }
  
  // ============================================================================
  // COMPONENT ATTACHMENT
  // ============================================================================
  
  /**
   * Attach a ReplayEngine for event streaming.
   * 
   * @param {Object} replayEngine - ReplayEngine instance
   * @returns {this} For chaining
   */
  attachReplayEngine(replayEngine) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);
    this.#replayEngine = replayEngine;
    return this;
  }
  
  /**
   * Attach an ExecutionEngine for order execution.
   * 
   * @param {Object} executionEngine - ExecutionEngine instance
   * @returns {this} For chaining
   */
  attachExecutionEngine(executionEngine) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);
    this.#executionEngine = executionEngine;
    return this;
  }
  
  /**
   * Attach a MetricsRegistry for observability.
   * 
   * @param {Object} metrics - MetricsRegistry instance
   * @returns {this} For chaining
   */
  attachMetrics(metrics) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);
    this.#metrics = metrics;
    return this;
  }
  
  /**
   * Attach an OrderingGuard for monotonicity checks.
   * 
   * @param {Object} orderingGuard - OrderingGuard instance
   * @returns {this} For chaining
   */
  attachOrderingGuard(orderingGuard) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);
    this.#orderingGuard = orderingGuard;
    return this;
  }
  
  /**
   * Attach an ErrorContainment policy wrapper.
   * 
   * @param {Object} errorContainment - ErrorContainment instance
   * @returns {this} For chaining
   */
  attachErrorContainment(errorContainment) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);
    this.#errorContainment = errorContainment;
    return this;
  }
  
  /**
   * Attach a CheckpointManager for state persistence.
   * 
   * @param {Object} checkpointManager - CheckpointManager instance
   * @returns {this} For chaining
   */
  attachCheckpointManager(checkpointManager) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);
    this.#checkpointManager = checkpointManager;
    return this;
  }

  /**
   * Attach a RiskManager for risk control.
   *
   * Risk checks are applied:
   * - onEvent: Updates risk state, checks for forced exits (SL/TP)
   * - placeOrder: Validates orders against risk rules
   *
   * @param {Object} riskManager - RiskManager instance
   * @returns {this} For chaining
   */
  attachRiskManager(riskManager) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);

    if (!riskManager || typeof riskManager.onEvent !== 'function') {
      throw new Error('RUNTIME_ERROR: Invalid RiskManager - must implement onEvent()');
    }

    this.#riskManager = riskManager;
    return this;
  }

  /**
   * Attach an MLDecisionAdapter for ML advisory signals.
   *
   * ML adapter is called during event processing:
   * - observeEvent(): Updates feature state
   * - computeShadow(): Triggers async ML inference
   *
   * Strategy can access results via context.getMlAdvice().
   *
   * @param {Object} mlAdapter - MLDecisionAdapter instance
   * @returns {this} For chaining
   */
  attachMlAdapter(mlAdapter) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.CREATED);

    if (mlAdapter && typeof mlAdapter.getLastResult !== 'function') {
      throw new Error('RUNTIME_ERROR: Invalid MLAdapter - must implement getLastResult()');
    }

    this.#mlAdapter = mlAdapter;
    return this;
  }

  // ============================================================================
  // LIFECYCLE METHODS
  // ============================================================================
  
  /**
   * Initialize the runtime.
   * Validates configuration and prepares components.
   * 
   * @returns {Promise<void>}
   */
  async init() {
    this.#lifecycle.initialize();
    
    try {
      // Create context with all attached components
      this.#context = createRuntimeContext({
        runId: this.#runId,
        dataset: this.#config.dataset,
        config: this.#config,
        metrics: this.#metrics,
        placeOrder: this.#executionEngine
          ? (intent) => this.#placeOrder(intent)
          : null,
        getExecutionState: this.#executionEngine
          ? () => this.#executionEngine.snapshot()
          : null,
        mlAdapter: this.#mlAdapter
      });
      
      // Call strategy.onInit if available (v2 interface)
      if (this.#strategy.onInit) {
        await this.#strategy.onInit(this.#context);
      } else if (this.#strategy.onStart) {
        // Legacy v1 interface
        await this.#strategy.onStart(this.#context);
      }
      
      this.#lifecycle.ready();
      this.emit('ready', { runId: this.#runId });
      
    } catch (error) {
      this.#lifecycle.fail(error);
      throw error;
    }
  }
  
  /**
   * Start the replay execution.
   * Requires a ReplayEngine to be attached.
   * 
   * @param {Object} [options] - Start options
   * @param {string} [options.startCursor] - Cursor to resume from
   * @returns {Promise<RunManifest>} Run manifest
   */
  async start(options = {}) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.READY);
    
    if (!this.#replayEngine) {
      throw new Error('RUNTIME_ERROR: ReplayEngine not attached');
    }

    return this.processReplay(this.#replayEngine, {
      clock: this.#config.clock,
      cursor: options.startCursor
    });
  }
  
  /**
   * Process events from an async iterator (for SSE integration).
   * Does not manage ReplayEngine internally.
   * 
   * @param {AsyncIterable} eventStream - Event stream
   * @returns {Promise<RunManifest>} Run manifest
   */
  async processStream(eventStream) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.READY);
    this.#lifecycle.start();
    this.emit('start', { runId: this.#runId });
    
    try {
      let eventIndex = 0;
      
      for await (const event of eventStream) {
        if (this.#lifecycle.isPaused) {
          await this.#waitForResume();
        }
        
        if (this.#lifecycle.isTerminal) {
          break;
        }
        
        await this.#processEvent(event, eventIndex);
        eventIndex++;
        
        if (this.#config.enableCheckpoints && 
            eventIndex % this.#config.checkpointInterval === 0) {
          await this.#saveCheckpoint(eventIndex);
        }
      }
      
      this.#lifecycle.finalize();
      await this.#finalize();
      this.#lifecycle.complete();
      
      const manifest = this.getManifest();
      this.emit('complete', manifest);
      
      return manifest;
      
    } catch (error) {
      this.#endedReason = 'error';
      this.#lifecycle.fail(error);
      this.emit('error', error);
      throw error;
    }
  }

  /**
   * Process events directly from ReplayEngine with deterministic replay lifecycle.
   * 
   * @param {Object} replayEngine - ReplayEngine instance
   * @param {Object} [replayOptions] - Replay options
   * @returns {Promise<RunManifest>} Run manifest
   */
  async processReplay(replayEngine, replayOptions = {}) {
    this.#lifecycle.assertStatus(RunLifecycleStatus.READY);
    this.#lifecycle.start();
    this.emit('start', { runId: this.#runId });

    this.#replayStartedAt = Date.now();
    this.#replayStopReason = null;
    this.#replayEmittedEventCount = 0;
    this.#replayFirstEventTs = null;
    this.#replayLastEventTs = null;

    let stats = null;
    try {
      const meta = await replayEngine.getMeta();
      this.#replayManifestId = meta.manifest_id;
      const seed = this.#config.seed ?? '';
      const replayHash = computeHash({ seed, manifest_id: meta.manifest_id });
      this.#replayRunId = `replay_${replayHash.substring(0, 16)}`;

      const iterator = replayEngine.replay({
        batchSize: this.#config.batchSize,
        ...replayOptions
      });

      let eventIndex = 0;
      while (true) {
        const { value, done } = await iterator.next();
        if (done) {
          stats = value;
          break;
        }

        if (this.#lifecycle.isPaused) {
          await this.#waitForResume();
        }

        if (this.#lifecycle.isTerminal) {
          break;
        }

        if (this.#executionEngine) {
          this.#executionEngine.onEvent(value);
        }

        await this.#processEvent(value, eventIndex);
        eventIndex++;

        if (this.#config.enableCheckpoints && 
            eventIndex % this.#config.checkpointInterval === 0) {
          await this.#saveCheckpoint(eventIndex);
        }
      }

      this.#replayEmittedEventCount = stats ? stats.rowsEmitted : eventIndex;
      this.#replayStopReason = stats ? stats.stop_reason : 'ERROR';

      this.#lifecycle.finalize();
      await this.#finalize();
      const manifest = this.getManifest();
      await this.#archiveReplayRun(manifest, stats);
      this.#lifecycle.complete();

      this.emit('complete', manifest);
      return manifest;
    } catch (error) {
      this.#endedReason = 'error';
      if (error && error.replay_stats && !this.#replayStopReason) {
        this.#replayStopReason = error.replay_stats.stop_reason;
      }
      this.#lifecycle.fail(error);
      this.emit('error', error);
      throw error;
    } finally {
      this.#replayFinishedAt = Date.now();
    }
  }
  
  // ============================================================================
  // CONTROL METHODS
  // ============================================================================
  
  /**
   * Pause execution.
   */
  pause() {
    this.#lifecycle.pause();
    this.emit('pause', { runId: this.#runId });
  }
  
  /**
   * Resume execution after pause.
   */
  resume() {
    this.#lifecycle.resume();
    this.emit('resume', { runId: this.#runId });
  }
  
  /**
   * Kill execution immediately.
   * 
   * @param {string} [reason='kill'] - Reason for kill
   */
  async kill(reason = 'kill') {
    this.#endedReason = reason;
    
    if (this.#lifecycle.status === RunLifecycleStatus.RUNNING ||
        this.#lifecycle.status === RunLifecycleStatus.PAUSED) {
      this.#lifecycle.finalize();
      await this.#finalize();
      this.#lifecycle.complete();
    } else {
      this.#lifecycle.cancel();
    }
    
    this.emit('kill', { runId: this.#runId, reason });
  }
  
  // ============================================================================
  // INTERNAL METHODS
  // ============================================================================
  
  /**
   * Process a single event.
   * 
   * @param {Object} event - Event to process
   * @param {number} eventIndex - Event index
   */
  async #processEvent(event, eventIndex) {
    // Ordering check
    if (this.#orderingGuard) {
      this.#orderingGuard.check(this.#lastEvent, event);
    }
    
    // Update cursor
    const cursor = {
      ts_event: event.ts_event,
      seq: event.seq,
      encoded: event.cursor
    };
    this.#context.updateCursor(cursor);
    this.#state.updateCursor(cursor);

    // Risk state update & forced exits (before strategy processing)
    if (this.#riskManager) {
      this.#riskManager.onEvent(event, this.#context);

      const forceExit = this.#riskManager.checkForExit(event, this.#context);
      if (forceExit) {
        this.#context.logger.info(
          `[RISK] Forced exit: ${forceExit.symbol} ${forceExit.side} qty=${forceExit.qty} reason=${forceExit.reason}`
        );
        try {
          this.#placeOrder({
            symbol: forceExit.symbol,
            side: forceExit.side,
            qty: forceExit.qty,
            _riskForced: true,
            _riskReason: forceExit.reason
          });
          this.emit('riskExit', {
            eventIndex,
            order: forceExit,
            cursor: cursor.encoded
          });
          if (this.#metrics) {
            this.#metrics.increment('risk_forced_exits_total');
          }
        } catch (err) {
          this.#context.logger.error(`[RISK] Force exit failed: ${err.message}`);
        }
      }
    }

    // ML advisory update (before strategy processing)
    if (this.#mlAdapter) {
      this.#mlAdapter.observeEvent(event);
      this.#mlAdapter.computeShadow(event);
    }

    // Process event through strategy with error containment
    if (this.#errorContainment) {
      const result = await this.#errorContainment.wrap(async () => {
        await this.#strategy.onEvent(event, this.#context);
      });
      
      if (result.error) {
        this.emit('eventError', { event, error: result.error, eventIndex });
      }
    } else {
      // No error containment - fail fast
      await this.#strategy.onEvent(event, this.#context);
    }
    
    // Update state
    this.#state.incrementEventCount();
    this.#context.incrementProcessed();
    if (this.#replayRunId) {
      if (this.#replayFirstEventTs === null) this.#replayFirstEventTs = BigInt(event.ts_event);
      this.#replayLastEventTs = BigInt(event.ts_event);
    }
    
    if (this.#executionEngine) {
      this.#state.updateExecutionState(this.#executionEngine.snapshot());
    }
    
    if (this.#strategy.getState) {
      this.#state.updateStrategyState(this.#strategy.getState());
    }
    
    if (this.#metrics) {
      this.#metrics.increment('events_total');
      this.#state.updateMetrics(this.#metrics.snapshot());
    }
    
    // Store last event for ordering check
    this.#lastEvent = event;
    
    // Emit progress periodically
    if (eventIndex > 0 && eventIndex % 10000 === 0) {
      this.emit('progress', {
        eventIndex,
        cursor: cursor.encoded,
        stateHash: this.#state.computeStateHash().substring(0, 8)
      });
    }

    if (this.#eventObserver) {
      try {
        this.#eventObserver({
          event,
          eventIndex,
          decisionCount: this.#decisions.length,
          stateSnapshot: this.#state.snapshot()
        });
      } catch (err) {
        console.warn('[StrategyRuntime] eventObserver error:', err.message || String(err));
      }
    }
  }
  
  /**
   * Place an order through the execution engine.
   * 
   * @param {Object} intent - Order intent
   * @returns {Object} Fill result
   */
  #placeOrder(intent) {
    if (!this.#executionEngine) {
      throw new Error('RUNTIME_ERROR: No execution engine attached');
    }

    // Risk validation (skip for forced exits marked with _riskForced)
    if (this.#riskManager && !intent._riskForced) {
      const { allowed, reason } = this.#riskManager.allow(intent, this.#context);
      if (!allowed) {
        this.#context.logger.warn(
          `[RISK] Order rejected: ${intent.symbol} ${intent.side} qty=${intent.qty} reason=${reason}`
        );
        this.emit('riskReject', {
          order: intent,
          reason,
          cursor: this.#context.cursor.encoded
        });
        if (this.#metrics) {
          this.#metrics.increment('risk_rejections_total');
        }
        return {
          fill_id: null,
          status: 'REJECTED',
          reason: reason,
          symbol: intent.symbol,
          side: intent.side,
          qty: intent.qty
        };
      }
    }

    const decisionPayload = {
      replay_run_id: this.#replayRunId,
      cursor: this.#context.cursor.encoded,
      ts_event: this.#context.cursor.ts_event,
      decision: { ...intent }
    };
    this.#decisions.push(decisionPayload);

    const decisionHash = crypto.createHash('sha256').update(canonicalStringify(decisionPayload)).digest('hex');
    emitAudit({
      actor: 'system',
      action: 'DECISION',
      target_type: 'decision',
      target_id: decisionHash,
      reason: null,
      metadata: {
        live_run_id: this.#replayRunId,
        strategy_id: this.#config.strategy?.id || null,
        decision_id: decisionHash,
        decision_hash: decisionHash
      }
    });
    
    const fill = this.#executionEngine.onOrder({
      ...intent,
      ts_event: this.#context.cursor.ts_event
    });
    
    // Track fill
    this.#state.addFill(fill);
    
    if (this.#metrics) {
      this.#metrics.increment('fills_total');
    }
    
    this.emit('fill', fill);
    
    return fill;
  }
  
  /**
   * Finalize the run.
   */
  async #finalize() {
    // Call strategy.onFinalize if available (v2 interface)
    if (this.#strategy.onFinalize) {
      await this.#strategy.onFinalize(this.#context);
    } else if (this.#strategy.onEnd) {
      // Legacy v1 interface
      await this.#strategy.onEnd(this.#context);
    }
    
    // Update final state
    if (this.#executionEngine) {
      this.#state.updateExecutionState(this.#executionEngine.snapshot());
    }
    
    if (this.#strategy.getState) {
      this.#state.updateStrategyState(this.#strategy.getState());
    }
    
    if (this.#metrics) {
      this.#state.updateMetrics(this.#metrics.snapshot());
    }
    
    // Save final checkpoint if enabled
    if (this.#checkpointManager) {
      await this.#saveCheckpoint(this.#state.eventCount);
    }
  }
  
  /**
   * Save a checkpoint.
   * 
   * @param {number} eventIndex - Event index
   */
  async #saveCheckpoint(eventIndex) {
    if (!this.#checkpointManager) return;
    
    const snapshot = this.#state.snapshot();
    const checkpointId = `checkpoint_${eventIndex}`;
    
    await this.#checkpointManager.save(snapshot, checkpointId, eventIndex);
    
    this.emit('checkpoint', { checkpointId, eventIndex });
  }
  
  /**
   * Wait for resume after pause.
   * 
   * @returns {Promise<void>}
   */
  async #waitForResume() {
    return new Promise((resolve) => {
      const checkResume = () => {
        if (!this.#lifecycle.isPaused) {
          resolve();
        } else {
          setTimeout(checkResume, 100);
        }
      };
      checkResume();
    });
  }
  
  // ============================================================================
  // OUTPUT METHODS
  // ============================================================================
  
  /**
   * Get run manifest.
   * 
   * @returns {RunManifest} Run manifest
   */
  getManifest() {
    const snapshot = this.#state.snapshot();
    
    return {
      run_id: this.#runId,
      started_at: this.#lifecycle.startedAt 
        ? new Date(this.#lifecycle.startedAt).toISOString() 
        : null,
      ended_at: this.#lifecycle.endedAt 
        ? new Date(this.#lifecycle.endedAt).toISOString() 
        : null,
      ended_reason: this.#endedReason,
      duration_ms: this.#lifecycle.durationMs,
      input: {
        dataset: this.#config.toObject().dataset,
        config_hash: this.#config.configHash.substring(0, 8)
      },
      output: {
        event_count: snapshot.eventCount,
        fills_count: snapshot.fillsCount,
        decision_count: this.#decisions.length,
        decision_hash: this.decisionHash,
        state_hash: snapshot.stateHash,
        fills_hash: snapshot.fillsHash,
        last_cursor: snapshot.cursor
      },
      replay: {
        replay_run_id: this.#replayRunId,
        started_at: this.#replayFirstEventTs !== null
          ? new Date(Number(this.#replayFirstEventTs / 1_000_000n)).toISOString()
          : null,
        finished_at: this.#replayLastEventTs !== null
          ? new Date(Number(this.#replayLastEventTs / 1_000_000n)).toISOString()
          : null,
        stop_reason: this.#replayStopReason,
        emitted_event_count: this.#replayEmittedEventCount,
        decision_count: this.#decisions.length
      },
      risk: this.#riskManager ? {
        enabled: true,
        ...this.#riskManager.getStats()
      } : { enabled: false },
      ml: this.#mlAdapter ? {
        enabled: true,
        lastResult: this.#mlAdapter.getLastResult()
      } : { enabled: false }
    };
  }

  async #archiveReplayRun(manifest, stats) {
    if (!this.#replayRunId) return;

    const writer = RunArchiveWriter.fromEnv();
    const run = {
      replay_run_id: this.#replayRunId,
      seed: this.#config.seed ?? '',
      manifest_id: this.#replayManifestId,
      parquet_path: this.#config.dataset.parquet,
      first_ts_event: this.#replayFirstEventTs,
      last_ts_event: this.#replayLastEventTs,
      stop_reason: this.#replayStopReason,
      decisions: this.#decisions,
      stats: {
        emitted_event_count: stats ? stats.rowsEmitted : this.#replayEmittedEventCount,
        decision_count: this.#decisions.length,
        duration_ms: (this.#replayFirstEventTs !== null && this.#replayLastEventTs !== null)
          ? Number((this.#replayLastEventTs - this.#replayFirstEventTs) / 1_000_000n)
          : 0
      }
    };

    try {
      await writer.write(run);
    } catch (err) {
      this.#replayStopReason = 'ERROR';
      console.error(JSON.stringify({
        event: 'run_archive_error',
        replay_run_id: this.#replayRunId,
        error: err.message
      }));
      throw err;
    }
  }
  
  /**
   * Get state snapshot.
   * 
   * @returns {Object} State snapshot
   */
  getSnapshot() {
    return this.#state.snapshot();
  }
}

/**
 * Create a StrategyRuntime.
 * 
 * @param {Object} options - Runtime options
 * @returns {StrategyRuntime} Runtime instance
 */
export function createStrategyRuntime(options) {
  return new StrategyRuntime(options);
}

export default StrategyRuntime;
