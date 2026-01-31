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
import { computeRunId } from '../safety/DeterminismValidator.js';
import { RunLifecycleStatus } from '../interface/types.js';

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
  #lastEvent;
  
  /** @type {string} */
  #endedReason;
  
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
    this.#lastEvent = null;
    this.#endedReason = 'finished';
    
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
          : null
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
    
    this.#lifecycle.start();
    this.emit('start', { runId: this.#runId });
    
    try {
      // Start replay
      const replayOpts = {
        batchSize: this.#config.batchSize,
        clock: this.#config.clock,
        startCursor: options.startCursor
      };
      
      const replayGenerator = this.#replayEngine.replay(replayOpts);
      
      // Process events
      let eventIndex = 0;
      for await (const event of replayGenerator) {
        // Check lifecycle status (for pause/cancel)
        if (this.#lifecycle.isPaused) {
          await this.#waitForResume();
        }
        
        if (this.#lifecycle.isTerminal) {
          break;
        }
        
        // Process event
        await this.#processEvent(event, eventIndex);
        eventIndex++;
        
        // Checkpoint if enabled
        if (this.#config.enableCheckpoints && 
            eventIndex % this.#config.checkpointInterval === 0) {
          await this.#saveCheckpoint(eventIndex);
        }
      }
      
      // Finalize
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
        state_hash: snapshot.stateHash,
        fills_hash: snapshot.fillsHash,
        last_cursor: snapshot.cursor
      }
    };
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
