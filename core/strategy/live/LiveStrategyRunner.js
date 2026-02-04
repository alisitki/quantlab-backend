/**
 * QuantLab Live Strategy Runner
 * Replay-parity event processing for live streams.
 */

import crypto from 'node:crypto';
import { StrategyRuntime } from '../runtime/StrategyRuntime.js';
import { OrderingGuard } from '../safety/OrderingGuard.js';
import { ErrorContainment } from '../safety/ErrorContainment.js';
import { MetricsRegistry } from '../metrics/MetricsRegistry.js';
import { ErrorPolicy, OrderingMode } from '../interface/types.js';
import { StrategyLoader } from '../interface/StrategyLoader.js';
import { RunArchiveWriter } from '../../run-archive/RunArchiveWriter.js';
import { LiveWSConsumer } from '../../live-ws/LiveWSConsumer.js';
import { LiveEventSequencer } from '../../live-ws/LiveEventSequencer.js';
import { PromotionGuardManager } from '../guards/PromotionGuardManager.js';
import { RunBudgetManager } from '../limits/RunBudgetManager.js';
import { observerRegistry, RUN_STATUS } from '../../observer/ObserverRegistry.js';
import { emitAudit } from '../../audit/AuditWriter.js';
import { RiskManager } from '../../risk/RiskManager.js';
import { getKillSwitchManager } from '../../futures/KillSwitchManager.js';

export class LiveStrategyRunner {
  /** @type {StrategyRuntime} */
  #runtime;
  /** @type {string} */
  #liveRunId;
  /** @type {boolean} */
  #stopRequested = false;
  /** @type {number} */
  #startedAt;
  /** @type {number} */
  #finishedAt;
  /** @type {string} */
  #stopReason = 'STREAM_END';
  /** @type {number} */
  #emittedEventCount = 0;
  /** @type {bigint|null} */
  #firstTsEvent = null;
  /** @type {bigint|null} */
  #lastTsEvent = null;
  /** @type {number} */
  #maxLagMs;
  /** @type {number} */
  #lagWarnIntervalMs = 5000;
  /** @type {number} */
  #lastLagWarnAt = 0;
  /** @type {function|null} */
  #sigintHandler = null;
  /** @type {Object} */
  #archiveInfo;
  /** @type {LiveWSConsumer|null} */
  #consumer = null;
  /** @type {LiveEventSequencer} */
  #sequencer = new LiveEventSequencer();
  /** @type {Object|null} */
  #archiveWriterOverride = null;
  /** @type {Object} */
  #config;
  /** @type {Object|null} */
  #executionEngine = null;
  /** @type {PromotionGuardManager} */
  #guardManager;
  /** @type {string|null} */
  #guardName = null;
  /** @type {string|null} */
  #guardReason = null;
  /** @type {RunBudgetManager} */
  #budgetManager;
  /** @type {string|null} */
  #budgetName = null;
  /** @type {number|null} */
  #budgetValue = null;
  /** @type {import('../../futures/KillSwitchManager.js').KillSwitchManager} */
  #killSwitchManager;
  /** @type {string|null} */
  #killSwitchReason = null;

  /**
   * @param {Object} options
   * @param {Object} options.dataset
   * @param {string} options.exchange
   * @param {string[]} [options.symbols]
   * @param {string[]} [options.streams]
   * @param {string} [options.strategyPath]
   * @param {Object} [options.strategy]
   * @param {Object} [options.strategyConfig]
   * @param {string} [options.seed]
   * @param {string} [options.errorPolicy]
   * @param {string} [options.orderingMode]
   * @param {boolean} [options.enableMetrics]
   * @param {Object|null} [options.executionEngine]
   * @param {number} [options.maxLagMs]
   * @param {Object} [options.archiveInfo]
   * @param {string} [options.archiveInfo.manifest_id]
   * @param {string} [options.archiveInfo.parquet_path]
   * @param {Object|null} [options.archiveWriter]
   */
  constructor(options) {
    const {
      dataset,
      exchange,
      symbols,
      streams,
      strategyPath,
      strategy,
      strategyConfig,
      seed = '',
      errorPolicy = ErrorPolicy.FAIL_FAST,
      orderingMode = OrderingMode.STRICT,
      enableMetrics = true,
      executionEngine = null,
      maxLagMs = 5000,
      archiveInfo = {},
      archiveWriter = null,
      guardConfig = null,
      budgetConfig = null,
      riskConfig = null,
      executionConfig = null
    } = options;

    this.#liveRunId = crypto.randomUUID();
    this.#maxLagMs = maxLagMs;
    this.#archiveInfo = archiveInfo;
    this.#archiveWriterOverride = archiveWriter;
    if (streams && streams.length > 0) {
      console.warn(JSON.stringify({
        event: 'live_streams_ignored',
        exchange,
        streams
      }));
    }
    this.#consumer = new LiveWSConsumer({ exchange, symbols });
    this.#guardManager = PromotionGuardManager.fromEnv(guardConfig || {});
    this.#budgetManager = RunBudgetManager.fromEnv(budgetConfig || {});
    this.#killSwitchManager = getKillSwitchManager();

    this.#config = {
      dataset,
      strategy,
      strategyConfig,
      seed,
      errorPolicy,
      orderingMode,
      enableMetrics,
      riskConfig,
      executionConfig
    };
    this.#executionEngine = executionEngine;

    if (strategy) {
      this.#runtime = this.#createRuntime(strategy);
    }
  }

  #createRuntime(strategy) {
    const runtime = new StrategyRuntime({
      dataset: this.#config.dataset,
      strategy,
      strategyConfig: this.#config.strategyConfig,
      seed: this.#config.seed,
      errorPolicy: this.#config.errorPolicy,
      orderingMode: this.#config.orderingMode,
      enableMetrics: this.#config.enableMetrics,
      enableCheckpoints: false
    });

    const orderingGuard = new OrderingGuard({ mode: this.#config.orderingMode });
    runtime.attachOrderingGuard(orderingGuard);

    const errorContainment = new ErrorContainment({
      policy: this.#config.errorPolicy,
      maxErrors: 100
    });
    runtime.attachErrorContainment(errorContainment);

    if (this.#config.enableMetrics) {
      const metrics = new MetricsRegistry({ runId: runtime.runId });
      runtime.attachMetrics(metrics);
    }

    if (this.#executionEngine) {
      runtime.attachExecutionEngine(this.#executionEngine);
    }

    // Risk management (Phase 2 Safety Guards)
    if (this.#config.riskConfig?.enabled !== false) {
      const initialCapital = this.#config.executionConfig?.initialCapital || 10000;
      const riskManager = new RiskManager(this.#config.riskConfig || {}, initialCapital);
      runtime.attachRiskManager(riskManager);
    }

    return runtime;
  }

  get liveRunId() { return this.#liveRunId; }
  get decisionCount() { return this.#runtime.decisions.length; }
  get decisionHash() { return this.#runtime.decisionHash; }

  stop(reason = 'MANUAL_STOP') {
    this.#stopRequested = true;
    // Preserve existing stop reason if already set (e.g., KILL_SWITCH)
    if (this.#stopReason === 'STREAM_END') {
      this.#stopReason = reason;
    }
    if (this.#consumer) this.#consumer.stop();
    observerRegistry.updateRun(this.#liveRunId, {
      status: RUN_STATUS.STOPPED,
      stop_reason: this.#stopReason
    });
  }

  /**
   * Stop due to kill switch activation
   * @param {string} reason - Kill switch reason
   */
  #stopForKillSwitch(reason) {
    this.#stopReason = 'KILL_SWITCH';
    this.#killSwitchReason = reason;
    this.stop('KILL_SWITCH');
  }

  #installSigintHandler() {
    if (this.#sigintHandler) return;
    this.#sigintHandler = () => this.stop();
    process.on('SIGINT', this.#sigintHandler);
  }

  #removeSigintHandler() {
    if (!this.#sigintHandler) return;
    process.off('SIGINT', this.#sigintHandler);
    this.#sigintHandler = null;
  }

  async run({ handleSignals = true, eventStream = null, strategyPath = null } = {}) {
    this.#startedAt = Date.now();
    if (handleSignals) this.#installSigintHandler();

    // Check kill switch before starting
    if (this.#killSwitchManager.isGlobalActive()) {
      const status = this.#killSwitchManager.getStatus();
      throw new Error(`KILL_SWITCH_ACTIVE: ${status.reason}`);
    }

    // Register with kill switch manager for emergency stops
    this.#killSwitchManager.registerRun(this.#liveRunId, () => {
      this.#stopForKillSwitch(this.#killSwitchManager.getStatus().reason);
    });

    try {
      if (!this.#runtime || strategyPath) {
        const path = strategyPath;
        if (!path) {
          throw new Error('STRATEGY_PATH_REQUIRED');
        }
        const loaded = await StrategyLoader.loadFromFile(strategyPath, {
          config: this.#config.strategyConfig ?? {},
          autoAdapt: true
        });
        this.#runtime = this.#createRuntime(loaded);
      }

      await this.#runtime.init();
      this.#runtime.setEventObserver((payload) => {
        const result = this.#guardManager.evaluateEvent({
          stateSnapshot: payload.stateSnapshot,
          live_run_id: this.#liveRunId,
          strategy_id: this.#runtime.config.strategy?.id || null
        });
        if (!result.ok && this.#stopReason !== 'ERROR') {
          this.#stopReason = 'PROMOTION_GUARD_FAIL';
          this.#guardName = result.guard;
          this.#guardReason = result.reason;
          this.stop();
        }

        this.#budgetManager.recordDecision();
      });
      this.#runtime.setReplayRunId(this.#liveRunId);
      observerRegistry.addRun({
        live_run_id: this.#liveRunId,
        strategy_id: this.#runtime.config.strategy?.id || 'unknown',
        started_at: new Date(this.#startedAt).toISOString(),
        stopFn: () => this.stop()
      });
      emitAudit({
        actor: 'system',
        action: 'RUN_START',
        target_type: 'run',
        target_id: this.#liveRunId,
        reason: null,
        metadata: {
          live_run_id: this.#liveRunId,
          strategy_id: this.#runtime.config.strategy?.id || null
        }
      });

      let source = eventStream;
      if (!source) {
        this.#consumer.start();
        source = this.#consumer.events();
      }
      const wrapped = this.#wrapStream(this.#sequencer.sequence(source));
      await this.#runtime.processStream(wrapped);
      if (this.#stopReason !== 'ERROR' && this.#stopReason !== 'PROMOTION_GUARD_FAIL') {
        const final = this.#guardManager.evaluateFinal({
          decisionCount: this.#runtime.decisions.length,
          decisionHash: this.#runtime.decisionHash,
          live_run_id: this.#liveRunId,
          strategy_id: this.#runtime.config.strategy?.id || null
        });
        if (!final.ok) {
          this.#stopReason = 'PROMOTION_GUARD_FAIL';
          this.#guardName = final.guard;
          this.#guardReason = final.reason;
        }
      }
      if (this.#stopReason !== 'ERROR' && this.#stopReason !== 'PROMOTION_GUARD_FAIL') {
        const budgetFinal = this.#budgetManager.evaluate({
          eventIndex: this.#emittedEventCount - 1,
          live_run_id: this.#liveRunId,
          strategy_id: this.#runtime.config.strategy?.id || null
        });
        if (!budgetFinal.ok) {
          this.#stopReason = 'BUDGET_EXCEEDED';
          this.#budgetName = budgetFinal.budget;
          this.#budgetValue = budgetFinal.value;
        }
      }
      if (this.#stopReason !== 'MANUAL_STOP' && this.#stopReason !== 'PROMOTION_GUARD_FAIL') {
        this.#stopReason = 'STREAM_END';
      }
      observerRegistry.updateRun(this.#liveRunId, {
        status: RUN_STATUS.STOPPED,
        stop_reason: this.#stopReason
      });
      emitAudit({
        actor: 'system',
        action: 'RUN_STOP',
        target_type: 'run',
        target_id: this.#liveRunId,
        reason: this.#stopReason,
        metadata: {
          live_run_id: this.#liveRunId,
          strategy_id: this.#runtime.config.strategy?.id || null,
          stop_reason: this.#stopReason,
          guard_name: this.#guardName,
          budget_name: this.#budgetName,
          kill_switch_reason: this.#killSwitchReason
        }
      });
    } catch (err) {
      this.#stopReason = 'ERROR';
      observerRegistry.updateRun(this.#liveRunId, {
        status: RUN_STATUS.ERROR,
        stop_reason: this.#stopReason
      });
      const strategyId = this.#runtime?.config?.strategy?.id || null;
      emitAudit({
        actor: 'system',
        action: 'RUN_STOP',
        target_type: 'run',
        target_id: this.#liveRunId,
        reason: this.#stopReason,
        metadata: {
          live_run_id: this.#liveRunId,
          strategy_id: strategyId,
          stop_reason: this.#stopReason
        }
      });
      throw err;
    } finally {
      this.#finishedAt = Date.now();
      this.#removeSigintHandler();
      this.#killSwitchManager.unregisterRun(this.#liveRunId);
      await this.#archive();
    }

    return this.#result();
  }

  async *#wrapStream(eventStream) {
    for await (const event of eventStream) {
      if (this.#stopRequested) break;

      // Check kill switch on every event
      if (this.#killSwitchManager.isGlobalActive()) {
        this.#stopForKillSwitch(this.#killSwitchManager.getStatus().reason);
        break;
      }

      // Check symbol-specific kill switch if event has symbol
      if (event?.symbol && this.#killSwitchManager.isSymbolKilled(event.symbol)) {
        this.#stopForKillSwitch(`Symbol ${event.symbol} killed`);
        break;
      }

      if (this.#executionEngine && typeof this.#executionEngine.onEvent === 'function') {
        this.#executionEngine.onEvent(event);
      }
      this.#updateLag(event);
      this.#emittedEventCount++;
      observerRegistry.updateRun(this.#liveRunId, {
        last_event_ts: Date.now(),
        budget_pressure: this.#budgetManager.getPressure()
      });
      const budget = this.#budgetManager.evaluate({
        eventIndex: this.#emittedEventCount - 1,
        live_run_id: this.#liveRunId,
        strategy_id: this.#runtime.config.strategy?.id || null
      });
      if (!budget.ok && this.#stopReason !== 'ERROR') {
        this.#stopReason = 'BUDGET_EXCEEDED';
        this.#budgetName = budget.budget;
        this.#budgetValue = budget.value;
        this.stop();
        break;
      }
      if (event && event.ts_event !== undefined) {
        try {
          const ts = BigInt(event.ts_event);
          if (this.#firstTsEvent === null) this.#firstTsEvent = ts;
          this.#lastTsEvent = ts;
        } catch {
          // Ignore ts_event conversion errors for lag tracking
        }
      }

      yield event;
    }
  }

  #updateLag(event) {
    try {
      if (!event || event.ts_event === undefined) return;
      const ts = BigInt(event.ts_event);
      const nowNs = BigInt(Date.now()) * 1_000_000n;
      const lagMs = Number((nowNs - ts) / 1_000_000n);
      if (lagMs > this.#maxLagMs) {
        const now = Date.now();
        if (now - this.#lastLagWarnAt >= this.#lagWarnIntervalMs) {
          this.#lastLagWarnAt = now;
          console.warn(JSON.stringify({
            event: 'live_lag_warning',
            live_run_id: this.#liveRunId,
            lag_ms: lagMs
          }));
        }
      }
    } catch {
      // Ignore lag computation errors
    }
  }

  async #archive() {
    try {
      if (!this.#runtime) {
        console.error(JSON.stringify({
          event: 'live_archive_skip',
          live_run_id: this.#liveRunId,
          reason: 'runtime_missing'
        }));
        return;
      }
      const writer = this.#archiveWriterOverride || RunArchiveWriter.fromEnv();
      const run = {
        replay_run_id: this.#liveRunId,
        seed: this.#runtime.config.seed ?? '',
        manifest_id: this.#archiveInfo.manifest_id ?? null,
        parquet_path: this.#archiveInfo.parquet_path ?? null,
        first_ts_event: this.#firstTsEvent,
        last_ts_event: this.#lastTsEvent,
        stop_reason: this.#stopReason,
        decisions: this.#runtime.decisions,
        stats: {
          emitted_event_count: this.#emittedEventCount,
          decision_count: this.#runtime.decisions.length,
          duration_ms: this.#startedAt && this.#finishedAt ? this.#finishedAt - this.#startedAt : 0,
          guard_name: this.#guardName,
          guard_reason: this.#guardReason,
          guard_evaluated_count: this.#guardManager.stats.evaluated_count,
          guard_failed_count: this.#guardManager.stats.failed_count,
          budget_name: this.#budgetName,
          budget_value: this.#budgetValue,
          budget_evaluated_count: this.#budgetManager.stats.evaluated_count,
          budget_exceeded_count: this.#budgetManager.stats.exceeded_count,
          kill_switch_reason: this.#killSwitchReason
        }
      };
      await writer.write(run);
    } catch (err) {
      console.error(JSON.stringify({
        event: 'live_archive_error',
        live_run_id: this.#liveRunId,
        error: err.message || String(err)
      }));
      this.#stopReason = 'ERROR';
    }
  }

  #result() {
    return {
      live_run_id: this.#liveRunId,
      started_at: new Date(this.#startedAt).toISOString(),
      finished_at: new Date(this.#finishedAt).toISOString(),
      stop_reason: this.#stopReason,
      emitted_event_count: this.#emittedEventCount,
      decision_count: this.#runtime.decisions.length,
      decision_hash: this.#runtime.decisionHash
    };
  }
}

export default LiveStrategyRunner;
