/**
 * Strategy Lifecycle Manager
 *
 * Central orchestrator for strategy lifecycle management.
 * Integrates all lifecycle components and provides a unified API.
 */

import { PerformanceTracker } from './PerformanceTracker.js';
import { LifecycleStore } from './LifecycleStore.js';
import { LifecycleStage, canPromote, canDemote } from './LifecycleStage.js';
import * as PromotionEvaluator from './PromotionEvaluator.js';
import * as DemotionEvaluator from './DemotionEvaluator.js';

/**
 * Manages strategy lifecycle transitions and performance tracking
 */
export class StrategyLifecycleManager {
  constructor(storeDir = null, filename = null) {
    this.performanceTracker = new PerformanceTracker();
    this.store = new LifecycleStore(storeDir, filename);
    this.strategies = new Map(); // Map<strategyId, StrategyRecord>
    this.killSwitchManager = null;
    this.observerRegistry = null;
    this.edgeRegistry = null; // For edge health integration
  }

  // ============================================================================
  // REGISTRATION
  // ============================================================================

  /**
   * Register a newly deployed strategy
   * @param {DeployResult} deployResult - From StrategyDeployer
   * @returns {string} strategyId
   */
  register(deployResult) {
    const strategyId = deployResult.strategyId;

    const record = {
      strategyId,
      edgeId: deployResult.edgeId,
      templateType: deployResult.templateType || 'unknown',
      currentStage: LifecycleStage.CANDIDATE,
      stageHistory: [
        {
          stage: LifecycleStage.CANDIDATE,
          enteredAt: new Date().toISOString(),
          reason: 'Initial deployment'
        }
      ],
      backtestSummary: deployResult.backtestSummary || {},
      validationScore: deployResult.validationScore || 0,
      deployedAt: new Date().toISOString(),
      promotionGuards: deployResult.promotionGuards || {},
      pendingApproval: false
    };

    this.strategies.set(strategyId, record);
    return strategyId;
  }

  // ============================================================================
  // RUN COMPLETION
  // ============================================================================

  /**
   * Record a completed run and trigger evaluation
   * @param {string} strategyId
   * @param {RunResult} runResult
   * @returns {EvaluationResult|null}
   */
  recordRunResult(strategyId, runResult) {
    if (!this.strategies.has(strategyId)) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    // Record run
    this.performanceTracker.recordRun(strategyId, runResult);

    // Auto-evaluate (optional: can be disabled if too aggressive)
    return this.evaluateStrategy(strategyId);
  }

  // ============================================================================
  // EVALUATION
  // ============================================================================

  /**
   * Evaluate a strategy for promotion/demotion
   * @param {string} strategyId
   * @returns {EvaluationResult}
   */
  evaluateStrategy(strategyId) {
    const record = this.strategies.get(strategyId);
    if (!record) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    const metrics = this.performanceTracker.getRollingMetrics(strategyId);
    if (!metrics) {
      return { strategyId, promotion: null, demotion: null };
    }

    // Check promotion
    const promotionResult = PromotionEvaluator.evaluate(record, metrics);

    // Check demotion (with edge health if available)
    let edgeHealth = null;
    if (this.edgeRegistry && record.edgeId) {
      const edge = this.edgeRegistry.get(record.edgeId);
      if (edge) {
        edgeHealth = edge.getHealthScore();
      }
    }
    const demotionResult = DemotionEvaluator.evaluate(record, metrics, edgeHealth);

    return {
      strategyId,
      promotion: promotionResult,
      demotion: demotionResult
    };
  }

  /**
   * Evaluate all active strategies
   * @returns {EvaluationResult[]}
   */
  evaluateAll() {
    const results = [];
    for (const strategyId of this.strategies.keys()) {
      const record = this.strategies.get(strategyId);
      // Skip retired strategies
      if (record.currentStage !== LifecycleStage.RETIRED) {
        results.push(this.evaluateStrategy(strategyId));
      }
    }
    return results;
  }

  // ============================================================================
  // STAGE TRANSITIONS
  // ============================================================================

  /**
   * Promote strategy to next stage
   * @param {string} strategyId
   * @param {Object} metadata - { actor, reason }
   * @returns {boolean} success
   */
  promote(strategyId, metadata = {}) {
    const record = this.strategies.get(strategyId);
    if (!record) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    const evaluation = this.evaluateStrategy(strategyId);
    if (!evaluation.promotion || !evaluation.promotion.shouldPromote) {
      return false; // Doesn't meet promotion criteria
    }

    const targetStage = evaluation.promotion.targetStage;

    // Check if approval required
    if (evaluation.promotion.requiresApproval && !record.pendingApproval) {
      // Set pending approval flag
      record.pendingApproval = true;
      return false;
    }

    // Perform transition
    this.#transitionStage(record, targetStage, {
      reason: metadata.reason || 'Promotion criteria met',
      actor: metadata.actor || 'system'
    });

    // Clear pending approval
    record.pendingApproval = false;

    return true;
  }

  /**
   * Demote strategy to target stage
   * @param {string} strategyId
   * @param {string} targetStage
   * @param {Object} metadata - { actor, reason }
   * @returns {boolean} success
   */
  demote(strategyId, targetStage, metadata = {}) {
    const record = this.strategies.get(strategyId);
    if (!record) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    if (!canDemote(record.currentStage, targetStage)) {
      throw new Error(`Invalid demotion: ${record.currentStage} → ${targetStage}`);
    }

    this.#transitionStage(record, targetStage, {
      reason: metadata.reason || 'Demoted by operator',
      actor: metadata.actor || 'system'
    });

    return true;
  }

  /**
   * Retire strategy immediately
   * @param {string} strategyId
   * @param {Object} metadata - { actor, reason }
   * @returns {boolean} success
   */
  retire(strategyId, metadata = {}) {
    return this.demote(strategyId, LifecycleStage.RETIRED, {
      reason: metadata.reason || 'Retired',
      actor: metadata.actor || 'system'
    });
  }

  /**
   * Perform stage transition
   * @private
   */
  #transitionStage(record, newStage, metadata) {
    const oldStage = record.currentStage;

    // Close current stage history entry
    const currentEntry = record.stageHistory[record.stageHistory.length - 1];
    if (currentEntry && !currentEntry.exitedAt) {
      currentEntry.exitedAt = new Date().toISOString();
      currentEntry.reason = metadata.reason;
    }

    // Add new stage history entry
    record.stageHistory.push({
      stage: newStage,
      enteredAt: new Date().toISOString(),
      reason: metadata.reason,
      actor: metadata.actor
    });

    // Update current stage
    record.currentStage = newStage;
  }

  // ============================================================================
  // HUMAN APPROVAL
  // ============================================================================

  /**
   * Approve a pending promotion
   * @param {string} strategyId
   * @param {Object} metadata - { actor }
   * @returns {boolean} success
   */
  approvePromotion(strategyId, metadata = {}) {
    const record = this.strategies.get(strategyId);
    if (!record) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    if (!record.pendingApproval) {
      return false; // No pending approval
    }

    // Attempt promotion
    return this.promote(strategyId, metadata);
  }

  /**
   * Reject a pending promotion
   * @param {string} strategyId
   * @param {Object} metadata - { actor, reason }
   * @returns {boolean} success
   */
  rejectPromotion(strategyId, metadata = {}) {
    const record = this.strategies.get(strategyId);
    if (!record) {
      throw new Error(`Strategy not found: ${strategyId}`);
    }

    if (!record.pendingApproval) {
      return false;
    }

    // Clear pending approval
    record.pendingApproval = false;

    // Add audit entry
    record.stageHistory.push({
      stage: record.currentStage,
      enteredAt: new Date().toISOString(),
      reason: `Promotion rejected: ${metadata.reason || 'No reason provided'}`,
      actor: metadata.actor || 'system',
      exitedAt: new Date().toISOString()
    });

    return true;
  }

  // ============================================================================
  // QUERIES
  // ============================================================================

  /**
   * Get strategy record
   * @param {string} strategyId
   * @returns {StrategyRecord|null}
   */
  getStrategy(strategyId) {
    return this.strategies.get(strategyId) || null;
  }

  /**
   * List strategies by stage
   * @param {string} stage
   * @returns {StrategyRecord[]}
   */
  listByStage(stage) {
    const results = [];
    for (const record of this.strategies.values()) {
      if (record.currentStage === stage) {
        results.push(record);
      }
    }
    return results;
  }

  /**
   * List all strategies
   * @returns {StrategyRecord[]}
   */
  listAll() {
    return Array.from(this.strategies.values());
  }

  /**
   * Get system summary
   * @returns {Summary}
   */
  getSummary() {
    const byStage = {};
    let pendingApprovals = 0;

    for (const record of this.strategies.values()) {
      const stage = record.currentStage;
      byStage[stage] = (byStage[stage] || 0) + 1;

      if (record.pendingApproval) {
        pendingApprovals++;
      }
    }

    return {
      totalStrategies: this.strategies.size,
      byStage,
      pendingApprovals,
      lastEvaluated: new Date().toISOString()
    };
  }

  // ============================================================================
  // PERSISTENCE
  // ============================================================================

  /**
   * Persist current state to disk
   * @returns {Promise<void>}
   */
  async persist() {
    const state = {
      version: 1,
      lastUpdated: new Date().toISOString(),
      strategies: Object.fromEntries(this.strategies),
      performanceData: this.performanceTracker.toJSON()
    };

    await this.store.save(state);
  }

  /**
   * Restore state from disk
   * @returns {Promise<void>}
   */
  async restore() {
    const state = await this.store.load();

    // Restore strategies
    this.strategies = new Map(Object.entries(state.strategies || {}));

    // Restore performance tracker
    this.performanceTracker = PerformanceTracker.fromJSON(state.performanceData || {});
  }

  // ============================================================================
  // INTEGRATION
  // ============================================================================

  /**
   * Connect to EdgeRegistry for edge health monitoring
   * @param {EdgeRegistry} edgeRegistry
   */
  connectEdgeRegistry(edgeRegistry) {
    this.edgeRegistry = edgeRegistry;
  }

  /**
   * Connect to kill switch manager
   * @param {KillSwitchManager} killSwitchManager
   */
  connectKillSwitch(killSwitchManager) {
    this.killSwitchManager = killSwitchManager;

    // Register callback to auto-retire affected strategies
    killSwitchManager.on('activated', (event) => {
      const affectedStrategies = this.listAll().filter(s => {
        // If kill switch affects this strategy, retire it
        return s.currentStage !== LifecycleStage.RETIRED;
      });

      for (const strategy of affectedStrategies) {
        this.retire(strategy.strategyId, {
          actor: 'kill-switch',
          reason: `Kill switch activated: ${event.reason || 'unknown'}`
        });
      }
    });
  }

  /**
   * Connect to observer registry
   * @param {ObserverRegistry} observerRegistry
   */
  connectObserver(observerRegistry) {
    this.observerRegistry = observerRegistry;
    // Observer integration: add stage metadata to run records
    // This is passive observation, no action needed here
  }
}

/**
 * @typedef {Object} DeployResult
 * @property {string} strategyId
 * @property {string} edgeId
 * @property {string} templateType
 * @property {Object} backtestSummary
 * @property {number} validationScore
 * @property {Object} promotionGuards
 */

/**
 * @typedef {Object} EvaluationResult
 * @property {string} strategyId
 * @property {PromotionResult|null} promotion
 * @property {DemotionResult|null} demotion
 */

/**
 * @typedef {Object} Summary
 * @property {number} totalStrategies
 * @property {Object.<string, number>} byStage
 * @property {number} pendingApprovals
 * @property {string} lastEvaluated
 */
