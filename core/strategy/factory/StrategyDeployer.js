/**
 * StrategyDeployer - Register strategy in lifecycle and start promotion process
 *
 * Integrates with PromotionGuardManager for safe deployment.
 */

import { FACTORY_CONFIG } from './config.js';

export class StrategyDeployer {
  /**
   * @param {Object} config
   * @param {PromotionGuardManager} config.promotionGuardManager - Optional promotion manager
   * @param {StrategyLifecycleManager} config.lifecycleManager - Optional lifecycle manager
   */
  constructor(config = {}) {
    this.promotionGuardManager = config.promotionGuardManager || null;
    this.lifecycleManager = config.lifecycleManager || null;
  }

  /**
   * Deploy a backtested strategy into the lifecycle
   * @param {BaseTemplate} strategy
   * @param {BacktestResult} backtestResult
   * @param {ValidationResult} validationResult - From Phase 6
   * @returns {DeployResult}
   *
   * DeployResult = {
   *   strategyId: string,
   *   edgeId: string,
   *   stage: string,                 // Initial stage
   *   backtestSummary: Object,
   *   validationScore: number,
   *   promotionGuards: Object,
   *   deployedAt: number
   * }
   */
  deploy(strategy, backtestResult, validationResult) {
    console.log(`[StrategyDeployer] Deploying strategy: ${strategy.getStrategyId()}`);

    // Check if backtest passed
    if (FACTORY_CONFIG.deployment.requireBacktestPass && !backtestResult.passed) {
      throw new Error(`Strategy ${strategy.getStrategyId()} failed backtest requirements`);
    }

    // Create deployment record
    const deployResult = {
      strategyId: strategy.getStrategyId(),
      edgeId: strategy.edge.id,
      templateType: strategy.constructor.name || 'unknown',
      stage: FACTORY_CONFIG.deployment.initialStage,
      backtestSummary: {
        trades: backtestResult.trades,
        returnPct: backtestResult.returnPct,
        sharpe: backtestResult.sharpe,
        maxDrawdownPct: backtestResult.maxDrawdownPct,
        winRate: backtestResult.winRate
      },
      validationScore: validationResult?.score?.total || 0,
      promotionGuards: this._buildPromotionGuards(strategy, backtestResult),
      deployedAt: Date.now()
    };

    // Register with promotion guard manager if available
    if (this.promotionGuardManager) {
      // Integration with PromotionGuardManager would happen here
      // For now, just log
      console.log(`[StrategyDeployer] Would register with PromotionGuardManager`);
    }

    // Register with lifecycle manager if available
    if (this.lifecycleManager) {
      this.lifecycleManager.register(deployResult);
      console.log(`[StrategyDeployer] Registered ${deployResult.strategyId} with lifecycle manager`);
    }

    console.log(`[StrategyDeployer] Deployed ${deployResult.strategyId} at stage ${deployResult.stage}`);

    return deployResult;
  }

  /**
   * Build promotion guards based on strategy characteristics
   */
  _buildPromotionGuards(strategy, backtestResult) {
    return {
      maxDrawdownPct: backtestResult.maxDrawdownPct * 1.5, // Allow 50% worse than backtest
      minWinRate: Math.max(0.3, backtestResult.winRate * 0.8), // Allow 20% worse
      maxDailyLoss: 1000, // USD
      requireHumanApproval: true
    };
  }
}
