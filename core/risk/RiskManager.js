/**
 * QuantLab Risk Management v1 — Risk Manager (Core)
 * 
 * Orchestrates risk rules between Strategy and Execution.
 * Evaluates rules sequentially (fail-fast).
 * No side effects on execution state.
 * 
 * Usage in strategy:
 *   const risk = new RiskManager(config);
 *   
 *   onEvent(event, ctx) {
 *     risk.onEvent(event, ctx);
 *     
 *     // Check for forced exits (SL/TP)
 *     const forceExit = risk.checkForExit(event, ctx);
 *     if (forceExit) ctx.placeOrder(forceExit);
 *     
 *     // Check if signal allowed
 *     const decision = decide(features);
 *     const { allowed, reason } = risk.allow(decision, ctx);
 *     if (!allowed) return;
 *     
 *     ctx.placeOrder(order);
 *   }
 */

import { createRiskConfig } from './config.js';
import { MaxPositionRule } from './rules/MaxPositionRule.js';
import { CooldownRule } from './rules/CooldownRule.js';
import { MaxDailyLossRule } from './rules/MaxDailyLossRule.js';
import { StopLossTakeProfitRule } from './rules/StopLossTakeProfitRule.js';

/**
 * @typedef {Object} AllowResult
 * @property {boolean} allowed
 * @property {string} [reason]
 */

/**
 * Risk Manager - orchestrates all risk rules
 */
export class RiskManager {
  /** @type {import('./config.js').RiskConfig} */
  #config;
  
  /** @type {MaxPositionRule} */
  #maxPositionRule;
  
  /** @type {CooldownRule} */
  #cooldownRule;
  
  /** @type {MaxDailyLossRule} */
  #maxDailyLossRule;
  
  /** @type {StopLossTakeProfitRule} */
  #slTpRule;
  
  /** @type {number} */
  #rejectCount = 0;
  
  /** @type {number} */
  #forceExitCount = 0;

  /**
   * @param {Partial<import('./config.js').RiskConfig>} [userConfig={}]
   * @param {number} [initialCapital=10000]
   */
  constructor(userConfig = {}, initialCapital = 10000) {
    this.#config = createRiskConfig(userConfig);
    
    this.#maxPositionRule = new MaxPositionRule(this.#config.maxPositions);
    this.#cooldownRule = new CooldownRule(this.#config.cooldownEvents);
    this.#maxDailyLossRule = new MaxDailyLossRule(
      this.#config.maxDailyLossPct, 
      initialCapital
    );
    this.#slTpRule = new StopLossTakeProfitRule(
      this.#config.stopLossPct,
      this.#config.takeProfitPct
    );
  }

  /**
   * Update rule states on each event
   * Call this at the start of onEvent BEFORE any trading logic
   * @param {Object} event
   * @param {Object} ctx
   */
  onEvent(event, ctx) {
    if (!this.#config.enabled) return;
    
    this.#cooldownRule.onEvent(ctx);
    this.#maxDailyLossRule.onEvent(event, ctx);
  }

  /**
   * Check for forced exits (SL/TP)
   * @param {Object} event
   * @param {Object} ctx
   * @returns {import('./rules/StopLossTakeProfitRule.js').ForceExitOrder|null}
   */
  checkForExit(event, ctx) {
    if (!this.#config.enabled) return null;
    
    const exitOrder = this.#slTpRule.checkForExit(event, ctx);
    if (exitOrder) {
      this.#forceExitCount++;
    }
    return exitOrder;
  }

  /**
   * Check if signal is allowed by all rules
   * Fail-fast: first rejection blocks
   * @param {Object} signal - Trading signal with action property
   * @param {Object} ctx - Runner context
   * @returns {AllowResult}
   */
  allow(signal, ctx) {
    if (!this.#config.enabled) {
      return { allowed: true };
    }

    // Check rules in order (fail-fast)
    const rules = [
      this.#maxPositionRule,
      this.#cooldownRule,
      this.#maxDailyLossRule
    ];

    for (const rule of rules) {
      const result = rule.check(signal, ctx);
      if (!result.allowed) {
        this.#rejectCount++;
        return result;
      }
    }

    return { allowed: true };
  }

  /**
   * Get risk stats
   */
  getStats() {
    return {
      rejectCount: this.#rejectCount,
      forceExitCount: this.#forceExitCount,
      dailyLossLocked: this.#maxDailyLossRule.isLocked(),
      config: { ...this.#config }
    };
  }

  /**
   * Log configuration
   * @param {Object} logger
   */
  logConfig(logger) {
    logger.info('=== RiskManager v1 ===');
    logger.info(`  enabled=${this.#config.enabled}`);
    logger.info(`  maxPositions=${this.#config.maxPositions}`);
    logger.info(`  cooldownEvents=${this.#config.cooldownEvents}`);
    logger.info(`  maxDailyLossPct=${(this.#config.maxDailyLossPct * 100).toFixed(1)}%`);
    logger.info(`  stopLossPct=${(this.#config.stopLossPct * 100).toFixed(2)}%`);
    logger.info(`  takeProfitPct=${(this.#config.takeProfitPct * 100).toFixed(2)}%`);
  }
}
