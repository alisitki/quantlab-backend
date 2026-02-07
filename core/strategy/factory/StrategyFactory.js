/**
 * StrategyFactory - Main orchestrator for strategy generation from validated edges
 *
 * Full pipeline: Edge → Template → Parameters → Assemble → Backtest → Deploy
 */

import { StrategyTemplateSelector } from './StrategyTemplateSelector.js';
import { StrategyParameterMapper } from './StrategyParameterMapper.js';
import { StrategyAssembler } from './StrategyAssembler.js';
import { AutoBacktester } from './AutoBacktester.js';
import { StrategyDeployer } from './StrategyDeployer.js';

export class StrategyFactory {
  /**
   * @param {Object} config
   * @param {EdgeRegistry} config.registry - EdgeRegistry
   * @param {Object} config.backtestConfig - AutoBacktester config
   * @param {Object} config.dataConfig - Paths for backtest data
   * @param {PromotionGuardManager} config.promotionGuardManager - Optional
   */
  constructor(config = {}) {
    this.registry = config.registry || null;
    this.backtestConfig = config.backtestConfig || {};
    this.dataConfig = config.dataConfig || {};

    this.templateSelector = new StrategyTemplateSelector();
    this.parameterMapper = new StrategyParameterMapper();
    this.assembler = new StrategyAssembler();
    this.backtester = new AutoBacktester(this.backtestConfig);
    this.deployer = new StrategyDeployer({
      promotionGuardManager: config.promotionGuardManager
    });
  }

  /**
   * Generate strategy from a validated edge
   * @param {Edge} edge - Must be VALIDATED status
   * @param {Object} validationResult - Validation result from Phase 6
   * @returns {Promise<FactoryResult>}
   *
   * FactoryResult = {
   *   strategyId: string,
   *   edgeId: string,
   *   templateType: string,
   *   backtestResult: BacktestResult,
   *   deployResult: DeployResult|null,
   *   status: 'DEPLOYED'|'BACKTEST_FAILED'|'ERROR'
   * }
   */
  async produce(edge, validationResult = null) {
    console.log('[StrategyFactory] ========================================');
    console.log(`[StrategyFactory] Producing strategy from edge: ${edge.id}`);
    console.log('[StrategyFactory] ========================================');

    try {
      // Step 1: Select template
      console.log('[StrategyFactory] Step 1: Selecting template...');
      const { templateClass, reason } = this.templateSelector.select(edge);
      console.log(`[StrategyFactory] Selected: ${templateClass.name} (${reason})`);

      // Step 2: Map parameters
      console.log('[StrategyFactory] Step 2: Mapping parameters...');
      const params = this.parameterMapper.map(edge, templateClass.name);

      // Step 3: Assemble strategy
      console.log('[StrategyFactory] Step 3: Assembling strategy...');
      const strategy = this.assembler.assemble(templateClass, edge, params);
      const metadata = this.assembler.getMetadata(strategy);
      console.log(`[StrategyFactory] Assembled: ${metadata.strategyId}`);

      // Step 4: Backtest strategy
      console.log('[StrategyFactory] Step 4: Running backtest...');

      if (!this.dataConfig.parquetPath) {
        throw new Error('StrategyFactory: dataConfig.parquetPath required for backtest');
      }

      const backtestResult = await this.backtester.run(strategy, this.dataConfig);

      if (!backtestResult.passed) {
        console.log(`[StrategyFactory] Backtest FAILED for ${metadata.strategyId}`);
        return {
          strategyId: metadata.strategyId,
          edgeId: edge.id,
          templateType: metadata.templateType,
          backtestResult,
          deployResult: null,
          status: 'BACKTEST_FAILED'
        };
      }

      // Step 5: Deploy strategy
      console.log('[StrategyFactory] Step 5: Deploying strategy...');
      const deployResult = this.deployer.deploy(strategy, backtestResult, validationResult);

      console.log('[StrategyFactory] ========================================');
      console.log(`[StrategyFactory] SUCCESS: ${metadata.strategyId} deployed`);
      console.log('[StrategyFactory] ========================================');

      return {
        strategyId: metadata.strategyId,
        edgeId: edge.id,
        templateType: metadata.templateType,
        backtestResult,
        deployResult,
        status: 'DEPLOYED'
      };
    } catch (error) {
      console.error(`[StrategyFactory] ERROR: ${error.message}`);
      return {
        strategyId: `error_${edge.id}`,
        edgeId: edge.id,
        templateType: 'unknown',
        backtestResult: null,
        deployResult: null,
        status: 'ERROR',
        error: error.message
      };
    }
  }

  /**
   * Generate strategies for all VALIDATED edges
   * @returns {Promise<Array<FactoryResult>>}
   */
  async produceAll() {
    if (!this.registry) {
      throw new Error('StrategyFactory: registry required for produceAll()');
    }

    const validatedEdges = this.registry.getByStatus('VALIDATED');

    console.log(`[StrategyFactory] Producing strategies for ${validatedEdges.length} VALIDATED edges`);

    const results = [];

    for (const edge of validatedEdges) {
      const result = await this.produce(edge);
      results.push(result);
    }

    const deployed = results.filter(r => r.status === 'DEPLOYED').length;
    const failed = results.filter(r => r.status === 'BACKTEST_FAILED').length;
    const errors = results.filter(r => r.status === 'ERROR').length;

    console.log(`[StrategyFactory] Production complete: ${deployed} DEPLOYED, ${failed} FAILED, ${errors} ERROR`);

    return results;
  }
}
