/**
 * StrategyAssembler - Assemble a fully configured strategy from template + edge + params
 *
 * Creates strategy instances ready for backtesting or deployment.
 */

export class StrategyAssembler {
  /**
   * Assemble a fully configured strategy from template + edge + params
   * @param {typeof BaseTemplate} TemplateClass
   * @param {Edge} edge
   * @param {Object} params - From ParameterMapper
   * @returns {BaseTemplate} Ready-to-run strategy instance
   */
  assemble(TemplateClass, edge, params) {
    // Create strategy instance
    const strategy = new TemplateClass({
      edge,
      config: {
        baseQuantity: params.baseQuantity,
        maxQuantity: params.maxQuantity,
        timeHorizon: params.timeHorizon,
        cooldownMs: params.cooldownMs,
        enabledFeatures: params.enabledFeatures,
        gateConfig: params.gateConfig
      }
    });

    return strategy;
  }

  /**
   * Get strategy metadata for registration
   * @param {BaseTemplate} strategy
   * @returns {Object} { strategyId, edgeId, templateType, params }
   */
  getMetadata(strategy) {
    return {
      strategyId: strategy.getStrategyId(),
      edgeId: strategy.edge.id,
      templateType: strategy.getTemplateType(),
      params: strategy.config
    };
  }
}
