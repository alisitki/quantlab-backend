/**
 * EdgeValidationPipeline - Orchestrate the full validation workflow
 *
 * Main entry point for edge validation.
 * Workflow: OOS → WalkForward → Decay → Regime → Score → Update status
 */

import { OutOfSampleValidator } from './OutOfSampleValidator.js';
import { WalkForwardAnalyzer } from './WalkForwardAnalyzer.js';
import { DecayDetector } from './DecayDetector.js';
import { RegimeRobustnessTester } from './RegimeRobustnessTester.js';
import { EdgeScorer } from './EdgeScorer.js';

export class EdgeValidationPipeline {
  /**
   * @param {Object} config
   * @param {EdgeRegistry} config.registry - EdgeRegistry
   * @param {Object} config.validatorConfig - OOS config
   * @param {Object} config.wfConfig - Walk-forward config
   * @param {Object} config.decayConfig - Decay detection config
   * @param {Object} config.regimeConfig - Regime robustness config
   * @param {Object} config.scorerConfig - Scorer config
   */
  constructor(config = {}) {
    this.registry = config.registry || null;
    this.oosValidator = new OutOfSampleValidator(config.validatorConfig);
    this.wfAnalyzer = new WalkForwardAnalyzer(config.wfConfig);
    this.decayDetector = new DecayDetector(config.decayConfig);
    this.regimeTester = new RegimeRobustnessTester(config.regimeConfig);
    this.scorer = new EdgeScorer(config.scorerConfig);
  }

  /**
   * Validate a single edge candidate
   * @param {Edge} edge
   * @param {DiscoveryDataset} dataset
   * @returns {Promise<ValidationResult>}
   *
   * ValidationResult = {
   *   edgeId: string,
   *   score: EdgeScore,
   *   oosResult: OOSResult,
   *   walkForwardResult: WalkForwardResult,
   *   decayResult: DecayResult,
   *   regimeResult: RegimeRobustnessResult,
   *   newStatus: 'VALIDATED'|'REJECTED',
   *   validatedAt: number
   * }
   */
  async validate(edge, dataset) {
    console.log('[EdgeValidationPipeline] ========================================');
    console.log(`[EdgeValidationPipeline] Validating edge: ${edge.id}`);
    console.log('[EdgeValidationPipeline] ========================================');

    // Step 1: Out-of-sample validation
    console.log('[EdgeValidationPipeline] Step 1: Out-of-sample validation...');
    const oosResult = this.oosValidator.validate(edge, dataset);

    // Step 2: Walk-forward analysis
    console.log('[EdgeValidationPipeline] Step 2: Walk-forward analysis...');
    const walkForwardResult = this.wfAnalyzer.analyze(edge, dataset);

    // Step 3: Decay detection
    console.log('[EdgeValidationPipeline] Step 3: Decay detection...');
    const decayResult = this.decayDetector.detect(edge, dataset);

    // Step 4: Regime robustness
    console.log('[EdgeValidationPipeline] Step 4: Regime robustness testing...');
    const regimeResult = this.regimeTester.test(edge, dataset);

    // Step 5: Score edge
    console.log('[EdgeValidationPipeline] Step 5: Scoring...');
    const score = this.scorer.score(oosResult, walkForwardResult, decayResult, regimeResult);

    console.log(`[EdgeValidationPipeline] ${score.summary}`);

    // Determine new status
    const newStatus = score.recommendation === 'VALIDATED' ? 'VALIDATED' : 'REJECTED';

    // Update edge status in registry if provided
    if (this.registry) {
      edge.status = newStatus;
      console.log(`[EdgeValidationPipeline] Updated edge status to: ${newStatus}`);
    }

    console.log('[EdgeValidationPipeline] ========================================');

    return {
      edgeId: edge.id,
      score,
      oosResult,
      walkForwardResult,
      decayResult,
      regimeResult,
      newStatus,
      validatedAt: Date.now()
    };
  }

  /**
   * Validate all CANDIDATE edges in registry
   * @param {DiscoveryDataset} dataset
   * @returns {Promise<Array<ValidationResult>>}
   */
  async validateAll(dataset) {
    if (!this.registry) {
      throw new Error('EdgeValidationPipeline: registry required for validateAll()');
    }

    const candidates = this.registry.getByStatus('CANDIDATE');

    console.log(`[EdgeValidationPipeline] Validating ${candidates.length} CANDIDATE edges`);

    const results = [];

    for (const edge of candidates) {
      const result = await this.validate(edge, dataset);
      results.push(result);
    }

    const validated = results.filter(r => r.newStatus === 'VALIDATED').length;
    const rejected = results.filter(r => r.newStatus === 'REJECTED').length;

    console.log(`[EdgeValidationPipeline] Validation complete: ${validated} VALIDATED, ${rejected} REJECTED`);

    return results;
  }

  /**
   * Re-validate a DEPLOYED edge (for decay monitoring)
   * @param {Edge} edge
   * @param {DiscoveryDataset} recentDataset - Recent data only
   * @returns {Promise<ValidationResult>}
   */
  async revalidate(edge, recentDataset) {
    console.log(`[EdgeValidationPipeline] Re-validating deployed edge: ${edge.id}`);

    return await this.validate(edge, recentDataset);
  }
}
