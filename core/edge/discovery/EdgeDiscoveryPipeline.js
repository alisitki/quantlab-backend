/**
 * EdgeDiscoveryPipeline - Orchestrate the full discovery workflow
 *
 * This is the main entry point for edge discovery.
 * Workflow: load data → scan patterns → test significance → generate edges → register
 */

import { DiscoveryDataLoader } from './DiscoveryDataLoader.js';
import { PatternScanner } from './PatternScanner.js';
import { StatisticalEdgeTester } from './StatisticalEdgeTester.js';
import { EdgeCandidateGenerator } from './EdgeCandidateGenerator.js';
import { DISCOVERY_CONFIG } from './config.js';

export class EdgeDiscoveryPipeline {
  /**
   * @param {Object} config
   * @param {Object} config.loader - DiscoveryDataLoader config
   * @param {Object} config.scanner - PatternScanner config
   * @param {Object} config.tester - StatisticalEdgeTester config
   * @param {Object} config.generator - EdgeCandidateGenerator config
   * @param {EdgeRegistry} config.registry - EdgeRegistry to register candidates
   * @param {number} config.seed - Global seed
   * @param {number} config.maxEdgesPerRun - Maximum edges to generate per run
   */
  constructor(config = {}) {
    this.loader = new DiscoveryDataLoader(config.loader);
    this.scanner = new PatternScanner(config.scanner);
    this.tester = new StatisticalEdgeTester(config.tester);
    this.generator = new EdgeCandidateGenerator(config.generator);
    this.registry = config.registry || null;
    this.seed = config.seed || DISCOVERY_CONFIG.seed;
    this.maxEdgesPerRun = config.maxEdgesPerRun || DISCOVERY_CONFIG.generator.maxEdgesPerRun;
  }

  /**
   * Run full discovery pipeline on historical data
   * @param {Object} params
   * @param {string} params.parquetPath
   * @param {string} params.metaPath
   * @param {string} params.symbol
   * @returns {Promise<DiscoveryResult>}
   *
   * DiscoveryResult = {
   *   patternsScanned: number,
   *   patternsTestedSignificant: number,
   *   edgeCandidatesGenerated: number,
   *   edgeCandidatesRegistered: number,
   *   edges: Edge[],
   *   rejectedPatterns: Array<{pattern, reason}>,
   *   metadata: { duration, dataRowCount, regimesUsed }
   * }
   */
  async run({ parquetPath, metaPath, symbol }) {
    const startTime = Date.now();

    console.log('[EdgeDiscoveryPipeline] ========================================');
    console.log('[EdgeDiscoveryPipeline] Starting Edge Discovery Pipeline');
    console.log('[EdgeDiscoveryPipeline] ========================================');

    // Step 1: Load data
    console.log('[EdgeDiscoveryPipeline] Step 1: Loading data...');
    const dataset = await this.loader.load({ parquetPath, metaPath, symbol });
    console.log(`[EdgeDiscoveryPipeline] Loaded ${dataset.rows.length} rows with ${dataset.metadata.regimeK} regimes`);

    // Step 2: Scan for patterns
    console.log('[EdgeDiscoveryPipeline] Step 2: Scanning for patterns...');
    const patterns = this.scanner.scan(dataset);
    console.log(`[EdgeDiscoveryPipeline] Found ${patterns.length} patterns`);

    if (patterns.length === 0) {
      console.log('[EdgeDiscoveryPipeline] No patterns found. Exiting.');
      return {
        patternsScanned: 0,
        patternsTestedSignificant: 0,
        edgeCandidatesGenerated: 0,
        edgeCandidatesRegistered: 0,
        edges: [],
        rejectedPatterns: [],
        metadata: {
          duration: Date.now() - startTime,
          dataRowCount: dataset.rows.length,
          regimesUsed: dataset.metadata.regimeK
        }
      };
    }

    // Step 3: Test patterns statistically
    console.log('[EdgeDiscoveryPipeline] Step 3: Testing patterns for statistical significance...');
    const testResults = await this.tester.testBatch(patterns, dataset);

    // Pair patterns with test results
    const pairedResults = patterns.map((pattern, i) => ({
      pattern,
      testResult: testResults[i]
    }));

    // Filter accepted patterns
    const acceptedPatterns = pairedResults.filter(({ testResult }) =>
      testResult.recommendation === 'ACCEPT'
    );

    const rejectedPatterns = pairedResults
      .filter(({ testResult }) => testResult.recommendation === 'REJECT')
      .map(({ pattern, testResult }) => ({
        pattern,
        reason: `overallScore=${testResult.overallScore.toFixed(2)}`
      }));

    console.log(`[EdgeDiscoveryPipeline] ${acceptedPatterns.length} patterns passed statistical tests`);

    if (acceptedPatterns.length === 0) {
      console.log('[EdgeDiscoveryPipeline] No patterns passed tests. Exiting.');
      return {
        patternsScanned: patterns.length,
        patternsTestedSignificant: 0,
        edgeCandidatesGenerated: 0,
        edgeCandidatesRegistered: 0,
        edges: [],
        rejectedPatterns,
        metadata: {
          duration: Date.now() - startTime,
          dataRowCount: dataset.rows.length,
          regimesUsed: dataset.metadata.regimeK
        }
      };
    }

    // Step 4: Generate edge candidates
    console.log('[EdgeDiscoveryPipeline] Step 4: Generating edge candidates...');

    // Limit number of edges
    const limitedAccepted = acceptedPatterns.slice(0, this.maxEdgesPerRun);
    if (limitedAccepted.length < acceptedPatterns.length) {
      console.log(`[EdgeDiscoveryPipeline] Limiting to ${this.maxEdgesPerRun} edges (from ${acceptedPatterns.length})`);
    }

    const edges = this.generator.generateBatch(limitedAccepted);

    console.log(`[EdgeDiscoveryPipeline] Generated ${edges.length} edge candidates`);

    // Step 5: Register edges (if registry provided)
    let registeredCount = 0;

    if (this.registry) {
      console.log('[EdgeDiscoveryPipeline] Step 5: Registering edges...');

      for (const edge of edges) {
        // Find original pattern for definition storage
        const paired = limitedAccepted.find(p =>
          edge.id.includes(p.pattern.id)
        );

        if (paired) {
          this.registry.register(edge, {
            pattern: paired.pattern,
            testResult: paired.testResult,
            discoveredAt: Date.now()
          });
          registeredCount++;
        }
      }

      console.log(`[EdgeDiscoveryPipeline] Registered ${registeredCount} edges in registry`);
    }

    const duration = Date.now() - startTime;

    console.log('[EdgeDiscoveryPipeline] ========================================');
    console.log(`[EdgeDiscoveryPipeline] Discovery complete in ${(duration / 1000).toFixed(1)}s`);
    console.log(`[EdgeDiscoveryPipeline] Patterns scanned: ${patterns.length}`);
    console.log(`[EdgeDiscoveryPipeline] Patterns passed tests: ${acceptedPatterns.length}`);
    console.log(`[EdgeDiscoveryPipeline] Edge candidates generated: ${edges.length}`);
    console.log(`[EdgeDiscoveryPipeline] Edges registered: ${registeredCount}`);
    console.log('[EdgeDiscoveryPipeline] ========================================');

    return {
      patternsScanned: patterns.length,
      patternsTestedSignificant: acceptedPatterns.length,
      edgeCandidatesGenerated: edges.length,
      edgeCandidatesRegistered: registeredCount,
      edges,
      rejectedPatterns,
      metadata: {
        duration,
        dataRowCount: dataset.rows.length,
        regimesUsed: dataset.metadata.regimeK
      }
    };
  }

  /**
   * Run on multiple days for more robust discovery (STREAMING)
   * @param {Array<{parquetPath, metaPath}>} files
   * @param {string} symbol
   * @returns {Promise<DiscoveryResult>}
   */
  async runMultiDayStreaming(files, symbol) {
    const startTime = Date.now();
    const diagTiming = process.env.QUANTLAB_DIAG_TIMING === 'true';
    const pipelineStart = diagTiming ? Date.now() : 0;
    const timing = diagTiming
      ? (checkpointName) => {
        console.log(
          `[Timing][Pipeline] ${checkpointName} ` +
          `elapsed_ms_since_pipeline_start=${Date.now() - pipelineStart}`
        );
      }
      : () => {};

    timing('start');

    console.log('[EdgeDiscoveryPipeline] ========================================');
    console.log(`[EdgeDiscoveryPipeline] Running multi-day discovery (STREAMING) with ${files.length} files`);
    console.log('[EdgeDiscoveryPipeline] ========================================');

    // Step 1: Load multi-day iterator factory
    console.log('[EdgeDiscoveryPipeline] Step 1: Creating iterator factory...');
    const iteratorFactory = await this.loader.loadMultiDayStreaming(files, symbol);
    console.log(`[EdgeDiscoveryPipeline] Iterator factory ready. Estimated rows: ${iteratorFactory.metadata.rowCount}`);

    // Step 2: Prepare streaming dataset (NO row accumulation)
    console.log('[EdgeDiscoveryPipeline] Step 2: Preparing streaming dataset...');

    const dataset = {
      rows: iteratorFactory(),              // ← Iterator (streaming)
      rowsFactory: iteratorFactory,         // ← Factory for multi-pass
      regimeModel: iteratorFactory.metadata.regimeModel,
      featureNames: iteratorFactory.metadata.featureNames,
      metadata: iteratorFactory.metadata
    };

    console.log('[EdgeDiscoveryPipeline] Dataset ready (streaming mode)');
    timing('after_load_or_open');

    // Step 3: Scan for patterns (streaming-compatible)
    console.log('[EdgeDiscoveryPipeline] Step 3: Scanning for patterns...');
    const patterns = await this.scanner.scan(dataset);
    console.log(`[EdgeDiscoveryPipeline] Found ${patterns.length} patterns`);
    timing('after_pattern_scan');

    if (patterns.length === 0) {
      console.log('[EdgeDiscoveryPipeline] No patterns found. Exiting.');
      timing('early_exit_no_patterns');
      timing('after_stat_tests');
      timing('end');
      return {
        patternsScanned: 0,
        patternsTestedSignificant: 0,
        edgeCandidatesGenerated: 0,
        edgeCandidatesRegistered: 0,
        edges: [],
        rejectedPatterns: [],
        metadata: {
          duration: Date.now() - startTime,
          dataRowCount: dataset.metadata.rowCount || 0,
          regimesUsed: dataset.metadata.regimeK,
          filesLoaded: files.length
        }
      };
    }

    console.log('[EdgeDiscoveryPipeline] Step 4: Testing patterns for statistical significance...');
    const testResults = await this.tester.testBatch(patterns, dataset);
    timing('after_stat_tests');

    const pairedResults = patterns.map((pattern, i) => ({
      pattern,
      testResult: testResults[i]
    }));

    const acceptedPatterns = pairedResults.filter(({ testResult }) =>
      testResult.recommendation === 'ACCEPT'
    );

    const rejectedPatterns = pairedResults
      .filter(({ testResult }) => testResult.recommendation === 'REJECT')
      .map(({ pattern, testResult }) => ({
        pattern,
        reason: `overallScore=${testResult.overallScore.toFixed(2)}`
      }));

    console.log(`[EdgeDiscoveryPipeline] ${acceptedPatterns.length} patterns passed statistical tests`);

    if (acceptedPatterns.length === 0) {
      console.log('[EdgeDiscoveryPipeline] No patterns passed tests. Exiting.');
      timing('end');
      return {
        patternsScanned: patterns.length,
        patternsTestedSignificant: 0,
        edgeCandidatesGenerated: 0,
        edgeCandidatesRegistered: 0,
        edges: [],
        rejectedPatterns,
        metadata: {
          duration: Date.now() - startTime,
          dataRowCount: dataset.metadata.rowCount || 0,
          regimesUsed: dataset.metadata.regimeK,
          filesLoaded: files.length
        }
      };
    }

    console.log('[EdgeDiscoveryPipeline] Step 5: Generating edge candidates...');

    const limitedAccepted = acceptedPatterns.slice(0, this.maxEdgesPerRun);
    if (limitedAccepted.length < acceptedPatterns.length) {
      console.log(`[EdgeDiscoveryPipeline] Limiting to ${this.maxEdgesPerRun} edges (from ${acceptedPatterns.length})`);
    }

    const edges = this.generator.generateBatch(limitedAccepted);
    console.log(`[EdgeDiscoveryPipeline] Generated ${edges.length} edge candidates`);

    let registeredCount = 0;

    if (this.registry) {
      console.log('[EdgeDiscoveryPipeline] Step 6: Registering edges...');

      for (const edge of edges) {
        const paired = limitedAccepted.find(p =>
          edge.id.includes(p.pattern.id)
        );

        if (paired) {
          this.registry.register(edge, {
            pattern: paired.pattern,
            testResult: paired.testResult,
            discoveredAt: Date.now()
          });
          registeredCount++;
        }
      }

      console.log(`[EdgeDiscoveryPipeline] Registered ${registeredCount} edges in registry`);
      timing('after_registry_register');
    }

    const duration = Date.now() - startTime;

    console.log('[EdgeDiscoveryPipeline] ========================================');
    console.log(`[EdgeDiscoveryPipeline] Multi-day discovery complete in ${(duration / 1000).toFixed(1)}s`);
    console.log(`[EdgeDiscoveryPipeline] Patterns scanned: ${patterns.length}`);
    console.log(`[EdgeDiscoveryPipeline] Patterns passed tests: ${acceptedPatterns.length}`);
    console.log(`[EdgeDiscoveryPipeline] Edge candidates generated: ${edges.length}`);
    console.log(`[EdgeDiscoveryPipeline] Edges registered: ${registeredCount}`);
    console.log('[EdgeDiscoveryPipeline] ========================================');

    timing('end');
    return {
      patternsScanned: patterns.length,
      patternsTestedSignificant: acceptedPatterns.length,
      edgeCandidatesGenerated: edges.length,
      edgeCandidatesRegistered: registeredCount,
      edges,
      rejectedPatterns,
      metadata: {
        duration,
        dataRowCount: dataset.metadata.rowCount || 0,
        regimesUsed: dataset.metadata.regimeK,
        filesLoaded: files.length
      }
    };
  }

  /**
   * Run on multiple days for more robust discovery (LEGACY - uses allRows concat)
   * @param {Array<{parquetPath, metaPath}>} files
   * @param {string} symbol
   * @returns {Promise<DiscoveryResult>}
   */
  async runMultiDay(files, symbol) {
    const startTime = Date.now();

    console.log(`[EdgeDiscoveryPipeline] Running multi-day discovery with ${files.length} files`);

    // Load multi-day data
    const dataset = await this.loader.loadMultiDay(files, symbol);

    // Continue with standard pipeline
    const patterns = this.scanner.scan(dataset);
    const testResults = await this.tester.testBatch(patterns, dataset);

    const pairedResults = patterns.map((pattern, i) => ({
      pattern,
      testResult: testResults[i]
    }));

    const acceptedPatterns = pairedResults.filter(({ testResult }) =>
      testResult.recommendation === 'ACCEPT'
    );

    const rejectedPatterns = pairedResults
      .filter(({ testResult }) => testResult.recommendation === 'REJECT')
      .map(({ pattern, testResult }) => ({
        pattern,
        reason: `overallScore=${testResult.overallScore.toFixed(2)}`
      }));

    const limitedAccepted = acceptedPatterns.slice(0, this.maxEdgesPerRun);
    const edges = this.generator.generateBatch(limitedAccepted);

    let registeredCount = 0;

    if (this.registry) {
      for (const edge of edges) {
        const paired = limitedAccepted.find(p =>
          edge.id.includes(p.pattern.id)
        );

        if (paired) {
          this.registry.register(edge, {
            pattern: paired.pattern,
            testResult: paired.testResult,
            discoveredAt: Date.now()
          });
          registeredCount++;
        }
      }
    }

    const duration = Date.now() - startTime;

    console.log(`[EdgeDiscoveryPipeline] Multi-day discovery complete in ${(duration / 1000).toFixed(1)}s`);

    return {
      patternsScanned: patterns.length,
      patternsTestedSignificant: acceptedPatterns.length,
      edgeCandidatesGenerated: edges.length,
      edgeCandidatesRegistered: registeredCount,
      edges,
      rejectedPatterns,
      metadata: {
        duration,
        dataRowCount: dataset.rows.length,
        regimesUsed: dataset.metadata.regimeK,
        filesLoaded: files.length
      }
    };
  }
}
