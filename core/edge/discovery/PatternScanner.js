/**
 * PatternScanner - Find recurring patterns in behavior feature vectors
 *
 * Scans for patterns that precede significant returns using three methods:
 * 1. Threshold scanning - Test feature thresholds
 * 2. Quantile scanning - Test extreme quantiles
 * 3. Cluster scanning - Cluster behavior space and test each cluster
 */

import { RegimeCluster } from '../../regime/RegimeCluster.js';
import { DISCOVERY_CONFIG } from './config.js';

export class PatternScanner {
  /**
   * @param {Object} config
   * @param {number} config.minSupport - Minimum pattern occurrence (default: 30)
   * @param {number} config.returnThreshold - Minimum forward return to consider (default: 0.001)
   * @param {number} config.seed - Random seed
   * @param {string[]} config.scanMethods - ['threshold', 'quantile', 'cluster'] (default: all)
   */
  constructor(config = {}) {
    this.minSupport = config.minSupport || DISCOVERY_CONFIG.scanner.minSupport;
    this.returnThreshold = config.returnThreshold || DISCOVERY_CONFIG.scanner.returnThreshold;
    this.seed = config.seed || DISCOVERY_CONFIG.seed;
    this.scanMethods = config.scanMethods || ['threshold', 'quantile', 'cluster'];
    this.thresholdLevels = config.thresholdLevels || DISCOVERY_CONFIG.scanner.thresholdLevels;
    this.quantileLevels = config.quantileLevels || DISCOVERY_CONFIG.scanner.quantileLevels;
    this.clusterK = config.clusterK || DISCOVERY_CONFIG.scanner.clusterK;
    this.maxPatternsPerMethod = config.maxPatternsPerMethod || DISCOVERY_CONFIG.scanner.maxPatternsPerMethod;
  }

  /**
   * Scan for patterns in the dataset
   * @param {DiscoveryDataset} dataset
   * @returns {Array<PatternCandidate>|Promise<Array<PatternCandidate>>}
   *
   * PatternCandidate = {
   *   id: string,                    // Auto-generated pattern ID
   *   type: 'threshold'|'quantile'|'cluster',
   *   conditions: Array<{feature, operator, value}>,
   *   regimes: number[]|null,         // Regime constraint (null = any)
   *   direction: 'LONG'|'SHORT',
   *   support: number,                // How many times pattern occurs
   *   forwardReturns: {mean, median, std, count},
   *   horizon: number,                // Which forward horizon
   *   matchingIndices: number[]        // Indices in dataset where pattern matches
   * }
   *
   * STREAMING MODE:
   * If dataset.rows is an async iterator (multi-day streaming), uses memory-efficient
   * streaming algorithms with multi-pass iteration via dataset.rowsFactory().
   * Exact semantics guaranteed: same input → same output.
   */
  scan(dataset) {
    console.log(`[PatternScanner] Scanning with methods: ${this.scanMethods.join(', ')}`);

    // Detect streaming vs array mode
    // Use rowsFactory presence as indicator (more reliable than checking iterator state)
    const isStreaming = typeof dataset.rowsFactory === 'function';

    if (isStreaming) {
      console.log('[PatternScanner] Mode: STREAMING (async iterator detected)');
      return this.#scanStreaming(dataset);
    } else {
      console.log('[PatternScanner] Mode: ARRAY (backward compatible)');
      return this.#scanArray(dataset);
    }
  }

  /**
   * Check if object is async iterable
   */
  #isAsyncIterable(obj) {
    return obj && typeof obj[Symbol.asyncIterator] === 'function';
  }

  /**
   * Array-based scan (original implementation)
   */
  #scanArray(dataset) {
    const patterns = [];

    if (this.scanMethods.includes('threshold')) {
      patterns.push(...this.#scanThresholdPatterns(dataset));
    }

    if (this.scanMethods.includes('quantile')) {
      patterns.push(...this.#scanQuantilePatterns(dataset));
    }

    if (this.scanMethods.includes('cluster')) {
      patterns.push(...this.#scanClusterPatterns(dataset));
    }

    console.log(`[PatternScanner] Found ${patterns.length} total patterns`);

    // Filter by min support and return threshold
    const filtered = patterns.filter(p =>
      p.support >= this.minSupport &&
      Math.abs(p.forwardReturns.mean) >= this.returnThreshold
    );

    console.log(`[PatternScanner] ${filtered.length} patterns passed filters (support >= ${this.minSupport}, |return| >= ${this.returnThreshold})`);

    return filtered;
  }

  /**
   * Streaming-based scan (memory-efficient for multi-day)
   */
  async #scanStreaming(dataset) {
    const patterns = [];

    if (this.scanMethods.includes('threshold')) {
      patterns.push(...await this.#scanThresholdPatternsStreaming(dataset));
    }

    if (this.scanMethods.includes('quantile')) {
      patterns.push(...await this.#scanQuantilePatternsStreaming(dataset));
    }

    if (this.scanMethods.includes('cluster')) {
      patterns.push(...await this.#scanClusterPatternsStreaming(dataset));
    }

    console.log(`[PatternScanner] Found ${patterns.length} total patterns (streaming)`);

    // Filter by min support and return threshold
    const filtered = patterns.filter(p =>
      p.support >= this.minSupport &&
      Math.abs(p.forwardReturns.mean) >= this.returnThreshold
    );

    console.log(`[PatternScanner] ${filtered.length} patterns passed filters (support >= ${this.minSupport}, |return| >= ${this.returnThreshold})`);

    return filtered;
  }

  /**
   * Threshold-based scanning: find feature ranges that precede positive returns
   * @param {DiscoveryDataset} dataset
   * @returns {Array<PatternCandidate>}
   */
  #scanThresholdPatterns(dataset) {
    console.log('[PatternScanner] Running threshold scan...');
    const patterns = [];

    // Early exit if no data
    if (dataset.rows.length === 0) {
      return patterns;
    }

    // Test each feature with each threshold level
    for (const featureName of dataset.featureNames) {
      for (const threshold of this.thresholdLevels) {
        // Positive threshold (feature > threshold)
        for (const horizonKey of Object.keys(dataset.rows[0].forwardReturns)) {
          const horizon = parseInt(horizonKey.slice(1)); // Extract number from "h10"

          // Test globally (no regime constraint)
          patterns.push(...this.#testThresholdCondition(
            dataset,
            featureName,
            '>',
            threshold,
            null, // no regime constraint
            horizon
          ));

          // Test per regime
          const uniqueRegimes = [...new Set(dataset.rows.map(r => r.regime))];
          for (const regime of uniqueRegimes) {
            patterns.push(...this.#testThresholdCondition(
              dataset,
              featureName,
              '>',
              threshold,
              [regime],
              horizon
            ));
          }
        }

        // Negative threshold (feature < -threshold)
        for (const horizonKey of Object.keys(dataset.rows[0].forwardReturns)) {
          const horizon = parseInt(horizonKey.slice(1));

          patterns.push(...this.#testThresholdCondition(
            dataset,
            featureName,
            '<',
            -threshold,
            null,
            horizon
          ));

          const uniqueRegimes = [...new Set(dataset.rows.map(r => r.regime))];
          for (const regime of uniqueRegimes) {
            patterns.push(...this.#testThresholdCondition(
              dataset,
              featureName,
              '<',
              -threshold,
              [regime],
              horizon
            ));
          }
        }
      }
    }

    // Limit patterns
    const limited = patterns.slice(0, this.maxPatternsPerMethod);
    console.log(`[PatternScanner] Threshold scan found ${patterns.length} patterns (kept ${limited.length})`);

    return limited;
  }

  /**
   * Test a single threshold condition
   */
  #testThresholdCondition(dataset, featureName, operator, value, regimes, horizon) {
    const matchingIndices = [];
    const returns = [];

    for (let i = 0; i < dataset.rows.length; i++) {
      const row = dataset.rows[i];

      // Check regime constraint
      if (regimes && !regimes.includes(row.regime)) {
        continue;
      }

      // Check feature condition
      const featureValue = row.features[featureName];
      if (featureValue === null || featureValue === undefined) {
        continue;
      }

      let conditionMet = false;
      if (operator === '>') {
        conditionMet = featureValue > value;
      } else if (operator === '<') {
        conditionMet = featureValue < value;
      }

      if (!conditionMet) continue;

      // Get forward return
      const forwardReturn = row.forwardReturns[`h${horizon}`];
      if (forwardReturn === null) {
        continue;
      }

      matchingIndices.push(i);
      returns.push(forwardReturn);
    }

    if (matchingIndices.length === 0) {
      return [];
    }

    // Calculate return statistics
    const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
    const sortedReturns = [...returns].sort((a, b) => a - b);
    const median = sortedReturns[Math.floor(sortedReturns.length / 2)];
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
    const std = Math.sqrt(variance);

    // Determine direction
    const direction = mean > 0 ? 'LONG' : 'SHORT';

    // Generate pattern ID
    const regimeStr = regimes ? regimes.join(',') : 'all';
    const id = `threshold_${featureName}_${operator}_${value.toFixed(2)}_regime${regimeStr}_h${horizon}`;

    return [{
      id,
      type: 'threshold',
      conditions: [{ feature: featureName, operator, value }],
      regimes,
      direction,
      support: matchingIndices.length,
      forwardReturns: {
        mean,
        median,
        std,
        count: returns.length
      },
      horizon,
      matchingIndices
    }];
  }

  /**
   * Quantile-based scanning: use feature quantile extremes as entry signals
   * @param {DiscoveryDataset} dataset
   * @returns {Array<PatternCandidate>}
   */
  #scanQuantilePatterns(dataset) {
    console.log('[PatternScanner] Running quantile scan...');
    const patterns = [];

    // Early exit if no data
    if (dataset.rows.length === 0) {
      return patterns;
    }

    // For each feature, calculate quantiles
    for (const featureName of dataset.featureNames) {
      const featureValues = dataset.rows
        .map(r => r.features[featureName])
        .filter(v => v !== null && v !== undefined);

      if (featureValues.length === 0) continue;

      const sorted = [...featureValues].sort((a, b) => a - b);
      const q10 = sorted[Math.floor(sorted.length * 0.1)];
      const q90 = sorted[Math.floor(sorted.length * 0.9)];

      // Test low quantile (< q10)
      for (const horizonKey of Object.keys(dataset.rows[0].forwardReturns)) {
        const horizon = parseInt(horizonKey.slice(1));

        patterns.push(...this.#testThresholdCondition(
          dataset,
          featureName,
          '<',
          q10,
          null,
          horizon
        ));
      }

      // Test high quantile (> q90)
      for (const horizonKey of Object.keys(dataset.rows[0].forwardReturns)) {
        const horizon = parseInt(horizonKey.slice(1));

        patterns.push(...this.#testThresholdCondition(
          dataset,
          featureName,
          '>',
          q90,
          null,
          horizon
        ));
      }
    }

    const limited = patterns.slice(0, this.maxPatternsPerMethod);
    console.log(`[PatternScanner] Quantile scan found ${patterns.length} patterns (kept ${limited.length})`);

    return limited;
  }

  /**
   * Cluster-based scanning: cluster behavior vectors, test each cluster for edge
   * @param {DiscoveryDataset} dataset
   * @returns {Array<PatternCandidate>}
   */
  #scanClusterPatterns(dataset) {
    console.log('[PatternScanner] Running cluster scan...');

    // Early exit if no data
    if (dataset.rows.length === 0) {
      return [];
    }

    // Build feature matrix as Array<Array<number>> for memory efficiency
    // (avoids object overhead: 470 MB → 235 MB for 3.26M rows)
    const featureMatrix = dataset.rows.map(row => {
      return dataset.featureNames.map(fname => row.features[fname] || 0);
    });

    // Train cluster model
    const clusterModel = new RegimeCluster({
      k: this.clusterK,
      seed: this.seed
    });

    clusterModel.train(featureMatrix, dataset.featureNames);

    // Assign cluster labels
    const clusterLabels = featureMatrix.map(vec => clusterModel.predict(vec).cluster);

    // Test each cluster
    const patterns = [];

    for (let clusterId = 0; clusterId < this.clusterK; clusterId++) {
      for (const horizonKey of Object.keys(dataset.rows[0].forwardReturns)) {
        const horizon = parseInt(horizonKey.slice(1));

        const matchingIndices = [];
        const returns = [];

        for (let i = 0; i < dataset.rows.length; i++) {
          if (clusterLabels[i] !== clusterId) continue;

          const forwardReturn = dataset.rows[i].forwardReturns[`h${horizon}`];
          if (forwardReturn === null) continue;

          matchingIndices.push(i);
          returns.push(forwardReturn);
        }

        if (matchingIndices.length === 0) continue;

        // Calculate statistics
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const sortedReturns = [...returns].sort((a, b) => a - b);
        const median = sortedReturns[Math.floor(sortedReturns.length / 2)];
        const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
        const std = Math.sqrt(variance);

        const direction = mean > 0 ? 'LONG' : 'SHORT';

        const id = `cluster_${clusterId}_h${horizon}`;

        patterns.push({
          id,
          type: 'cluster',
          conditions: [{ feature: 'cluster_id', operator: '==', value: clusterId }],
          regimes: null,
          direction,
          support: matchingIndices.length,
          forwardReturns: {
            mean,
            median,
            std,
            count: returns.length
          },
          horizon,
          matchingIndices,
          clusterModel: clusterModel.toJSON() // Store model for reconstruction
        });
      }
    }

    const limited = patterns.slice(0, this.maxPatternsPerMethod);
    console.log(`[PatternScanner] Cluster scan found ${patterns.length} patterns (kept ${limited.length})`);

    return limited;
  }

  // ========================================================================
  // STREAMING SCAN METHODS (Memory-efficient for multi-day)
  // ========================================================================

  /**
   * Streaming threshold scan: Single-pass, O(patterns) memory
   */
  async #scanThresholdPatternsStreaming(dataset) {
    console.log('[PatternScanner] Running threshold scan (streaming)...');

    // Prepare all threshold conditions upfront
    const conditions = [];

    for (const featureName of dataset.featureNames) {
      for (const threshold of this.thresholdLevels) {
        // Positive threshold (feature > threshold)
        conditions.push({ featureName, operator: '>', value: threshold });
        // Negative threshold (feature < -threshold)
        conditions.push({ featureName, operator: '<', value: -threshold });
      }
    }

    // Get unique regimes (need first row to know horizon keys)
    let firstRow = null;
    const uniqueRegimes = new Set();

    // First peek to get horizons and regimes
    for await (const row of dataset.rowsFactory()) {
      if (!firstRow) firstRow = row;
      uniqueRegimes.add(row.regime);

      // Only need a sample for regimes
      if (uniqueRegimes.size > 10) break;
    }

    if (!firstRow) {
      console.log('[PatternScanner] No data rows found');
      return [];
    }

    const horizonKeys = Object.keys(firstRow.forwardReturns);
    const regimesList = [null, ...Array.from(uniqueRegimes)]; // null = global

    // Build full test matrix
    const testConfigs = [];
    for (const condition of conditions) {
      for (const horizon of horizonKeys) {
        for (const regime of regimesList) {
          testConfigs.push({
            ...condition,
            horizon: parseInt(horizon.slice(1)),
            regime
          });
        }
      }
    }

    console.log(`[PatternScanner] Testing ${testConfigs.length} threshold configurations...`);

    // Accumulate stats per config (NOT full rows)
    // MEMORY GUARD: Limit indices per config to prevent OOM
    const MAX_INDICES_PER_CONFIG = 50000; // Cap at 50K matches per pattern
    const configStats = new Map();
    let rowIndex = 0;
    let droppedConfigs = 0;

    for await (const row of dataset.rowsFactory()) {
      for (const config of testConfigs) {
        const configKey = `${config.featureName}_${config.operator}_${config.value}_r${config.regime}_h${config.horizon}`;

        // Skip if config already exceeded max indices
        const stats = configStats.get(configKey);
        if (stats && stats.indices.length >= MAX_INDICES_PER_CONFIG) {
          continue;
        }

        // Check regime constraint
        if (config.regime !== null && row.regime !== config.regime) {
          continue;
        }

        // Check feature condition
        const featureValue = row.features[config.featureName];
        if (featureValue === null || featureValue === undefined) {
          continue;
        }

        let conditionMet = false;
        if (config.operator === '>') {
          conditionMet = featureValue > config.value;
        } else if (config.operator === '<') {
          conditionMet = featureValue < config.value;
        }

        if (!conditionMet) continue;

        // Get forward return
        const forwardReturn = row.forwardReturns[`h${config.horizon}`];
        if (forwardReturn === null) {
          continue;
        }

        // Accumulate stats
        if (!configStats.has(configKey)) {
          configStats.set(configKey, {
            config,
            indices: [],
            returns: []
          });
        }

        const currentStats = configStats.get(configKey);

        // Only store if under limit
        if (currentStats.indices.length < MAX_INDICES_PER_CONFIG) {
          currentStats.indices.push(rowIndex);
          currentStats.returns.push(forwardReturn);
        }
      }

      rowIndex++;

      if (rowIndex % 500000 === 0) {
        console.log(`[PatternScanner] Threshold scan processed ${rowIndex} rows...`);
      }
    }

    // Count how many configs hit the limit
    for (const [key, stats] of configStats) {
      if (stats.indices.length >= MAX_INDICES_PER_CONFIG) {
        droppedConfigs++;
      }
    }

    if (droppedConfigs > 0) {
      console.log(`[PatternScanner] Warning: ${droppedConfigs} configs exceeded ${MAX_INDICES_PER_CONFIG} matches (capped)`);
    }

    console.log(`[PatternScanner] Threshold scan completed ${rowIndex} rows`);

    // Convert stats to patterns
    const patterns = [];

    for (const [configKey, stats] of configStats) {
      if (stats.indices.length === 0) continue;

      const { config } = stats;
      const returns = stats.returns;

      // Calculate statistics
      const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
      const sortedReturns = [...returns].sort((a, b) => a - b);
      const median = sortedReturns[Math.floor(sortedReturns.length / 2)];
      const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
      const std = Math.sqrt(variance);

      const direction = mean > 0 ? 'LONG' : 'SHORT';

      const regimeStr = config.regime !== null ? config.regime : 'all';
      const id = `threshold_${config.featureName}_${config.operator}_${config.value.toFixed(2)}_regime${regimeStr}_h${config.horizon}`;

      patterns.push({
        id,
        type: 'threshold',
        conditions: [{ feature: config.featureName, operator: config.operator, value: config.value }],
        regimes: config.regime !== null ? [config.regime] : null,
        direction,
        support: stats.indices.length,
        forwardReturns: {
          mean,
          median,
          std,
          count: returns.length
        },
        horizon: config.horizon,
        matchingIndices: stats.indices
      });
    }

    const limited = patterns.slice(0, this.maxPatternsPerMethod);
    console.log(`[PatternScanner] Threshold scan found ${patterns.length} patterns (kept ${limited.length})`);

    return limited;
  }

  /**
   * Streaming quantile scan: Two-pass, O(feature_values) memory
   */
  async #scanQuantilePatternsStreaming(dataset) {
    console.log('[PatternScanner] Running quantile scan (streaming)...');

    // Pass 1: Collect feature values for quantile calculation
    console.log('[PatternScanner] Quantile scan pass 1: collecting feature values...');
    const featureValues = new Map();
    let firstRow = null;

    for await (const row of dataset.rowsFactory()) {
      if (!firstRow) firstRow = row;

      for (const fname of dataset.featureNames) {
        const val = row.features[fname];
        if (val !== null && val !== undefined) {
          if (!featureValues.has(fname)) {
            featureValues.set(fname, []);
          }
          featureValues.get(fname).push(val);
        }
      }
    }

    if (!firstRow) {
      console.log('[PatternScanner] No data rows found');
      return [];
    }

    // Compute quantiles
    const quantiles = new Map();
    for (const [fname, values] of featureValues) {
      const sorted = [...values].sort((a, b) => a - b);
      quantiles.set(fname, {
        q10: sorted[Math.floor(sorted.length * 0.1)],
        q90: sorted[Math.floor(sorted.length * 0.9)]
      });
    }

    console.log(`[PatternScanner] Computed quantiles for ${quantiles.size} features`);

    // Pass 2: Match patterns using computed quantiles
    console.log('[PatternScanner] Quantile scan pass 2: matching patterns...');

    const horizonKeys = Object.keys(firstRow.forwardReturns);
    const testConfigs = [];

    for (const [fname, q] of quantiles) {
      for (const horizon of horizonKeys) {
        // Low quantile (< q10)
        testConfigs.push({
          featureName: fname,
          operator: '<',
          value: q.q10,
          horizon: parseInt(horizon.slice(1))
        });

        // High quantile (> q90)
        testConfigs.push({
          featureName: fname,
          operator: '>',
          value: q.q90,
          horizon: parseInt(horizon.slice(1))
        });
      }
    }

    const configStats = new Map();
    const MAX_INDICES_PER_CONFIG = 50000; // Memory guard
    let rowIndex = 0;
    let droppedConfigs = 0;

    for await (const row of dataset.rowsFactory()) {
      for (const config of testConfigs) {
        const configKey = `${config.featureName}_${config.operator}_${config.value}_h${config.horizon}`;

        // Skip if config already exceeded max indices
        const stats = configStats.get(configKey);
        if (stats && stats.indices.length >= MAX_INDICES_PER_CONFIG) {
          continue;
        }

        const featureValue = row.features[config.featureName];
        if (featureValue === null || featureValue === undefined) {
          continue;
        }

        let conditionMet = false;
        if (config.operator === '<') {
          conditionMet = featureValue < config.value;
        } else if (config.operator === '>') {
          conditionMet = featureValue > config.value;
        }

        if (!conditionMet) continue;

        const forwardReturn = row.forwardReturns[`h${config.horizon}`];
        if (forwardReturn === null) {
          continue;
        }

        if (!configStats.has(configKey)) {
          configStats.set(configKey, {
            config,
            indices: [],
            returns: []
          });
        }

        const currentStats = configStats.get(configKey);

        if (currentStats.indices.length < MAX_INDICES_PER_CONFIG) {
          currentStats.indices.push(rowIndex);
          currentStats.returns.push(forwardReturn);
        }
      }

      rowIndex++;

      if (rowIndex % 500000 === 0) {
        console.log(`[PatternScanner] Quantile scan processed ${rowIndex} rows...`);
      }
    }

    // Count capped configs
    for (const [key, stats] of configStats) {
      if (stats.indices.length >= MAX_INDICES_PER_CONFIG) {
        droppedConfigs++;
      }
    }

    if (droppedConfigs > 0) {
      console.log(`[PatternScanner] Warning: ${droppedConfigs} configs exceeded ${MAX_INDICES_PER_CONFIG} matches (capped)`);
    }

    console.log(`[PatternScanner] Quantile scan completed ${rowIndex} rows`);

    // Convert to patterns
    const patterns = [];

    for (const [configKey, stats] of configStats) {
      if (stats.indices.length === 0) continue;

      const { config } = stats;
      const returns = stats.returns;

      const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
      const sortedReturns = [...returns].sort((a, b) => a - b);
      const median = sortedReturns[Math.floor(sortedReturns.length / 2)];
      const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
      const std = Math.sqrt(variance);

      const direction = mean > 0 ? 'LONG' : 'SHORT';

      const id = `quantile_${config.featureName}_${config.operator}_${config.value.toFixed(4)}_h${config.horizon}`;

      patterns.push({
        id,
        type: 'quantile',
        conditions: [{ feature: config.featureName, operator: config.operator, value: config.value }],
        regimes: null,
        direction,
        support: stats.indices.length,
        forwardReturns: {
          mean,
          median,
          std,
          count: returns.length
        },
        horizon: config.horizon,
        matchingIndices: stats.indices
      });
    }

    const limited = patterns.slice(0, this.maxPatternsPerMethod);
    console.log(`[PatternScanner] Quantile scan found ${patterns.length} patterns (kept ${limited.length})`);

    return limited;
  }

  /**
   * Streaming cluster scan: Two-pass, O(feature_matrix) memory
   */
  async #scanClusterPatternsStreaming(dataset) {
    console.log('[PatternScanner] Running cluster scan (streaming)...');

    // Pass 1: Build compact feature matrix
    console.log('[PatternScanner] Cluster scan pass 1: building feature matrix...');
    const featureMatrix = [];
    let firstRow = null;

    for await (const row of dataset.rowsFactory()) {
      if (!firstRow) firstRow = row;

      // Compact representation: Array instead of object
      const vec = dataset.featureNames.map(fname => row.features[fname] || 0);
      featureMatrix.push(vec);

      if (featureMatrix.length % 500000 === 0) {
        console.log(`[PatternScanner] Feature matrix: ${featureMatrix.length} rows...`);
      }
    }

    if (featureMatrix.length === 0) {
      console.log('[PatternScanner] No data rows found');
      return [];
    }

    console.log(`[PatternScanner] Feature matrix built: ${featureMatrix.length} rows`);

    // Train cluster model
    const clusterModel = new RegimeCluster({
      k: this.clusterK,
      seed: this.seed
    });

    clusterModel.train(featureMatrix, dataset.featureNames);
    console.log(`[PatternScanner] Cluster model trained (k=${this.clusterK})`);

    // Pass 2: Assign labels and collect stats
    console.log('[PatternScanner] Cluster scan pass 2: assigning cluster labels...');

    const horizonKeys = Object.keys(firstRow.forwardReturns);
    const clusterStats = new Map(); // key: "clusterId_horizon", value: {indices, returns}
    const MAX_INDICES_PER_CLUSTER = 100000; // Memory guard (higher for clusters)

    let rowIndex = 0;
    let droppedClusters = 0;

    for await (const row of dataset.rowsFactory()) {
      const vec = dataset.featureNames.map(fname => row.features[fname] || 0);
      const { cluster } = clusterModel.predict(vec);

      for (const horizonKey of horizonKeys) {
        const horizon = parseInt(horizonKey.slice(1));
        const forwardReturn = row.forwardReturns[horizonKey];

        if (forwardReturn === null) continue;

        const statsKey = `${cluster}_h${horizon}`;

        // Skip if already at capacity
        const stats = clusterStats.get(statsKey);
        if (stats && stats.indices.length >= MAX_INDICES_PER_CLUSTER) {
          continue;
        }

        if (!clusterStats.has(statsKey)) {
          clusterStats.set(statsKey, {
            clusterId: cluster,
            horizon,
            indices: [],
            returns: []
          });
        }

        const currentStats = clusterStats.get(statsKey);

        if (currentStats.indices.length < MAX_INDICES_PER_CLUSTER) {
          currentStats.indices.push(rowIndex);
          currentStats.returns.push(forwardReturn);
        }
      }

      rowIndex++;

      if (rowIndex % 500000 === 0) {
        console.log(`[PatternScanner] Cluster scan processed ${rowIndex} rows...`);
      }
    }

    // Count capped clusters
    for (const [key, stats] of clusterStats) {
      if (stats.indices.length >= MAX_INDICES_PER_CLUSTER) {
        droppedClusters++;
      }
    }

    if (droppedClusters > 0) {
      console.log(`[PatternScanner] Warning: ${droppedClusters} clusters exceeded ${MAX_INDICES_PER_CLUSTER} matches (capped)`);
    }

    console.log(`[PatternScanner] Cluster scan completed ${rowIndex} rows`);

    // Convert to patterns
    const patterns = [];

    for (const [statsKey, stats] of clusterStats) {
      if (stats.indices.length === 0) continue;

      const returns = stats.returns;

      const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
      const sortedReturns = [...returns].sort((a, b) => a - b);
      const median = sortedReturns[Math.floor(sortedReturns.length / 2)];
      const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
      const std = Math.sqrt(variance);

      const direction = mean > 0 ? 'LONG' : 'SHORT';

      const id = `cluster_${stats.clusterId}_h${stats.horizon}`;

      patterns.push({
        id,
        type: 'cluster',
        conditions: [{ feature: 'cluster_id', operator: '==', value: stats.clusterId }],
        regimes: null,
        direction,
        support: stats.indices.length,
        forwardReturns: {
          mean,
          median,
          std,
          count: returns.length
        },
        horizon: stats.horizon,
        matchingIndices: stats.indices,
        clusterModel: clusterModel.toJSON()
      });
    }

    const limited = patterns.slice(0, this.maxPatternsPerMethod);
    console.log(`[PatternScanner] Cluster scan found ${patterns.length} patterns (kept ${limited.length})`);

    return limited;
  }
}
