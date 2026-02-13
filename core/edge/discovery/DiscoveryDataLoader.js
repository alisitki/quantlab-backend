/**
 * DiscoveryDataLoader - Load historical data into feature+regime+return matrix
 *
 * Bridges ReplayEngine/FeatureBuilder and discovery pipeline.
 * Loads historical data, extracts features, computes regime labels, aligns with forward returns.
 */

import { ReplayEngine } from '../../replay/ReplayEngine.js';
import { FeatureRegistry } from '../../features/FeatureRegistry.js';
import { RegimeCluster } from '../../regime/RegimeCluster.js';
import { DISCOVERY_CONFIG } from './config.js';

export class DiscoveryDataLoader {
  /**
   * @param {Object} config
   * @param {string[]} config.featureNames - Behavior features to extract
   * @param {string[]} config.regimeFeatures - Features for regime clustering
   * @param {number} config.regimeK - Number of regime clusters (default: 4)
   * @param {number[]} config.forwardHorizons - Forward return horizons in events [10, 50, 100]
   * @param {number} config.seed - Random seed
   * @param {number|null} config.maxRowsPerDay - Optional cap on feature rows per day (streaming only)
   * @param {string|null} config.smokeSlice - Optional slice selection for smoke (head|tail|head_tail)
   */
  constructor(config = {}) {
    this.featureNames = config.featureNames || DISCOVERY_CONFIG.behaviorFeatures;
    this.regimeFeatures = config.regimeFeatures || DISCOVERY_CONFIG.regimeFeatures;
    this.regimeK = config.regimeK || DISCOVERY_CONFIG.regimeK;
    this.forwardHorizons = config.forwardHorizons || DISCOVERY_CONFIG.forwardHorizons;
    this.seed = config.seed || DISCOVERY_CONFIG.seed;
    this.maxRowsPerDay = config.maxRowsPerDay ?? null;
    this.smokeSlice = config.smokeSlice ?? 'head';
    this.stream = config.stream ?? null;
  }

  /**
   * Trade-only adapter: provide BBO-compatible fields for feature builders.
   * No-op for non-trade streams.
   * @param {Object} event
   * @param {Object|null} adapterStats
   * @param {string|null} stage
   * @returns {Object}
   */
  adaptTradeEventForFeatures(event, adapterStats = null, stage = null) {
    if (this.stream !== 'trade') return event;

    const px = Number(event?.price ?? event?.trade_price ?? event?.last_trade_price);
    if (!Number.isFinite(px)) {
      if (adapterStats) {
        adapterStats.px_missing_passthrough++;
      }
      return event;
    }

    let qty = Number(event?.qty ?? event?.trade_qty ?? event?.size);
    if (!Number.isFinite(qty) || qty <= 0) {
      qty = 1e-9;
    }

    const spread = Math.max(Math.abs(px) * 1e-6, 1e-8);

    if (adapterStats && stage === 'step1') {
      adapterStats.step1_applied++;
    } else if (adapterStats && stage === 'day_collect') {
      adapterStats.day_collect_applied++;
    }

    return {
      ...event,
      bid_price: px - spread / 2,
      ask_price: px + spread / 2,
      bid_qty: qty,
      ask_qty: qty
    };
  }

  /**
   * Load data from a single replay file
   * @param {Object} params
   * @param {string} params.parquetPath
   * @param {string} params.metaPath
   * @param {string} params.symbol
   * @returns {Promise<DiscoveryDataset>}
   *
   * DiscoveryDataset = {
   *   rows: Array<{features: Object, regime: number, forwardReturns: Object, timestamp: bigint}>,
   *   regimeModel: RegimeCluster (trained),
   *   featureNames: string[],
   *   metadata: { symbol, rowCount, dateRange }
   * }
   */
  async load({ parquetPath, metaPath, symbol }) {
    console.log(`[DiscoveryDataLoader] Loading data from ${parquetPath}`);

    // Initialize replay engine
    const replayEngine = new ReplayEngine({ parquet: parquetPath, meta: metaPath });
    const meta = await replayEngine.getMeta();

    // Create feature builder with all required features
    const allFeatures = [
      ...DISCOVERY_CONFIG.baseFeatures,
      ...this.featureNames,
      ...this.regimeFeatures
    ];

    console.log(`[DiscoveryDataLoader] Creating FeatureBuilder for symbol: ${symbol}`);
    console.log(`[DiscoveryDataLoader] Enabled features (${allFeatures.length}): ${allFeatures.join(', ')}`);

    const featureBuilder = FeatureRegistry.createFeatureBuilder(symbol, {
      enabledFeatures: allFeatures
    });

    console.log(`[DiscoveryDataLoader] FeatureBuilder created successfully`);

    // Test with a mock event
    const testEvent = {
      symbol: 'ADAUSDT',
      bid_price: 0.2987,
      ask_price: 0.2988,
      bid_qty: 70000,
      ask_qty: 130000,
      ts_event: 1770076735490
    };
    const testFeatures = featureBuilder.onEvent(testEvent);
    console.log(`[DiscoveryDataLoader] Manual test with mock event: ${testFeatures ? `mid_price=${testFeatures.mid_price}` : 'null'}`);

    // First pass: collect all features and mid prices
    console.log('[DiscoveryDataLoader] First pass: collecting features...');
    const rawRows = [];
    const midPrices = [];
    let eventCount = 0;
    let warmupCount = 0;

    for await (const event of replayEngine.replay()) {
      eventCount++;

      if (eventCount <= 3 || eventCount === 100 || eventCount === 1000) {
        console.log(`[DiscoveryDataLoader] Event ${eventCount}: symbol=${event.symbol}, bid=${event.bid_price}, ask=${event.ask_price}`);
      }

      const adaptedEvent = this.adaptTradeEventForFeatures(event);
      const features = featureBuilder.onEvent(adaptedEvent);

      if (eventCount <= 3 || eventCount === 100 || eventCount === 1000) {
        console.log(`[DiscoveryDataLoader] Features ${eventCount}: ${features ? `mid_price=${features.mid_price}` : 'null'}`);
      }

      if (!features || features.mid_price === null || features.mid_price === undefined) {
        warmupCount++;
        if (warmupCount <= 5) {
          console.log(`[DiscoveryDataLoader] Warmup ${warmupCount}: features=${features ? 'object but mid_price null/undefined' : 'null'}`);
        }
        continue; // Skip warmup period
      }

      rawRows.push({
        features,
        timestamp: event.ts_event,
        index: rawRows.length
      });

      midPrices.push(features.mid_price);
    }

    console.log(`[DiscoveryDataLoader] Collected ${rawRows.length} feature rows from ${eventCount} events (${warmupCount} warmup)`);

    // Calculate forward returns
    console.log('[DiscoveryDataLoader] Calculating forward returns...');
    const rows = rawRows.map((row, i) => {
      const forwardReturns = {};

      for (const horizon of this.forwardHorizons) {
        const futureIdx = i + horizon;

        if (futureIdx < midPrices.length) {
          const currentPrice = midPrices[i];
          const futurePrice = midPrices[futureIdx];
          forwardReturns[`h${horizon}`] = (futurePrice - currentPrice) / currentPrice;
        } else {
          forwardReturns[`h${horizon}`] = null; // No future data
        }
      }

      return {
        ...row,
        forwardReturns
      };
    });

    // Extract regime feature vectors for clustering
    console.log('[DiscoveryDataLoader] Training regime cluster model...');
    const regimeVectors = rows.map(row => {
      const vector = {};
      for (const fname of this.regimeFeatures) {
        vector[fname] = row.features[fname] || 0;
      }
      return vector;
    });

    // Train regime cluster
    const regimeModel = new RegimeCluster({
      k: this.regimeK,
      seed: this.seed
    });

    regimeModel.train(regimeVectors, this.regimeFeatures);

    // Assign regime labels to each row
    console.log('[DiscoveryDataLoader] Assigning regime labels...');
    rows.forEach(row => {
      const regimeVector = this.regimeFeatures.map(fname => row.features[fname] || 0);
      const prediction = regimeModel.predict(regimeVector);
      row.regime = prediction.cluster;
      row.regimeConfidence = prediction.confidence;
    });

    // Build dataset metadata
    const timestamps = rows.map(r => r.timestamp);
    const metadata = {
      symbol,
      rowCount: rows.length,
      dateRange: {
        start: timestamps[0],
        end: timestamps[timestamps.length - 1]
      },
      horizonsUsed: this.forwardHorizons,
      regimeK: this.regimeK
    };

    console.log(`[DiscoveryDataLoader] Dataset ready: ${rows.length} rows, ${this.regimeK} regimes`);

    return {
      rows,
      regimeModel,
      featureNames: this.featureNames,
      metadata
    };
  }

  /**
   * Load from multiple date files (LEGACY - uses allRows concat)
   * @param {Array<{parquetPath, metaPath}>} files
   * @param {string} symbol
   * @returns {Promise<DiscoveryDataset>}
   */
  async loadMultiDay(files, symbol) {
    console.log(`[DiscoveryDataLoader] Loading multi-day data: ${files.length} files`);

    const allRows = [];
    let regimeModel = null;

    for (const file of files) {
      const dataset = await this.load({
        parquetPath: file.parquetPath,
        metaPath: file.metaPath,
        symbol
      });

      // Use concat instead of spread to avoid call stack overflow with large arrays
      for (const row of dataset.rows) {
        allRows.push(row);
      }

      // Use regime model from first file for consistency
      if (!regimeModel) {
        regimeModel = dataset.regimeModel;
      }
    }

    // Re-assign regime labels using consistent model
    console.log('[DiscoveryDataLoader] Re-labeling regimes with consistent model...');
    allRows.forEach(row => {
      const regimeVector = this.regimeFeatures.map(fname => row.features[fname] || 0);
      const prediction = regimeModel.predict(regimeVector);
      row.regime = prediction.cluster;
      row.regimeConfidence = prediction.confidence;
    });

    const timestamps = allRows.map(r => r.timestamp);
    const metadata = {
      symbol,
      rowCount: allRows.length,
      dateRange: {
        start: timestamps[0],
        end: timestamps[timestamps.length - 1]
      },
      filesLoaded: files.length,
      horizonsUsed: this.forwardHorizons,
      regimeK: this.regimeK
    };

    console.log(`[DiscoveryDataLoader] Multi-day dataset ready: ${allRows.length} rows`);

    return {
      rows: allRows,
      regimeModel,
      featureNames: this.featureNames,
      metadata
    };
  }

  /**
   * Load from multiple date files (STREAMING - iterator factory)
   * @param {Array<{parquetPath, metaPath}>} files
   * @param {string} symbol
   * @returns {Promise<Function>} Iterator factory + metadata
   */
  async loadMultiDayStreaming(files, symbol) {
    console.log(`[DiscoveryDataLoader] Loading multi-day data (streaming): ${files.length} files`);
    const maxRowsPerDay = Number.isFinite(this.maxRowsPerDay) ? this.maxRowsPerDay : null;
    const smokeSlice = typeof this.smokeSlice === 'string' ? this.smokeSlice : 'head';
    const replayOpts =
      maxRowsPerDay !== null && smokeSlice !== 'head'
        ? { slice: smokeSlice, rowLimit: maxRowsPerDay }
        : undefined;
    const smokeCapEnabled = maxRowsPerDay !== null;
    let smokeSelectedMax = 0;
    let smokeEmittedMax = 0;
    const isTradeStream = this.stream === 'trade';
    const tradeAdapterStats = {
      step1_applied: 0,
      day_collect_applied: 0,
      px_missing_passthrough: 0
    };
    let tradeTotalRowsSeen = 0;
    let tradeRowsWithRequiredFields = 0;
    let tradeFeatureableRowsAfterWarmup = 0;
    const tradeHasRequiredFields = (event) => {
      const parseFinite = (value) => {
        const n = Number(value);
        return Number.isFinite(n);
      };

      const price = event?.price ?? event?.trade_price ?? event?.last_trade_price;
      const qty = event?.qty ?? event?.trade_qty ?? event?.size;
      if (parseFinite(price) && parseFinite(qty)) {
        return true;
      }

      for (const value of Object.values(event || {})) {
        if (parseFinite(value)) return true;
      }
      return false;
    };

    // Step 1: Train regime model on first file (STREAMING - no full load)
    console.log('[DiscoveryDataLoader] Training regime model on first file (streaming)...');

    const replayEngine = new ReplayEngine({
      parquet: files[0].parquetPath,
      meta: files[0].metaPath
    });

    // Create feature builder for regime training
    const allFeatures = [
      ...DISCOVERY_CONFIG.baseFeatures,
      ...this.featureNames,
      ...this.regimeFeatures
    ];

    const featureBuilder = FeatureRegistry.createFeatureBuilder(symbol, {
      enabledFeatures: allFeatures
    });

    // Streaming collect: ONLY regime vectors (no rawRows, no midPrices)
    const regimeVectors = [];
    let firstTimestamp = null;
    let eventCount = 0;
    let warmupCount = 0;

    console.log('[DiscoveryDataLoader] Collecting regime vectors (streaming)...');

    for await (const event of replayEngine.replay(replayOpts)) {
      eventCount++;
      if (isTradeStream) {
        tradeTotalRowsSeen++;
        if (tradeHasRequiredFields(event)) {
          tradeRowsWithRequiredFields++;
        }
      }
      const adaptedEvent = this.adaptTradeEventForFeatures(event, tradeAdapterStats, 'step1');
      const features = featureBuilder.onEvent(adaptedEvent);

      if (!features || features.mid_price === null || features.mid_price === undefined) {
        warmupCount++;
        continue;
      }

      // Capture first timestamp
      if (!firstTimestamp) {
        firstTimestamp = event.ts_event;
      }

      // Extract ONLY regime features (no full row storage)
      const regimeVector = {};
      for (const fname of this.regimeFeatures) {
        regimeVector[fname] = features[fname] || 0;
      }
      regimeVectors.push(regimeVector);
      if (isTradeStream) {
        tradeFeatureableRowsAfterWarmup++;
      }

      if (maxRowsPerDay !== null && regimeVectors.length >= maxRowsPerDay) {
        break;
      }
    }

    console.log(`[DiscoveryDataLoader] Collected ${regimeVectors.length} regime vectors from ${eventCount} events (${warmupCount} warmup)`);
    if (isTradeStream) {
      const tradeCoverage = tradeTotalRowsSeen > 0
        ? tradeFeatureableRowsAfterWarmup / tradeTotalRowsSeen
        : 0;
      console.log(
        `[TradeDiag] total_rows=${tradeTotalRowsSeen} ` +
        `rows_with_required_fields=${tradeRowsWithRequiredFields} ` +
        `featureable_rows=${tradeFeatureableRowsAfterWarmup} ` +
        `coverage=${tradeCoverage.toFixed(6)}`
      );
      if (tradeFeatureableRowsAfterWarmup === 0) {
        console.error('[TradeDiag] FAIL: Training data would be empty (featureable_rows=0)');
        const err = new Error('TRADE_DIAG_EMPTY');
        err.code = 'TRADE_DIAG_EMPTY';
        err.exit_code = 1;
        throw err;
      }
    }

    // Train regime cluster (full data - exact semantics)
    const regimeModel = new RegimeCluster({
      k: this.regimeK,
      seed: this.seed
    });

    regimeModel.train(regimeVectors, this.regimeFeatures);

    console.log(`[DiscoveryDataLoader] Regime model trained: ${this.regimeK} clusters`);
    console.log(`[DiscoveryDataLoader] Memory: regime vectors GC-able now (${regimeVectors.length} vectors)`);

    // Step 2: Prepare metadata (without loading all files - memory efficient)
    const lastTimestamp = null; // Unknown until last file is processed

    console.log(`[DiscoveryDataLoader] Metadata prepared (totalRowCount: TBD via iterator)`);

    // Step 3: Create iterator factory
    const self = this;
    const iteratorFactory = async function*() {
      let passSelectedTotal = 0;
      let passEmittedTotal = 0;
      try {
      for (const file of files) {
        console.log(`[DiscoveryDataLoader] Streaming file: ${file.parquetPath}`);

        // Load day data
        const replayEngine = new ReplayEngine({ parquet: file.parquetPath, meta: file.metaPath });

        // Create feature builder
        const allFeatures = [
          ...DISCOVERY_CONFIG.baseFeatures,
          ...self.featureNames,
          ...self.regimeFeatures
        ];

        const featureBuilder = FeatureRegistry.createFeatureBuilder(symbol, {
          enabledFeatures: allFeatures
        });

        const dayRows = [];
        const dayMidPrices = [];
        let eventCount = 0;
        let warmupCount = 0;

        // Collect day data
        for await (const event of replayEngine.replay(replayOpts)) {
          eventCount++;
          const adaptedEvent = self.adaptTradeEventForFeatures(event, tradeAdapterStats, 'day_collect');
          const features = featureBuilder.onEvent(adaptedEvent);

          if (!features || features.mid_price === null || features.mid_price === undefined) {
            warmupCount++;
            continue;
          }

          dayRows.push({
            features,
            timestamp: event.ts_event
          });

          dayMidPrices.push(features.mid_price);

          if (maxRowsPerDay !== null && dayRows.length >= maxRowsPerDay) {
            break;
          }
        }

        passSelectedTotal += eventCount;
        console.log(`[DiscoveryDataLoader] Day collected: ${dayRows.length} rows from ${eventCount} events (${warmupCount} warmup)`);

        // Calculate forward returns for this day
        for (let i = 0; i < dayRows.length; i++) {
          const forwardReturns = {};

          for (const horizon of self.forwardHorizons) {
            const futureIdx = i + horizon;

            if (futureIdx < dayMidPrices.length) {
              const currentPrice = dayMidPrices[i];
              const futurePrice = dayMidPrices[futureIdx];
              forwardReturns[`h${horizon}`] = (futurePrice - currentPrice) / currentPrice;
            } else {
              forwardReturns[`h${horizon}`] = null;
            }
          }

          const row = dayRows[i];
          row.forwardReturns = forwardReturns;
          row.index = i;

          // Assign regime using consistent model
          const regimeVector = self.regimeFeatures.map(fname => row.features[fname] || 0);
          const prediction = regimeModel.predict(regimeVector);
          row.regime = prediction.cluster;
          row.regimeConfidence = prediction.confidence;

          yield row;  // ← Stream row
          passEmittedTotal++;
        }

        console.log(`[DiscoveryDataLoader] Day streamed: ${dayRows.length} rows yielded`);
      }
      } finally {
        if (isTradeStream) {
          console.log(
            `[TradeAdapter] step1_applied=${tradeAdapterStats.step1_applied} ` +
            `day_collect_applied=${tradeAdapterStats.day_collect_applied} ` +
            `px_missing_passthrough=${tradeAdapterStats.px_missing_passthrough}`
          );
          iteratorFactory.metadata.tradeAdapter = {
            step1_applied: tradeAdapterStats.step1_applied,
            day_collect_applied: tradeAdapterStats.day_collect_applied,
            px_missing_passthrough: tradeAdapterStats.px_missing_passthrough
          };
        }

        if (smokeCapEnabled) {
          if (passSelectedTotal > smokeSelectedMax) smokeSelectedMax = passSelectedTotal;
          if (passEmittedTotal > smokeEmittedMax) smokeEmittedMax = passEmittedTotal;

          iteratorFactory.metadata.rowCount = smokeEmittedMax;
          iteratorFactory.metadata.smokeCap = {
            enabled: true,
            maxRowsPerDay,
            slice: smokeSlice,
            rows_selected_total: smokeSelectedMax,
            rows_emitted_total: smokeEmittedMax
          };
        }
      }
    };

    // Attach metadata to factory function
    // Note: rowCount will be accurate only after iterator consumed
    iteratorFactory.metadata = {
      symbol,
      rowCount: null, // Unknown until iterator consumed
      dateRange: {
        start: firstTimestamp,
        end: null // Unknown until last file processed
      },
      filesLoaded: files.length,
      horizonsUsed: this.forwardHorizons,
      regimeK: this.regimeK,
      regimeModel,
      featureNames: this.featureNames,
      ...(isTradeStream
        ? {
          tradeAdapter: {
            step1_applied: tradeAdapterStats.step1_applied,
            day_collect_applied: tradeAdapterStats.day_collect_applied,
            px_missing_passthrough: tradeAdapterStats.px_missing_passthrough
          }
        }
        : {}),
      ...(isTradeStream
        ? {
          tradeDiag: {
            total_rows_seen: tradeTotalRowsSeen,
            rows_with_required_fields: tradeRowsWithRequiredFields,
            featureable_rows_after_warmup: tradeFeatureableRowsAfterWarmup,
            coverage: tradeTotalRowsSeen > 0 ? tradeFeatureableRowsAfterWarmup / tradeTotalRowsSeen : 0
          }
        }
        : {}),
      ...(smokeCapEnabled
        ? {
          smokeCap: {
            enabled: true,
            maxRowsPerDay,
            slice: smokeSlice,
            rows_selected_total: 0,
            rows_emitted_total: 0
          }
        }
        : {})
    };

    console.log(`[DiscoveryDataLoader] Iterator factory ready for streaming`);

    return iteratorFactory;
  }
}
