/**
 * StrategyV1 - Feature-Driven, Regime-Aware Strategy
 *
 * Ana orchestrator:
 * - Dinamik feature seçimi (Feature Analysis raporundan)
 * - Regime mode switching (trade bloklama YOK)
 * - Alpha-weighted sinyal birleştirme
 *
 * Flow:
 * Event → Features → Regime → Mode Selection → Signals → Combine → Execute
 */

import { FeatureRegistry } from '../../features/FeatureRegistry.js';
import { RegimeModeSelector } from './decision/RegimeModeSelector.js';
import { SignalGenerator, SIGNAL_DIRECTION } from './decision/SignalGenerator.js';
import { Combiner, ACTION } from './decision/Combiner.js';
import { DEFAULT_CONFIG, mergeConfig } from './config.js';
import { DecisionLogger } from '../../ml/logging/DecisionLogger.js';
import { RegimeLogger } from '../../ml/logging/RegimeLogger.js';
import { SignalGate } from '../../decision/SignalGate.js';

/**
 * Position sizing modes
 */
const POSITION_SIZING = {
  FIXED: 'fixed',
  CONFIDENCE_SCALED: 'confidence_scaled',
  ALPHA_SCALED: 'alpha_scaled'
};

export class StrategyV1 {
  #config;
  #featureBuilder = null;
  #regimeModeSelector;
  #signalGenerator;
  #combiner;
  #signalGate = null;
  #decisionLogger = null;
  #regimeLogger = null;

  // State
  #position = 'FLAT';
  #lastMode = null;
  #lastTradeTime = null;
  #tradeCount = 0;
  #signalCount = 0;
  #warmupComplete = false;

  /**
   * @param {Object} userConfig - User configuration overrides
   */
  constructor(userConfig = {}) {
    this.#config = mergeConfig(DEFAULT_CONFIG, userConfig);

    // Initialize decision components
    this.#regimeModeSelector = new RegimeModeSelector(this.#config.regime);
    this.#signalGenerator = new SignalGenerator(this.#config.signals);
    this.#combiner = new Combiner(this.#config.combiner);

    // Initialize decision gating layer
    if (this.#config.gate?.enabled) {
      this.#signalGate = new SignalGate(this.#config.gate);
    }
  }

  /**
   * Called before replay/live starts
   * @param {Object} ctx - RunnerContext
   */
  async onStart(ctx) {
    const symbol = ctx.symbol || this.#config.symbol || 'btcusdt';

    ctx.logger.info('=== StrategyV1 (Feature-Driven, Regime-Aware) ===');
    ctx.logger.info(`Symbol: ${symbol}`);

    // 1. Load feature report for dynamic feature selection
    if (this.#config.featureReportPath) {
      try {
        const topFeatures = this.#signalGenerator.loadFromReport(
          this.#config.featureReportPath,
          this.#config.topFeatureCount,
          this.#config.minAlphaScore
        );
        ctx.logger.info(`Loaded ${topFeatures.length} top features from report:`, {
          features: topFeatures.map(f => `${f.name} (alpha: ${f.alphaScore.toFixed(3)})`)
        });
      } catch (err) {
        ctx.logger.warn(`Failed to load feature report: ${err.message}`);
        ctx.logger.warn('Using fallback feature set');
        this.#loadFallbackFeatures();
      }
    } else {
      ctx.logger.warn('No featureReportPath configured, using fallback features');
      this.#loadFallbackFeatures();
    }

    // 2. Build enabled features list (regime features always included)
    const dynamicFeatures = this.#signalGenerator.getTopFeatures().map(f => f.name);
    const enabledFeatures = [
      'mid_price', 'spread',
      'regime_volatility', 'regime_trend', 'regime_spread',
      ...dynamicFeatures
    ];

    // 3. Create feature builder
    this.#featureBuilder = FeatureRegistry.createFeatureBuilder(symbol, {
      enabledFeatures,
      ...this.#config.featureParams
    });

    ctx.logger.info(`Active features (${enabledFeatures.length}):`, enabledFeatures);
    ctx.logger.info(`Combiner mode: ${this.#config.combiner.mode}`);
    ctx.logger.info(`Min confidence: ${this.#config.execution.minConfidence}`);

    // Log gate configuration
    if (this.#signalGate) {
      const gateConfig = this.#signalGate.getConfig();
      ctx.logger.info('Decision Gate: ENABLED', {
        minSignalScore: gateConfig.minSignalScore,
        cooldownMs: gateConfig.cooldownMs,
        regimeTrendMin: gateConfig.regimeTrendMin,
        maxSpreadNormalized: gateConfig.maxSpreadNormalized
      });
    } else {
      ctx.logger.warn('Decision Gate: DISABLED (expect high trade frequency!)');
    }

    // 4. Initialize loggers if enabled
    if (this.#config.logging?.decisionLogging) {
      this.#decisionLogger = DecisionLogger;
      this.#decisionLogger.init({
        logPath: `logs/decisions_${symbol}.jsonl`,
        enabled: true
      });
    }

    if (this.#config.logging?.regimeLogging) {
      this.#regimeLogger = RegimeLogger;
      this.#regimeLogger.init({
        logPath: `logs/regimes_${symbol}.jsonl`,
        enabled: true
      });
    }

    // 5. Reset state
    this.#position = 'FLAT';
    this.#lastMode = null;
    this.#lastTradeTime = null;
    this.#tradeCount = 0;
    this.#signalCount = 0;
    this.#warmupComplete = false;

    // Reset gate stats if enabled
    if (this.#signalGate) {
      this.#signalGate.resetStats();
    }
  }

  /**
   * Load fallback features when report is not available
   */
  #loadFallbackFeatures() {
    // Use a sensible default set for signal generation
    const fallbackFeatures = [
      { name: 'roc', alphaScore: 0.4, labelCorrelation: 0.1 },
      { name: 'ema_slope', alphaScore: 0.35, labelCorrelation: 0.08 },
      { name: 'volatility', alphaScore: 0.3, labelCorrelation: -0.05 }
    ];

    this.#signalGenerator.loadFromConfig(fallbackFeatures);
  }

  /**
   * Process each BBO event
   * @param {Object} event
   * @param {Object} ctx - RunnerContext
   */
  async onEvent(event, ctx) {
    // 1. Extract features
    const features = this.#featureBuilder.onEvent(event);

    // Skip during warmup
    if (!features) {
      return;
    }

    if (!this.#warmupComplete) {
      this.#warmupComplete = true;
      if (ctx.logger?.info) {
        ctx.logger.info('Warmup complete, starting signal generation');
      }
    }

    // 2. Get regime values
    const regimes = {
      volatility: features.regime_volatility ?? 1,
      trend: features.regime_trend ?? 0,
      spread: features.regime_spread ?? 1
    };

    // 3. Select mode based on regime (NOT filter!)
    const mode = this.#regimeModeSelector.selectMode(regimes);

    // Log mode transitions
    if (mode.modeChanged && this.#lastMode) {
      const transition = this.#regimeModeSelector.describeTransition(this.#lastMode, mode.primary);
      ctx.logger.info(`Mode transition: ${transition.description}`);
    }
    this.#lastMode = mode.primary;

    // 4. Log regime state
    if (this.#regimeLogger?.isEnabled()) {
      this.#regimeLogger.logRegimeState({
        timestamp: event.ts,
        symbol: ctx.symbol,
        regimes
      });
    }

    // 5. Generate signals from top features
    const signalResult = this.#signalGenerator.generate(features, mode);

    if (signalResult.signals.length === 0) {
      return; // No signals generated
    }

    this.#signalCount++;

    // 6. Apply mode-based adjustments
    const adjustedSignals = this.#combiner.applyModeAdjustments(
      signalResult.signals,
      mode
    );

    // 7. Combine signals into decision
    const decision = this.#combiner.combine(adjustedSignals);

    // 8. Check spread delay
    if (mode.combined.executionDelay) {
      const currentSpread = features.spread;
      if (currentSpread > this.#config.execution.spreadThreshold) {
        if (ctx.logger?.info) {
          ctx.logger.info('Spread delay active', {
            spread: currentSpread,
            threshold: this.#config.execution.spreadThreshold
          });
        }
        return; // Wait for better spread
      }
    }

    // 9. Execute if actionable
    if (decision.action !== ACTION.HOLD &&
        decision.confidence >= this.#config.execution.minConfidence) {

      // Apply decision gate (if enabled)
      if (this.#signalGate) {
        const gateResult = this.#signalGate.evaluate({
          signalScore: decision.confidence,
          features,
          regime: regimes,
          mode,
          lastTradeTime: this.#lastTradeTime,
          now: event.ts_event
        });

        if (!gateResult.allow) {
          // Trade blocked by gate
          if (ctx.logger?.debug) {
            ctx.logger.debug(`Gate blocked: ${gateResult.reason}`);
          }
          return;
        }
      }

      const quantity = this.#calculateQuantity(decision, mode, ctx);

      if (ctx.placeOrder) {
        const symbol = event.symbol || this.#config.symbol.toUpperCase();
        ctx.placeOrder({
          symbol,
          side: decision.action === ACTION.LONG ? 'BUY' : 'SELL',
          type: 'MARKET',
          qty: quantity,
          ts_event: event.ts_event
        });

        this.#tradeCount++;
        this.#lastTradeTime = event.ts_event;  // Track last trade time
        ctx.logger.info(`Trade #${this.#tradeCount}: ${decision.action}`, {
          confidence: decision.confidence.toFixed(3),
          quantity,
          mode: mode.primary,
          reason: decision.reason
        });
      }
    }

    // 10. Log decision
    if (this.#decisionLogger?.isEnabled()) {
      this.#decisionLogger.logDecision({
        timestamp: event.ts,
        symbol: ctx.symbol,
        features,
        prediction: decision.action === ACTION.LONG ? 1 :
                    decision.action === ACTION.SHORT ? -1 : 0,
        confidence: decision.confidence,
        metadata: {
          mode: mode.primary,
          regimes,
          signalCount: signalResult.signals.length,
          action: decision.action,
          reason: decision.reason
        }
      });
    }
  }

  /**
   * Calculate position size
   * @param {Object} decision
   * @param {Object} mode
   * @param {Object} ctx
   * @returns {number}
   */
  #calculateQuantity(decision, mode, ctx) {
    const { baseQuantity, maxQuantity, positionSizing } = this.#config.execution;
    const modeScale = mode.combined.positionScale || 1.0;

    let quantity = baseQuantity;

    switch (positionSizing) {
      case POSITION_SIZING.CONFIDENCE_SCALED:
        quantity = baseQuantity * decision.confidence;
        break;

      case POSITION_SIZING.ALPHA_SCALED:
        // Scale by average alpha score of contributing signals
        const avgAlpha = this.#signalGenerator.getTopFeatures()
          .reduce((sum, f) => sum + f.alphaScore, 0) /
          Math.max(1, this.#signalGenerator.getTopFeatures().length);
        quantity = baseQuantity * avgAlpha * decision.confidence;
        break;

      case POSITION_SIZING.FIXED:
      default:
        quantity = baseQuantity;
    }

    // Apply mode scaling
    quantity *= modeScale;

    // Clamp to max
    return Math.min(quantity, maxQuantity);
  }

  /**
   * Called after replay/live ends
   * @param {Object} ctx
   */
  async onEnd(ctx) {
    // Flush loggers
    this.#decisionLogger?.flush();
    this.#regimeLogger?.flush();

    // Log summary
    ctx.logger.info('=== StrategyV1 Summary ===');
    ctx.logger.info(`Total trades: ${this.#tradeCount}`);
    ctx.logger.info(`Total signals: ${this.#signalCount}`);
    ctx.logger.info(`Active features: ${this.#signalGenerator.getTopFeatures().map(f => f.name).join(', ')}`);

    // Log gate statistics
    if (this.#signalGate) {
      const gateStats = this.#signalGate.getStats();
      ctx.logger.info('=== Decision Gate Statistics ===');
      ctx.logger.info(`Total evaluations: ${gateStats.total}`);
      ctx.logger.info(`Passed: ${gateStats.passed} (${(gateStats.passRate * 100).toFixed(1)}%)`);
      ctx.logger.info(`Blocked: ${gateStats.blocked}`);

      if (Object.keys(gateStats.blockReasons).length > 0) {
        ctx.logger.info('Block reasons:', gateStats.blockReasons);
      }
    }

    // Reset
    this.#featureBuilder?.reset();
    this.#regimeModeSelector.reset();
    this.#signalGenerator.reset();
  }

  /**
   * Get current strategy state
   * @returns {Object}
   */
  getState() {
    return {
      position: this.#position,
      mode: this.#lastMode,
      lastTradeTime: this.#lastTradeTime,
      tradeCount: this.#tradeCount,
      signalCount: this.#signalCount,
      warmupComplete: this.#warmupComplete,
      topFeatures: this.#signalGenerator.getTopFeatures(),
      gateStats: this.#signalGate ? this.#signalGate.getStats() : null
    };
  }

  /**
   * Get configuration
   * @returns {Object}
   */
  getConfig() {
    return { ...this.#config };
  }
}

export default StrategyV1;
