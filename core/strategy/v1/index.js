/**
 * StrategyV1 Module Exports
 */

// Main strategy
export { StrategyV1 } from './StrategyV1.js';
export { default } from './StrategyV1.js';

// Decision components
export { RegimeModeSelector, REGIME_LABELS } from './decision/RegimeModeSelector.js';
export { SignalGenerator, SIGNAL_DIRECTION } from './decision/SignalGenerator.js';
export { Combiner, ACTION, COMBINE_MODE } from './decision/Combiner.js';

// Configuration
export {
  DEFAULT_CONFIG,
  HIGH_FREQUENCY_CONFIG,
  QUALITY_CONFIG,
  AGGRESSIVE_CONFIG,
  CONSERVATIVE_CONFIG,
  getConfig,
  mergeConfig
} from './config.js';
