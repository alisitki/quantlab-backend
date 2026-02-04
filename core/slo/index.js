/**
 * SLO Module
 *
 * Service Level Objectives monitoring for QuantLab.
 * Provides definitions, calculations, and alerting.
 *
 * @example
 * import { SLOCalculator, SLOAlerter, SLO_DEFINITIONS } from './core/slo/index.js';
 *
 * const calculator = new SLOCalculator(metricsProvider);
 * const status = calculator.evaluate('exchange_availability');
 *
 * const alerter = new SLOAlerter(calculator, alertManager);
 * alerter.start();
 */

export {
  SLO_DEFINITIONS,
  getSLOsByTier,
  getAllSLOIds,
  getSLODefinition
} from './definitions.js';

export {
  SLOCalculator,
  SLO_STATUS
} from './SLOCalculator.js';

export {
  SLOAlerter,
  SLO_ALERT_TYPES
} from './SLOAlerter.js';
