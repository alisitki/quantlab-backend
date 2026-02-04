/**
 * Incident Response Module
 *
 * Exports:
 * - IncidentManager: Incident lifecycle management
 * - AutoResponder: Automatic incident response
 * - INCIDENT_STATES: State enum
 * - SEVERITY_LEVELS: Severity enum
 */

export {
  IncidentManager,
  getIncidentManager,
  INCIDENT_STATES,
  SEVERITY_LEVELS
} from './IncidentManager.js';

export {
  AutoResponder,
  getAutoResponder
} from './AutoResponder.js';
