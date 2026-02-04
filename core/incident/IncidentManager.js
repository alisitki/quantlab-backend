/**
 * IncidentManager: Incident lifecycle management.
 *
 * States: OPEN -> ACKNOWLEDGED -> RESOLVED
 *
 * Features:
 * - Incident creation from alerts
 * - Acknowledgment with optional notes
 * - Resolution with root cause
 * - Timeline tracking
 * - JSONL persistence
 */
import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';

/**
 * Incident states
 */
export const INCIDENT_STATES = {
  OPEN: 'OPEN',
  ACKNOWLEDGED: 'ACKNOWLEDGED',
  RESOLVED: 'RESOLVED'
};

/**
 * Incident severity levels
 */
export const SEVERITY_LEVELS = {
  CRITICAL: 'critical',
  ERROR: 'error',
  WARNING: 'warning',
  INFO: 'info'
};

export class IncidentManager {
  #logPath;
  #incidents; // In-memory cache: Map<id, incident>
  #loaded;

  constructor(options = {}) {
    this.#logPath = options.logPath || path.resolve(process.cwd(), 'logs/incidents.jsonl');
    this.#incidents = new Map();
    this.#loaded = false;
  }

  /**
   * Load incidents from JSONL log.
   */
  async load() {
    if (this.#loaded) return;

    try {
      const content = await fs.readFile(this.#logPath, 'utf8');
      const lines = content.trim().split('\n').filter(Boolean);

      for (const line of lines) {
        try {
          const event = JSON.parse(line);
          this.#applyEvent(event);
        } catch {
          // Skip malformed lines
        }
      }
    } catch (err) {
      if (err.code !== 'ENOENT') {
        console.warn(`[IncidentManager] Failed to load incidents: ${err.message}`);
      }
    }

    this.#loaded = true;
  }

  /**
   * Apply an event to in-memory state.
   */
  #applyEvent(event) {
    const { type, incidentId, data, timestamp } = event;

    switch (type) {
      case 'INCIDENT_CREATED': {
        this.#incidents.set(incidentId, {
          id: incidentId,
          state: INCIDENT_STATES.OPEN,
          severity: data.severity,
          title: data.title,
          description: data.description,
          source: data.source,
          alertId: data.alertId,
          createdAt: timestamp,
          updatedAt: timestamp,
          acknowledgedAt: null,
          acknowledgedBy: null,
          resolvedAt: null,
          resolvedBy: null,
          rootCause: null,
          notes: [],
          timeline: [{
            type: 'CREATED',
            timestamp,
            data
          }],
          runbookExecutions: []
        });
        break;
      }

      case 'INCIDENT_ACKNOWLEDGED': {
        const incident = this.#incidents.get(incidentId);
        if (incident) {
          incident.state = INCIDENT_STATES.ACKNOWLEDGED;
          incident.acknowledgedAt = timestamp;
          incident.acknowledgedBy = data.by;
          incident.updatedAt = timestamp;
          if (data.note) {
            incident.notes.push({ timestamp, by: data.by, note: data.note });
          }
          incident.timeline.push({ type: 'ACKNOWLEDGED', timestamp, data });
        }
        break;
      }

      case 'INCIDENT_RESOLVED': {
        const incident = this.#incidents.get(incidentId);
        if (incident) {
          incident.state = INCIDENT_STATES.RESOLVED;
          incident.resolvedAt = timestamp;
          incident.resolvedBy = data.by;
          incident.rootCause = data.rootCause;
          incident.updatedAt = timestamp;
          if (data.note) {
            incident.notes.push({ timestamp, by: data.by, note: data.note });
          }
          incident.timeline.push({ type: 'RESOLVED', timestamp, data });
        }
        break;
      }

      case 'INCIDENT_NOTE_ADDED': {
        const incident = this.#incidents.get(incidentId);
        if (incident) {
          incident.notes.push({ timestamp, by: data.by, note: data.note });
          incident.updatedAt = timestamp;
          incident.timeline.push({ type: 'NOTE_ADDED', timestamp, data });
        }
        break;
      }

      case 'RUNBOOK_TRIGGERED': {
        const incident = this.#incidents.get(incidentId);
        if (incident) {
          incident.runbookExecutions.push({
            runbook: data.runbook,
            executionId: data.executionId,
            triggeredAt: timestamp,
            auto: data.auto ?? true
          });
          incident.timeline.push({ type: 'RUNBOOK_TRIGGERED', timestamp, data });
        }
        break;
      }

      case 'RUNBOOK_COMPLETED': {
        const incident = this.#incidents.get(incidentId);
        if (incident) {
          const exec = incident.runbookExecutions.find(e => e.executionId === data.executionId);
          if (exec) {
            exec.completedAt = timestamp;
            exec.status = data.status;
            exec.stepsCompleted = data.stepsCompleted;
            exec.stepsFailed = data.stepsFailed;
          }
          incident.timeline.push({ type: 'RUNBOOK_COMPLETED', timestamp, data });
        }
        break;
      }
    }
  }

  /**
   * Persist an event to JSONL log.
   */
  async #persistEvent(event) {
    const dir = path.dirname(this.#logPath);
    await fs.mkdir(dir, { recursive: true });
    await fs.appendFile(this.#logPath, JSON.stringify(event) + '\n');
  }

  /**
   * Create a new incident from an alert.
   * @param {Object} params - Incident parameters
   * @returns {Promise<Object>} Created incident
   */
  async createIncident(params) {
    await this.load();

    const { severity, title, description, source, alertId } = params;
    const incidentId = `inc-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
    const timestamp = new Date().toISOString();

    const event = {
      type: 'INCIDENT_CREATED',
      incidentId,
      timestamp,
      data: { severity, title, description, source, alertId }
    };

    this.#applyEvent(event);
    await this.#persistEvent(event);

    console.log(`[IncidentManager] Created incident ${incidentId}: ${title}`);
    return this.#incidents.get(incidentId);
  }

  /**
   * Acknowledge an incident.
   * @param {string} incidentId - Incident ID
   * @param {Object} params - Acknowledgment params
   * @returns {Promise<Object>} Updated incident
   */
  async acknowledge(incidentId, params) {
    await this.load();

    const incident = this.#incidents.get(incidentId);
    if (!incident) {
      throw new Error(`Incident ${incidentId} not found`);
    }

    if (incident.state === INCIDENT_STATES.RESOLVED) {
      throw new Error(`Incident ${incidentId} is already resolved`);
    }

    const { by, note } = params;
    const timestamp = new Date().toISOString();

    const event = {
      type: 'INCIDENT_ACKNOWLEDGED',
      incidentId,
      timestamp,
      data: { by, note }
    };

    this.#applyEvent(event);
    await this.#persistEvent(event);

    console.log(`[IncidentManager] Incident ${incidentId} acknowledged by ${by}`);
    return incident;
  }

  /**
   * Resolve an incident.
   * @param {string} incidentId - Incident ID
   * @param {Object} params - Resolution params
   * @returns {Promise<Object>} Updated incident
   */
  async resolve(incidentId, params) {
    await this.load();

    const incident = this.#incidents.get(incidentId);
    if (!incident) {
      throw new Error(`Incident ${incidentId} not found`);
    }

    if (incident.state === INCIDENT_STATES.RESOLVED) {
      throw new Error(`Incident ${incidentId} is already resolved`);
    }

    const { by, rootCause, note } = params;
    const timestamp = new Date().toISOString();

    const event = {
      type: 'INCIDENT_RESOLVED',
      incidentId,
      timestamp,
      data: { by, rootCause, note }
    };

    this.#applyEvent(event);
    await this.#persistEvent(event);

    console.log(`[IncidentManager] Incident ${incidentId} resolved by ${by}`);
    return incident;
  }

  /**
   * Add a note to an incident.
   * @param {string} incidentId - Incident ID
   * @param {Object} params - Note params
   * @returns {Promise<Object>} Updated incident
   */
  async addNote(incidentId, params) {
    await this.load();

    const incident = this.#incidents.get(incidentId);
    if (!incident) {
      throw new Error(`Incident ${incidentId} not found`);
    }

    const { by, note } = params;
    const timestamp = new Date().toISOString();

    const event = {
      type: 'INCIDENT_NOTE_ADDED',
      incidentId,
      timestamp,
      data: { by, note }
    };

    this.#applyEvent(event);
    await this.#persistEvent(event);

    return incident;
  }

  /**
   * Record a runbook trigger for an incident.
   * @param {string} incidentId - Incident ID
   * @param {Object} params - Runbook params
   */
  async recordRunbookTrigger(incidentId, params) {
    await this.load();

    const { runbook, executionId, auto } = params;
    const timestamp = new Date().toISOString();

    const event = {
      type: 'RUNBOOK_TRIGGERED',
      incidentId,
      timestamp,
      data: { runbook, executionId, auto }
    };

    this.#applyEvent(event);
    await this.#persistEvent(event);
  }

  /**
   * Record a runbook completion for an incident.
   * @param {string} incidentId - Incident ID
   * @param {Object} params - Completion params
   */
  async recordRunbookCompletion(incidentId, params) {
    await this.load();

    const { executionId, status, stepsCompleted, stepsFailed } = params;
    const timestamp = new Date().toISOString();

    const event = {
      type: 'RUNBOOK_COMPLETED',
      incidentId,
      timestamp,
      data: { executionId, status, stepsCompleted, stepsFailed }
    };

    this.#applyEvent(event);
    await this.#persistEvent(event);
  }

  /**
   * Get an incident by ID.
   * @param {string} incidentId - Incident ID
   * @returns {Promise<Object|null>} Incident or null
   */
  async get(incidentId) {
    await this.load();
    return this.#incidents.get(incidentId) || null;
  }

  /**
   * Get all active (non-resolved) incidents.
   * @returns {Promise<Array>} Active incidents
   */
  async getActive() {
    await this.load();
    return Array.from(this.#incidents.values())
      .filter(i => i.state !== INCIDENT_STATES.RESOLVED)
      .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  }

  /**
   * Get incident history with filters.
   * @param {Object} options - Filter options
   * @returns {Promise<Array>} Incidents
   */
  async getHistory(options = {}) {
    await this.load();

    const { state, severity, limit = 50, since } = options;
    let incidents = Array.from(this.#incidents.values());

    if (state) {
      incidents = incidents.filter(i => i.state === state);
    }

    if (severity) {
      incidents = incidents.filter(i => i.severity === severity);
    }

    if (since) {
      const sinceDate = new Date(since);
      incidents = incidents.filter(i => new Date(i.createdAt) >= sinceDate);
    }

    // Sort by createdAt descending
    incidents.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    return incidents.slice(0, limit);
  }

  /**
   * Get incident timeline.
   * @param {string} incidentId - Incident ID
   * @returns {Promise<Array>} Timeline events
   */
  async getTimeline(incidentId) {
    await this.load();
    const incident = this.#incidents.get(incidentId);
    if (!incident) {
      throw new Error(`Incident ${incidentId} not found`);
    }
    return incident.timeline;
  }

  /**
   * Get summary statistics.
   * @returns {Promise<Object>} Summary
   */
  async getSummary() {
    await this.load();

    const incidents = Array.from(this.#incidents.values());
    const open = incidents.filter(i => i.state === INCIDENT_STATES.OPEN);
    const acknowledged = incidents.filter(i => i.state === INCIDENT_STATES.ACKNOWLEDGED);
    const resolved = incidents.filter(i => i.state === INCIDENT_STATES.RESOLVED);

    // Last 24h
    const last24h = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const created24h = incidents.filter(i => new Date(i.createdAt) >= last24h);
    const resolved24h = incidents.filter(i => i.resolvedAt && new Date(i.resolvedAt) >= last24h);

    // MTTR (Mean Time To Resolve) for resolved incidents
    const resolvedWithTimes = resolved.filter(i => i.createdAt && i.resolvedAt);
    let mttrMs = 0;
    if (resolvedWithTimes.length > 0) {
      const totalMs = resolvedWithTimes.reduce((sum, i) => {
        return sum + (new Date(i.resolvedAt) - new Date(i.createdAt));
      }, 0);
      mttrMs = totalMs / resolvedWithTimes.length;
    }

    return {
      total: incidents.length,
      byState: {
        open: open.length,
        acknowledged: acknowledged.length,
        resolved: resolved.length
      },
      bySeverity: {
        critical: incidents.filter(i => i.severity === SEVERITY_LEVELS.CRITICAL).length,
        error: incidents.filter(i => i.severity === SEVERITY_LEVELS.ERROR).length,
        warning: incidents.filter(i => i.severity === SEVERITY_LEVELS.WARNING).length
      },
      last24h: {
        created: created24h.length,
        resolved: resolved24h.length
      },
      mttrMs: Math.round(mttrMs),
      mttrMinutes: Math.round(mttrMs / 60000)
    };
  }
}

/**
 * Singleton instance
 */
let instance = null;
export function getIncidentManager() {
  if (!instance) {
    instance = new IncidentManager();
  }
  return instance;
}
