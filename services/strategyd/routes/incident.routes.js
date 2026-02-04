/**
 * Incident Response Routes for strategyd
 *
 * GET  /v1/incident/active          - Get active incidents
 * GET  /v1/incident/history         - Get incident history
 * GET  /v1/incident/summary         - Get incident summary stats
 * GET  /v1/incident/:id             - Get incident details
 * GET  /v1/incident/:id/timeline    - Get incident timeline
 * POST /v1/incident/:id/ack         - Acknowledge incident
 * POST /v1/incident/:id/resolve     - Resolve incident
 * POST /v1/incident/:id/note        - Add note to incident
 * POST /v1/incident/:id/runbook     - Trigger runbook for incident
 * POST /v1/incident/process-alert   - Process an alert (auto-response)
 * GET  /v1/incident/responder/state - Get auto-responder state
 * POST /v1/incident/responder/enable - Enable/disable auto-responder
 */

import { getIncidentManager, getAutoResponder, INCIDENT_STATES, SEVERITY_LEVELS } from '../../../core/incident/index.js';

export default async function incidentRoutes(fastify, options) {

  /**
   * GET /v1/incident/active - Get active (non-resolved) incidents
   */
  fastify.get('/v1/incident/active', async (request, reply) => {
    const manager = getIncidentManager();
    const incidents = await manager.getActive();

    return {
      count: incidents.length,
      incidents: incidents.map(formatIncident)
    };
  });

  /**
   * GET /v1/incident/history - Get incident history
   */
  fastify.get('/v1/incident/history', async (request, reply) => {
    const { state, severity, limit, since } = request.query;
    const manager = getIncidentManager();

    const incidents = await manager.getHistory({
      state,
      severity,
      limit: parseInt(limit) || 50,
      since
    });

    return {
      count: incidents.length,
      incidents: incidents.map(formatIncident)
    };
  });

  /**
   * GET /v1/incident/summary - Get incident summary statistics
   */
  fastify.get('/v1/incident/summary', async (request, reply) => {
    const manager = getIncidentManager();
    return await manager.getSummary();
  });

  /**
   * GET /v1/incident/:id - Get incident details
   */
  fastify.get('/v1/incident/:id', async (request, reply) => {
    const { id } = request.params;
    const manager = getIncidentManager();

    const incident = await manager.get(id);
    if (!incident) {
      return reply.code(404).send({
        error: 'INCIDENT_NOT_FOUND',
        message: `Incident ${id} not found`
      });
    }

    return formatIncidentFull(incident);
  });

  /**
   * GET /v1/incident/:id/timeline - Get incident timeline
   */
  fastify.get('/v1/incident/:id/timeline', async (request, reply) => {
    const { id } = request.params;
    const manager = getIncidentManager();

    try {
      const timeline = await manager.getTimeline(id);
      return {
        incidentId: id,
        events: timeline
      };
    } catch (err) {
      return reply.code(404).send({
        error: 'INCIDENT_NOT_FOUND',
        message: err.message
      });
    }
  });

  /**
   * POST /v1/incident/:id/ack - Acknowledge incident
   */
  fastify.post('/v1/incident/:id/ack', async (request, reply) => {
    const { id } = request.params;
    const { by, note } = request.body || {};

    if (!by) {
      return reply.code(400).send({
        error: 'MISSING_FIELD',
        message: 'Field "by" is required'
      });
    }

    const manager = getIncidentManager();

    try {
      const incident = await manager.acknowledge(id, { by, note });
      return {
        success: true,
        incident: formatIncident(incident)
      };
    } catch (err) {
      return reply.code(400).send({
        error: 'ACK_FAILED',
        message: err.message
      });
    }
  });

  /**
   * POST /v1/incident/:id/resolve - Resolve incident
   */
  fastify.post('/v1/incident/:id/resolve', async (request, reply) => {
    const { id } = request.params;
    const { by, rootCause, note } = request.body || {};

    if (!by) {
      return reply.code(400).send({
        error: 'MISSING_FIELD',
        message: 'Field "by" is required'
      });
    }

    const manager = getIncidentManager();

    try {
      const incident = await manager.resolve(id, { by, rootCause, note });
      return {
        success: true,
        incident: formatIncident(incident)
      };
    } catch (err) {
      return reply.code(400).send({
        error: 'RESOLVE_FAILED',
        message: err.message
      });
    }
  });

  /**
   * POST /v1/incident/:id/note - Add note to incident
   */
  fastify.post('/v1/incident/:id/note', async (request, reply) => {
    const { id } = request.params;
    const { by, note } = request.body || {};

    if (!by || !note) {
      return reply.code(400).send({
        error: 'MISSING_FIELD',
        message: 'Fields "by" and "note" are required'
      });
    }

    const manager = getIncidentManager();

    try {
      const incident = await manager.addNote(id, { by, note });
      return {
        success: true,
        incident: formatIncident(incident)
      };
    } catch (err) {
      return reply.code(400).send({
        error: 'NOTE_FAILED',
        message: err.message
      });
    }
  });

  /**
   * POST /v1/incident/:id/runbook - Trigger runbook for incident
   */
  fastify.post('/v1/incident/:id/runbook', async (request, reply) => {
    const { id } = request.params;
    const { runbook, variables, dryRun } = request.body || {};

    if (!runbook) {
      return reply.code(400).send({
        error: 'MISSING_FIELD',
        message: 'Field "runbook" is required'
      });
    }

    const responder = getAutoResponder();
    if (dryRun) {
      responder.setDryRun(true);
    }

    try {
      const result = await responder.triggerManualRunbook(id, runbook, variables || {});
      return {
        success: true,
        execution: result
      };
    } catch (err) {
      return reply.code(400).send({
        error: 'RUNBOOK_FAILED',
        message: err.message
      });
    } finally {
      if (dryRun) {
        responder.setDryRun(false);
      }
    }
  });

  /**
   * POST /v1/incident/process-alert - Process an alert (auto-response)
   */
  fastify.post('/v1/incident/process-alert', async (request, reply) => {
    const alert = request.body;

    if (!alert || !alert.type) {
      return reply.code(400).send({
        error: 'INVALID_ALERT',
        message: 'Alert must have a "type" field'
      });
    }

    const responder = getAutoResponder();
    const result = await responder.processAlert(alert);

    return result;
  });

  /**
   * GET /v1/incident/responder/state - Get auto-responder state
   */
  fastify.get('/v1/incident/responder/state', async (request, reply) => {
    const responder = getAutoResponder();
    return responder.getState();
  });

  /**
   * GET /v1/incident/responder/rules - Get response rules
   */
  fastify.get('/v1/incident/responder/rules', async (request, reply) => {
    const responder = getAutoResponder();
    return {
      rules: responder.getRules()
    };
  });

  /**
   * POST /v1/incident/responder/enable - Enable/disable auto-responder
   */
  fastify.post('/v1/incident/responder/enable', async (request, reply) => {
    const { enabled } = request.body || {};

    if (typeof enabled !== 'boolean') {
      return reply.code(400).send({
        error: 'INVALID_FIELD',
        message: 'Field "enabled" must be a boolean'
      });
    }

    const responder = getAutoResponder();
    responder.setEnabled(enabled);

    return {
      success: true,
      enabled,
      state: responder.getState()
    };
  });

  /**
   * POST /v1/incident/create - Manually create an incident
   */
  fastify.post('/v1/incident/create', async (request, reply) => {
    const { severity, title, description, source } = request.body || {};

    if (!title) {
      return reply.code(400).send({
        error: 'MISSING_FIELD',
        message: 'Field "title" is required'
      });
    }

    const manager = getIncidentManager();
    const incident = await manager.createIncident({
      severity: severity || SEVERITY_LEVELS.WARNING,
      title,
      description: description || '',
      source: source || 'manual'
    });

    return {
      success: true,
      incident: formatIncident(incident)
    };
  });

}

/**
 * Format incident for API response (summary view)
 */
function formatIncident(incident) {
  return {
    id: incident.id,
    state: incident.state,
    severity: incident.severity,
    title: incident.title,
    createdAt: incident.createdAt,
    updatedAt: incident.updatedAt,
    acknowledgedAt: incident.acknowledgedAt,
    acknowledgedBy: incident.acknowledgedBy,
    resolvedAt: incident.resolvedAt,
    resolvedBy: incident.resolvedBy,
    notesCount: incident.notes?.length || 0,
    runbookCount: incident.runbookExecutions?.length || 0
  };
}

/**
 * Format incident for API response (full view)
 */
function formatIncidentFull(incident) {
  return {
    ...formatIncident(incident),
    description: incident.description,
    source: incident.source,
    alertId: incident.alertId,
    rootCause: incident.rootCause,
    notes: incident.notes,
    runbookExecutions: incident.runbookExecutions,
    timeline: incident.timeline
  };
}
