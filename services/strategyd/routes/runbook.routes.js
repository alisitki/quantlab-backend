/**
 * Runbook Routes for strategyd
 *
 * POST /v1/runbook/execute     - Execute a runbook
 * GET  /v1/runbook/list        - List available runbooks
 * GET  /v1/runbook/status/:id  - Get execution status
 * GET  /v1/runbook/history     - Get execution history
 */

import fs from 'fs/promises';
import path from 'path';
import { RunbookExecutor } from '../../../core/runbook/index.js';

// Singleton executor instance
let executor = null;

// Execution history cache (in-memory, last 100)
const executionHistory = [];
const MAX_HISTORY = 100;

/**
 * Get or create executor instance
 */
function getExecutor() {
  if (!executor) {
    executor = new RunbookExecutor({
      templatesDir: path.resolve(process.cwd(), 'core/runbook/templates'),
      logPath: path.resolve(process.cwd(), 'logs/runbook.jsonl')
    });
  }
  return executor;
}

/**
 * Add execution to history
 */
function addToHistory(execution) {
  executionHistory.unshift({
    id: execution.id,
    runbook: execution.runbookName,
    status: execution.status,
    startedAt: execution.startedAt,
    finishedAt: execution.finishedAt,
    durationMs: execution.durationMs,
    dryRun: execution.dryRun,
    stepsTotal: execution.stepsTotal,
    stepsCompleted: execution.stepsCompleted,
    stepsFailed: execution.stepsFailed
  });

  // Trim history
  if (executionHistory.length > MAX_HISTORY) {
    executionHistory.pop();
  }
}

export default async function runbookRoutes(fastify, options) {

  /**
   * POST /v1/runbook/execute - Execute a runbook
   */
  fastify.post('/v1/runbook/execute', async (request, reply) => {
    const { runbook, dryRun = false, variables = {} } = request.body || {};

    if (!runbook) {
      return reply.code(400).send({
        error: 'MISSING_RUNBOOK',
        message: 'runbook parameter is required'
      });
    }

    const exec = getExecutor();

    try {
      // Load runbook
      const runbookObj = await exec.loadRunbook(runbook);

      // Execute
      const result = await exec.execute(runbookObj, { dryRun, variables });

      // Add to history
      addToHistory(result);

      return {
        execution_id: result.id,
        runbook: result.runbookName,
        status: result.status,
        dry_run: result.dryRun,
        started_at: result.startedAt,
        finished_at: result.finishedAt,
        duration_ms: result.durationMs,
        summary: {
          steps_total: result.stepsTotal,
          steps_completed: result.stepsCompleted,
          steps_failed: result.stepsFailed
        },
        steps: result.steps
      };

    } catch (err) {
      return reply.code(400).send({
        error: 'RUNBOOK_ERROR',
        message: err.message
      });
    }
  });

  /**
   * GET /v1/runbook/list - List available runbooks
   */
  fastify.get('/v1/runbook/list', async (request, reply) => {
    const exec = getExecutor();
    const runbooks = await exec.listRunbooks();

    // Load each runbook to get metadata
    const details = [];
    for (const name of runbooks) {
      try {
        const rb = await exec.loadRunbook(name);
        details.push({
          name,
          description: rb.description || '',
          schedule: rb.schedule || 'manual',
          steps_count: rb.steps?.length || 0
        });
      } catch {
        details.push({
          name,
          description: 'Error loading runbook',
          schedule: 'unknown',
          steps_count: 0
        });
      }
    }

    return {
      count: details.length,
      runbooks: details
    };
  });

  /**
   * GET /v1/runbook/status/:id - Get execution status
   */
  fastify.get('/v1/runbook/status/:id', async (request, reply) => {
    const { id } = request.params;

    // Check running executions first
    const exec = getExecutor();
    const running = exec.getExecutionStatus(id);
    if (running) {
      return {
        execution_id: running.id,
        runbook: running.runbookName,
        status: running.status,
        dry_run: running.dryRun,
        started_at: running.startedAt,
        steps_completed: running.steps.filter(s => s.status === 'success').length,
        steps_total: running.stepsTotal || 0,
        current_step: running.steps.length > 0 ? running.steps[running.steps.length - 1] : null
      };
    }

    // Check history
    const historical = executionHistory.find(e => e.id === id);
    if (historical) {
      return {
        execution_id: historical.id,
        runbook: historical.runbook,
        status: historical.status,
        dry_run: historical.dryRun,
        started_at: historical.startedAt,
        finished_at: historical.finishedAt,
        duration_ms: historical.durationMs,
        summary: {
          steps_total: historical.stepsTotal,
          steps_completed: historical.stepsCompleted,
          steps_failed: historical.stepsFailed
        }
      };
    }

    return reply.code(404).send({
      error: 'EXECUTION_NOT_FOUND',
      message: `Execution ${id} not found`
    });
  });

  /**
   * GET /v1/runbook/history - Get execution history
   */
  fastify.get('/v1/runbook/history', async (request, reply) => {
    const { limit = 20, runbook, status } = request.query;
    const limitNum = Math.min(parseInt(limit) || 20, MAX_HISTORY);

    let filtered = executionHistory;

    // Filter by runbook name
    if (runbook) {
      filtered = filtered.filter(e => e.runbook === runbook);
    }

    // Filter by status
    if (status) {
      filtered = filtered.filter(e => e.status === status);
    }

    const sliced = filtered.slice(0, limitNum);

    return {
      count: sliced.length,
      total: executionHistory.length,
      executions: sliced
    };
  });

  /**
   * GET /v1/runbook/:name - Get runbook details
   */
  fastify.get('/v1/runbook/:name', async (request, reply) => {
    const { name } = request.params;
    const exec = getExecutor();

    try {
      const runbook = await exec.loadRunbook(name);

      return {
        name: runbook.name,
        description: runbook.description || '',
        schedule: runbook.schedule || 'manual',
        steps: runbook.steps.map(s => ({
          name: s.name,
          task: s.task,
          onFailure: s.onFailure || 'continue',
          hasCondition: !!s.condition
        }))
      };
    } catch (err) {
      return reply.code(404).send({
        error: 'RUNBOOK_NOT_FOUND',
        message: err.message
      });
    }
  });

}
