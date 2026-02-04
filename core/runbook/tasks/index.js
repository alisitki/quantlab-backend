/**
 * Task Registry
 *
 * Central registry of all runbook task types.
 * Each task implements: execute(params, context) => { success, result?, error? }
 */

import { HealthCheckTask } from './HealthCheckTask.js';
import { RestartServiceTask } from './RestartServiceTask.js';
import { RunScriptTask } from './RunScriptTask.js';
import { HttpCallTask } from './HttpCallTask.js';
import { AlertTask } from './AlertTask.js';
import { WaitTask } from './WaitTask.js';

/**
 * Task registry mapping task names to implementations
 */
export const TASK_REGISTRY = {
  'health-check': HealthCheckTask,
  'restart-service': RestartServiceTask,
  'run-script': RunScriptTask,
  'http-call': HttpCallTask,
  'alert': AlertTask,
  'wait': WaitTask
};

/**
 * Get task implementation by name
 * @param {string} taskName - Task type name
 * @returns {Object|null} Task implementation or null
 */
export function getTask(taskName) {
  return TASK_REGISTRY[taskName] || null;
}

/**
 * List all available task types
 * @returns {string[]} Array of task names
 */
export function listTaskTypes() {
  return Object.keys(TASK_REGISTRY);
}

/**
 * Check if task type exists
 * @param {string} taskName - Task type name
 * @returns {boolean}
 */
export function hasTask(taskName) {
  return taskName in TASK_REGISTRY;
}
