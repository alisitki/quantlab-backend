/**
 * Runbook Module
 *
 * YAML-based operational runbook automation for QuantLab.
 *
 * @example
 * import { RunbookExecutor } from './core/runbook/index.js';
 *
 * const executor = new RunbookExecutor({ alertManager });
 * const runbook = await executor.loadRunbook('daily-health-check');
 * const result = await executor.execute(runbook, { dryRun: true });
 */

export { RunbookExecutor } from './RunbookExecutor.js';

export {
  TASK_REGISTRY,
  getTask,
  listTaskTypes,
  hasTask
} from './tasks/index.js';

export { HealthCheckTask } from './tasks/HealthCheckTask.js';
export { RestartServiceTask } from './tasks/RestartServiceTask.js';
export { RunScriptTask } from './tasks/RunScriptTask.js';
export { HttpCallTask } from './tasks/HttpCallTask.js';
export { AlertTask } from './tasks/AlertTask.js';
export { WaitTask } from './tasks/WaitTask.js';
