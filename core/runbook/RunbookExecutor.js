/**
 * Runbook Executor
 *
 * Executes YAML-defined runbooks with sequential/conditional task execution.
 * Supports dry-run mode, logging, and alert integration.
 *
 * @example
 * const executor = new RunbookExecutor({ alertManager });
 * const runbook = await executor.loadRunbook('daily-health-check.yaml');
 * const result = await executor.execute(runbook, { dryRun: true });
 */

import fs from 'fs/promises';
import path from 'path';
import { getTask, hasTask, listTaskTypes } from './tasks/index.js';

// Simple YAML parser (no external dependency)
// Supports: strings, numbers, booleans, arrays, objects
function parseYAML(content) {
  const lines = content.split('\n');
  const result = {};
  const stack = [{ obj: result, indent: -1 }];
  let currentArray = null;
  let currentArrayKey = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();

    // Skip empty lines and comments
    if (!trimmed || trimmed.startsWith('#')) continue;

    const indent = line.search(/\S/);
    const isArrayItem = trimmed.startsWith('- ');

    // Pop stack for outdented lines
    while (stack.length > 1 && indent <= stack[stack.length - 1].indent) {
      stack.pop();
      currentArray = null;
    }

    const current = stack[stack.length - 1].obj;

    if (isArrayItem) {
      const value = trimmed.slice(2).trim();

      if (!currentArray) {
        // Find the key this array belongs to (previous non-array line)
        currentArray = [];
        if (currentArrayKey && current) {
          current[currentArrayKey] = currentArray;
        }
      }

      if (value.includes(':')) {
        // Object in array
        const obj = {};
        const [key, val] = value.split(':').map(s => s.trim());
        if (val) {
          obj[key] = parseValue(val);
        }
        currentArray.push(obj);
        stack.push({ obj: obj, indent: indent + 2 });
      } else if (value) {
        currentArray.push(parseValue(value));
      }
    } else if (trimmed.includes(':')) {
      const colonIdx = trimmed.indexOf(':');
      const key = trimmed.slice(0, colonIdx).trim();
      const value = trimmed.slice(colonIdx + 1).trim();

      if (value) {
        current[key] = parseValue(value);
      } else {
        // Nested object or array coming
        current[key] = {};
        currentArrayKey = key;
        stack.push({ obj: current[key], indent: indent });
      }
    }
  }

  return result;
}

function parseValue(str) {
  if (str === 'true') return true;
  if (str === 'false') return false;
  if (str === 'null') return null;
  if (/^-?\d+$/.test(str)) return parseInt(str, 10);
  if (/^-?\d+\.\d+$/.test(str)) return parseFloat(str);
  // Remove quotes
  if ((str.startsWith('"') && str.endsWith('"')) ||
      (str.startsWith("'") && str.endsWith("'"))) {
    return str.slice(1, -1);
  }
  return str;
}

/**
 * Generate unique runbook execution ID
 */
function generateRunbookId() {
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const random = Math.random().toString(36).slice(2, 8);
  return `rb-${date}-${random}`;
}

export class RunbookExecutor {
  #alertManager;
  #templatesDir;
  #logPath;
  #runningExecutions;

  /**
   * @param {Object} options
   * @param {Object} options.alertManager - AlertManager instance
   * @param {string} options.templatesDir - Templates directory path
   * @param {string} options.logPath - Execution log path
   */
  constructor(options = {}) {
    this.#alertManager = options.alertManager || null;
    this.#templatesDir = options.templatesDir ||
      path.resolve(process.cwd(), 'core/runbook/templates');
    this.#logPath = options.logPath ||
      path.resolve(process.cwd(), 'logs/runbook.jsonl');
    this.#runningExecutions = new Map();
  }

  /**
   * Load runbook from YAML file
   * @param {string} nameOrPath - Runbook name or path
   * @returns {Promise<Object>} Parsed runbook
   */
  async loadRunbook(nameOrPath) {
    let filePath = nameOrPath;

    // If just a name, look in templates directory
    if (!nameOrPath.includes('/') && !nameOrPath.includes('\\')) {
      const name = nameOrPath.endsWith('.yaml') ? nameOrPath : `${nameOrPath}.yaml`;
      filePath = path.join(this.#templatesDir, name);
    }

    const content = await fs.readFile(filePath, 'utf-8');
    const runbook = parseYAML(content);

    // Validate runbook structure
    if (!runbook.name) {
      throw new Error('Runbook missing required field: name');
    }
    if (!runbook.steps || !Array.isArray(runbook.steps)) {
      throw new Error('Runbook missing required field: steps (array)');
    }

    // Validate steps
    for (let i = 0; i < runbook.steps.length; i++) {
      const step = runbook.steps[i];
      if (!step.name) {
        throw new Error(`Step ${i + 1} missing required field: name`);
      }
      if (!step.task) {
        throw new Error(`Step "${step.name}" missing required field: task`);
      }
      if (!hasTask(step.task)) {
        throw new Error(`Step "${step.name}" has unknown task type: ${step.task}. Available: ${listTaskTypes().join(', ')}`);
      }
    }

    return runbook;
  }

  /**
   * List available runbook templates
   * @returns {Promise<string[]>} Array of runbook names
   */
  async listRunbooks() {
    try {
      const files = await fs.readdir(this.#templatesDir);
      return files
        .filter(f => f.endsWith('.yaml'))
        .map(f => f.replace('.yaml', ''));
    } catch {
      return [];
    }
  }

  /**
   * Execute a runbook
   * @param {Object} runbook - Parsed runbook object
   * @param {Object} options - Execution options
   * @param {boolean} options.dryRun - If true, don't execute tasks
   * @param {Object} options.variables - Variables for template substitution
   * @returns {Promise<Object>} Execution result
   */
  async execute(runbook, options = {}) {
    const { dryRun = false, variables = {} } = options;

    const executionId = generateRunbookId();
    const startTime = Date.now();

    const execution = {
      id: executionId,
      runbookName: runbook.name,
      status: 'running',
      startedAt: new Date().toISOString(),
      dryRun,
      steps: [],
      failures: 0,
      variables
    };

    this.#runningExecutions.set(executionId, execution);

    const context = {
      dryRun,
      runbookId: executionId,
      runbookName: runbook.name,
      alertManager: this.#alertManager,
      variables,
      stepResults: {},
      failures: 0
    };

    try {
      for (let i = 0; i < runbook.steps.length; i++) {
        const step = runbook.steps[i];

        // Check condition
        if (step.condition && !this.#evaluateCondition(step.condition, context)) {
          execution.steps.push({
            name: step.name,
            task: step.task,
            status: 'skipped',
            reason: 'Condition not met'
          });
          continue;
        }

        // Execute task
        const taskImpl = getTask(step.task);
        const params = this.#substituteVariables(step.params || {}, context);

        const stepStart = Date.now();
        let result;

        try {
          result = await taskImpl.execute(params, context);
        } catch (err) {
          result = { success: false, error: err.message };
        }

        const stepDuration = Date.now() - stepStart;

        // Store result
        context.stepResults[step.name] = result;

        const stepRecord = {
          name: step.name,
          task: step.task,
          status: result.success ? 'success' : 'failed',
          durationMs: stepDuration,
          result: result.result,
          error: result.error
        };

        execution.steps.push(stepRecord);

        if (!result.success) {
          context.failures++;
          execution.failures++;

          if (step.onFailure === 'abort') {
            execution.status = 'aborted';
            break;
          }
        }
      }

      if (execution.status !== 'aborted') {
        execution.status = execution.failures > 0 ? 'completed_with_failures' : 'completed';
      }

    } catch (err) {
      execution.status = 'error';
      execution.error = err.message;
    }

    execution.finishedAt = new Date().toISOString();
    execution.durationMs = Date.now() - startTime;
    execution.stepsTotal = runbook.steps.length;
    execution.stepsCompleted = execution.steps.filter(s => s.status === 'success').length;
    execution.stepsFailed = execution.failures;

    this.#runningExecutions.delete(executionId);

    // Log execution
    await this.#logExecution(execution);

    return execution;
  }

  /**
   * Get status of a running execution
   * @param {string} executionId - Execution ID
   * @returns {Object|null} Execution status or null
   */
  getExecutionStatus(executionId) {
    return this.#runningExecutions.get(executionId) || null;
  }

  /**
   * Evaluate a condition string
   */
  #evaluateCondition(condition, context) {
    // Simple conditions: "{{ failures > 0 }}", "{{ stepResults.check_replayd.success }}"
    const match = condition.match(/\{\{\s*(.+?)\s*\}\}/);
    if (!match) return true;

    const expr = match[1];

    // Handle simple comparisons
    if (expr.includes('>') || expr.includes('<') || expr.includes('==')) {
      const parts = expr.split(/\s*(>|<|==|>=|<=|!=)\s*/);
      if (parts.length === 3) {
        const left = this.#resolveValue(parts[0].trim(), context);
        const op = parts[1];
        const right = this.#resolveValue(parts[2].trim(), context);

        switch (op) {
          case '>': return left > right;
          case '<': return left < right;
          case '>=': return left >= right;
          case '<=': return left <= right;
          case '==': return left == right;
          case '!=': return left != right;
        }
      }
    }

    // Handle boolean check
    return !!this.#resolveValue(expr, context);
  }

  /**
   * Resolve a value from context
   */
  #resolveValue(path, context) {
    // Number literal
    if (/^-?\d+(\.\d+)?$/.test(path)) {
      return parseFloat(path);
    }

    // Boolean literal
    if (path === 'true') return true;
    if (path === 'false') return false;

    // Context path (e.g., "failures", "stepResults.foo.success")
    const parts = path.split('.');
    let value = context;

    for (const part of parts) {
      if (value === undefined || value === null) return undefined;
      value = value[part];
    }

    return value;
  }

  /**
   * Substitute variables in params
   */
  #substituteVariables(params, context) {
    const result = {};

    for (const [key, value] of Object.entries(params)) {
      if (typeof value === 'string') {
        result[key] = value.replace(/\{\{\s*(.+?)\s*\}\}/g, (_, path) => {
          const resolved = this.#resolveValue(path, context);
          return resolved !== undefined ? String(resolved) : '';
        });
      } else if (typeof value === 'object' && value !== null) {
        result[key] = this.#substituteVariables(value, context);
      } else {
        result[key] = value;
      }
    }

    return result;
  }

  /**
   * Log execution to JSONL file
   */
  async #logExecution(execution) {
    try {
      const logDir = path.dirname(this.#logPath);
      await fs.mkdir(logDir, { recursive: true });

      const logEntry = JSON.stringify({
        runbook_id: execution.id,
        runbook_name: execution.runbookName,
        started_at: execution.startedAt,
        finished_at: execution.finishedAt,
        status: execution.status,
        dry_run: execution.dryRun,
        steps_total: execution.stepsTotal,
        steps_completed: execution.stepsCompleted,
        steps_failed: execution.stepsFailed,
        duration_ms: execution.durationMs,
        error: execution.error
      }) + '\n';

      await fs.appendFile(this.#logPath, logEntry);
    } catch (err) {
      console.error(`[RunbookExecutor] Failed to log execution: ${err.message}`);
    }
  }
}

// CLI support
if (process.argv[1]?.endsWith('RunbookExecutor.js')) {
  const args = process.argv.slice(2);
  const runbookArg = args.find(a => a.startsWith('--runbook='))?.split('=')[1] ||
                     args[args.indexOf('--runbook') + 1];
  const dryRun = args.includes('--dry-run');

  if (!runbookArg) {
    console.error('Usage: node RunbookExecutor.js --runbook <name> [--dry-run]');
    process.exit(1);
  }

  const executor = new RunbookExecutor();
  executor.loadRunbook(runbookArg)
    .then(runbook => executor.execute(runbook, { dryRun }))
    .then(result => {
      console.log(JSON.stringify(result, null, 2));
      process.exit(result.status === 'completed' ? 0 : 1);
    })
    .catch(err => {
      console.error(`Error: ${err.message}`);
      process.exit(1);
    });
}
