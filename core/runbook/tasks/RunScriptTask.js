/**
 * Run Script Task
 *
 * Executes a Node.js script.
 *
 * @param {Object} params
 * @param {string} params.script - Script path (relative to project root)
 * @param {string[]} params.args - Script arguments
 * @param {number} params.timeout - Timeout in ms (default: 60000)
 * @param {Object} params.env - Additional environment variables
 */

import { spawn } from 'child_process';
import path from 'path';

export const RunScriptTask = {
  name: 'run-script',
  description: 'Execute a Node.js script',

  async execute(params, context) {
    const {
      script,
      args = [],
      timeout = 60000,
      env = {}
    } = params;

    if (!script) {
      return { success: false, error: 'Missing required param: script' };
    }

    // Dry-run mode
    if (context.dryRun) {
      return {
        success: true,
        result: {
          script,
          args,
          dryRun: true,
          message: `Would execute: node ${script} ${args.join(' ')}`
        }
      };
    }

    const startTime = Date.now();
    const projectRoot = process.cwd();
    const scriptPath = path.resolve(projectRoot, script);

    return new Promise((resolve) => {
      const proc = spawn('node', [scriptPath, ...args], {
        cwd: projectRoot,
        env: { ...process.env, ...env },
        stdio: 'pipe'
      });

      let stdout = '';
      let stderr = '';
      let timedOut = false;

      const timeoutId = setTimeout(() => {
        timedOut = true;
        proc.kill('SIGTERM');
      }, timeout);

      proc.stdout?.on('data', d => { stdout += d; });
      proc.stderr?.on('data', d => { stderr += d; });

      proc.on('close', code => {
        clearTimeout(timeoutId);
        const durationMs = Date.now() - startTime;

        if (timedOut) {
          resolve({
            success: false,
            result: { script, durationMs, timedOut: true },
            error: `Script timed out after ${timeout}ms`
          });
          return;
        }

        if (code === 0) {
          resolve({
            success: true,
            result: {
              script,
              exitCode: code,
              durationMs,
              stdout: stdout.slice(-1000), // Last 1000 chars
              stderr: stderr.slice(-500)
            }
          });
        } else {
          resolve({
            success: false,
            result: { script, exitCode: code, durationMs },
            error: stderr || `Script exited with code ${code}`
          });
        }
      });

      proc.on('error', err => {
        clearTimeout(timeoutId);
        resolve({
          success: false,
          result: { script },
          error: err.message
        });
      });
    });
  }
};
