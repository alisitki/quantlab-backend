/**
 * Restart Service Task
 *
 * Restarts a service via systemd or npm.
 *
 * @param {Object} params
 * @param {string} params.service - Service name
 * @param {string} params.type - 'systemd' or 'npm'
 * @param {string} params.unit - Systemd unit name (for systemd type)
 * @param {string} params.cwd - Working directory (for npm type)
 * @param {number} params.waitAfterMs - Wait after restart (default: 3000)
 */

import { spawn } from 'child_process';

export const RestartServiceTask = {
  name: 'restart-service',
  description: 'Restart a service via systemd or npm',

  /**
   * Execute service restart
   */
  async execute(params, context) {
    const {
      service,
      type = 'systemd',
      unit,
      cwd,
      waitAfterMs = 3000
    } = params;

    if (!service) {
      return { success: false, error: 'Missing required param: service' };
    }

    // Dry-run mode
    if (context.dryRun) {
      return {
        success: true,
        result: {
          service,
          type,
          action: 'restart',
          dryRun: true,
          message: `Would restart ${service} via ${type}`
        }
      };
    }

    try {
      if (type === 'systemd') {
        const unitName = unit || `quantlab-${service}.service`;
        await this.runCommand('sudo', ['systemctl', 'restart', unitName]);
      } else if (type === 'npm') {
        // For npm services, we can't easily restart without process manager
        // Log warning and skip
        return {
          success: true,
          result: {
            service,
            type,
            action: 'restart',
            skipped: true,
            message: 'NPM service restart requires manual intervention or PM2'
          }
        };
      } else {
        return { success: false, error: `Unknown service type: ${type}` };
      }

      // Wait for service to stabilize
      await new Promise(resolve => setTimeout(resolve, waitAfterMs));

      return {
        success: true,
        result: {
          service,
          type,
          action: 'restart',
          waitedMs: waitAfterMs
        }
      };

    } catch (err) {
      return {
        success: false,
        result: { service, type },
        error: err.message
      };
    }
  },

  /**
   * Run a shell command
   */
  runCommand(cmd, args) {
    return new Promise((resolve, reject) => {
      const proc = spawn(cmd, args, { stdio: 'pipe' });
      let stdout = '';
      let stderr = '';

      proc.stdout?.on('data', d => { stdout += d; });
      proc.stderr?.on('data', d => { stderr += d; });

      proc.on('close', code => {
        if (code === 0) {
          resolve({ stdout, stderr });
        } else {
          reject(new Error(`Command failed with code ${code}: ${stderr || stdout}`));
        }
      });

      proc.on('error', reject);
    });
  }
};
