/**
 * Alert Task
 *
 * Emits an alert through AlertManager.
 *
 * @param {Object} params
 * @param {string} params.severity - Alert severity (info, warning, error, critical)
 * @param {string} params.message - Alert message
 * @param {string} params.type - Alert type (default: RUNBOOK_ALERT)
 * @param {Object} params.metadata - Additional metadata
 */

export const AlertTask = {
  name: 'alert',
  description: 'Emit an alert',

  async execute(params, context) {
    const {
      severity = 'info',
      message,
      type = 'RUNBOOK_ALERT',
      metadata = {}
    } = params;

    if (!message) {
      return { success: false, error: 'Missing required param: message' };
    }

    // Dry-run mode
    if (context.dryRun) {
      return {
        success: true,
        result: {
          severity,
          type,
          message,
          dryRun: true
        }
      };
    }

    try {
      // Try to use AlertManager if available
      if (context.alertManager?.emit) {
        await context.alertManager.emit({
          type,
          severity,
          message,
          source: 'RunbookExecutor',
          runbook_id: context.runbookId,
          runbook_name: context.runbookName,
          ...metadata
        });

        return {
          success: true,
          result: {
            severity,
            type,
            message,
            emitted: true
          }
        };
      }

      // Fallback: log to console
      const logMethod = severity === 'critical' || severity === 'error'
        ? console.error
        : severity === 'warning'
          ? console.warn
          : console.log;

      logMethod(`[RUNBOOK_ALERT] [${severity.toUpperCase()}] ${message}`);

      return {
        success: true,
        result: {
          severity,
          type,
          message,
          emitted: false,
          fallback: 'console'
        }
      };

    } catch (err) {
      return {
        success: false,
        result: { severity, type, message },
        error: err.message
      };
    }
  }
};
