/**
 * Wait Task
 *
 * Waits for a specified duration.
 *
 * @param {Object} params
 * @param {number} params.durationMs - Wait duration in milliseconds
 * @param {string} params.reason - Reason for waiting (for logging)
 */

export const WaitTask = {
  name: 'wait',
  description: 'Wait for a specified duration',

  async execute(params, context) {
    const {
      durationMs = 1000,
      reason = 'Scheduled wait'
    } = params;

    if (durationMs <= 0) {
      return { success: true, result: { waited: 0, reason } };
    }

    // Cap at 5 minutes for safety
    const actualDuration = Math.min(durationMs, 5 * 60 * 1000);

    // Dry-run mode
    if (context.dryRun) {
      return {
        success: true,
        result: {
          durationMs: actualDuration,
          reason,
          dryRun: true,
          message: `Would wait ${actualDuration}ms`
        }
      };
    }

    const startTime = Date.now();

    await new Promise(resolve => setTimeout(resolve, actualDuration));

    const actualWaited = Date.now() - startTime;

    return {
      success: true,
      result: {
        requested: durationMs,
        actual: actualWaited,
        reason
      }
    };
  }
};
