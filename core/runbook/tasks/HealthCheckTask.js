/**
 * Health Check Task
 *
 * Checks health endpoint of a service.
 *
 * @param {Object} params
 * @param {string} params.service - Service name
 * @param {string} params.url - Health endpoint URL
 * @param {number} params.timeout - Timeout in ms (default: 5000)
 * @param {string} params.expectedStatus - Expected HTTP status (default: 200)
 */

export const HealthCheckTask = {
  name: 'health-check',
  description: 'Check service health endpoint',

  /**
   * Execute health check
   * @param {Object} params - Task parameters
   * @param {Object} context - Execution context
   * @returns {Promise<{ success: boolean, result?: Object, error?: string }>}
   */
  async execute(params, context) {
    const {
      service,
      url,
      timeout = 5000,
      expectedStatus = 200
    } = params;

    if (!url) {
      return { success: false, error: 'Missing required param: url' };
    }

    const startTime = Date.now();

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);

      const response = await fetch(url, {
        method: 'GET',
        signal: controller.signal,
        headers: {
          'Accept': 'application/json'
        }
      });

      clearTimeout(timeoutId);

      const latencyMs = Date.now() - startTime;
      let body = null;

      try {
        body = await response.json();
      } catch {
        // Non-JSON response is OK for health checks
      }

      const success = response.status === expectedStatus;

      return {
        success,
        result: {
          service,
          url,
          status: response.status,
          latencyMs,
          healthy: success,
          body
        },
        error: success ? undefined : `Expected status ${expectedStatus}, got ${response.status}`
      };

    } catch (err) {
      const latencyMs = Date.now() - startTime;

      if (err.name === 'AbortError') {
        return {
          success: false,
          result: { service, url, latencyMs, healthy: false },
          error: `Timeout after ${timeout}ms`
        };
      }

      return {
        success: false,
        result: { service, url, latencyMs, healthy: false },
        error: err.message
      };
    }
  }
};
