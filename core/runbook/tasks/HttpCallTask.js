/**
 * HTTP Call Task
 *
 * Makes an HTTP request to an endpoint.
 *
 * @param {Object} params
 * @param {string} params.url - Request URL
 * @param {string} params.method - HTTP method (default: GET)
 * @param {Object} params.headers - Request headers
 * @param {Object} params.body - Request body (for POST/PUT)
 * @param {number} params.timeout - Timeout in ms (default: 10000)
 * @param {number} params.expectedStatus - Expected status code
 */

export const HttpCallTask = {
  name: 'http-call',
  description: 'Make an HTTP request',

  async execute(params, context) {
    const {
      url,
      method = 'GET',
      headers = {},
      body,
      timeout = 10000,
      expectedStatus
    } = params;

    if (!url) {
      return { success: false, error: 'Missing required param: url' };
    }

    // Dry-run mode
    if (context.dryRun) {
      return {
        success: true,
        result: {
          url,
          method,
          dryRun: true,
          message: `Would call ${method} ${url}`
        }
      };
    }

    const startTime = Date.now();

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);

      const fetchOptions = {
        method,
        headers: {
          'Content-Type': 'application/json',
          ...headers
        },
        signal: controller.signal
      };

      if (body && ['POST', 'PUT', 'PATCH'].includes(method.toUpperCase())) {
        fetchOptions.body = JSON.stringify(body);
      }

      const response = await fetch(url, fetchOptions);
      clearTimeout(timeoutId);

      const latencyMs = Date.now() - startTime;
      let responseBody = null;

      try {
        responseBody = await response.json();
      } catch {
        try {
          responseBody = await response.text();
        } catch {
          // Ignore body parsing errors
        }
      }

      const success = expectedStatus
        ? response.status === expectedStatus
        : response.ok;

      return {
        success,
        result: {
          url,
          method,
          status: response.status,
          latencyMs,
          body: responseBody
        },
        error: success ? undefined : `HTTP ${response.status}`
      };

    } catch (err) {
      const latencyMs = Date.now() - startTime;

      if (err.name === 'AbortError') {
        return {
          success: false,
          result: { url, method, latencyMs },
          error: `Timeout after ${timeout}ms`
        };
      }

      return {
        success: false,
        result: { url, method, latencyMs },
        error: err.message
      };
    }
  }
};
