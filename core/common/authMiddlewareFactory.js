/**
 * Shared Auth Middleware Factory
 *
 * Provides consistent authentication and rate limiting for all Fastify services.
 *
 * @module core/common/authMiddlewareFactory
 */

/**
 * Create a rate limiter instance (per-service isolation).
 * @returns {Map} Rate limit map
 */
function createRateLimitMap() {
  return new Map();
}

/**
 * Check if IP is rate limited for a path.
 * @param {Map} rateLimitMap - Rate limit state map
 * @param {string} ip - Client IP
 * @param {string} path - Request path
 * @param {number} limit - Max requests per window
 * @param {number} windowMs - Window in milliseconds
 * @returns {boolean} True if rate limited
 */
function isRateLimited(rateLimitMap, ip, path, limit, windowMs = 60000) {
  const now = Date.now();
  if (!rateLimitMap.has(ip)) {
    rateLimitMap.set(ip, new Map());
  }
  const ipMap = rateLimitMap.get(ip);
  if (!ipMap.has(path)) {
    ipMap.set(path, []);
  }

  const timestamps = ipMap.get(path);
  while (timestamps.length > 0 && timestamps[0] < now - windowMs) {
    timestamps.shift();
  }

  if (timestamps.length >= limit) return true;
  timestamps.push(now);
  return false;
}

/**
 * Create auth middleware for a Fastify service.
 *
 * @param {Object} options - Configuration options
 * @param {string} options.tokenEnvVar - Environment variable name for token (e.g., 'STRATEGYD_TOKEN')
 * @param {number} [options.defaultLimit=60] - Default rate limit per minute
 * @param {Object} [options.pathLimits={}] - Path-specific rate limits (e.g., { '/stream': 30 })
 * @returns {Function} Fastify preHandler middleware
 *
 * @example
 * import { createAuthMiddleware } from '../../core/common/authMiddlewareFactory.js';
 *
 * const authMiddleware = createAuthMiddleware({
 *   tokenEnvVar: 'STRATEGYD_TOKEN',
 *   defaultLimit: 120,
 *   pathLimits: { '/control': 10 }
 * });
 *
 * fastify.addHook('preHandler', authMiddleware);
 */
export function createAuthMiddleware(options) {
  const {
    tokenEnvVar,
    defaultLimit = 60,
    pathLimits = {}
  } = options;

  const rateLimitMap = createRateLimitMap();

  return async function authMiddleware(request, reply) {
    const token = process.env[tokenEnvVar];
    const authRequired = process.env.AUTH_REQUIRED !== 'false';

    const { url, ip } = request;
    const path = url.split('?')[0];

    // 1. Determine rate limit for this path
    let limit = defaultLimit;
    for (const [pathPrefix, pathLimit] of Object.entries(pathLimits)) {
      if (path === pathPrefix || path.startsWith(pathPrefix)) {
        limit = pathLimit;
        break;
      }
    }

    // 2. Rate limiting
    if (isRateLimited(rateLimitMap, ip, path, limit)) {
      return reply.code(429).send({
        error: 'TOO_MANY_REQUESTS',
        message: `Rate limit exceeded for ${path}. Limit: ${limit}/min`
      });
    }

    // 3. Health check bypass
    if (path === '/health' || !authRequired) return;

    // 4. Token authentication
    const authHeader = request.headers.authorization;
    let tokenValue;

    if (authHeader && authHeader.startsWith('Bearer ')) {
      tokenValue = authHeader.split(' ')[1];
    } else if (request.query.token) {
      tokenValue = request.query.token;
    }

    if (!tokenValue) {
      return reply.code(401).send({
        error: 'UNAUTHORIZED',
        message: 'Token required (Bearer or query ?token=)'
      });
    }

    if (!token || tokenValue !== token) {
      return reply.code(401).send({
        error: 'UNAUTHORIZED',
        message: 'Invalid token'
      });
    }
  };
}

export default createAuthMiddleware;
