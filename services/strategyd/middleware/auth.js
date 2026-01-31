/**
 * Auth Middleware for Strategyd
 * Handles Bearer Token + IP-based Rate Limiting
 */

const TOKEN = process.env.STRATEGYD_TOKEN;

// IP -> { endpoints: { [path]: timestamp[] } }
const rateLimitMap = new Map();

/**
 * Basic Rate Limiter Logic
 * @param {string} ip 
 * @param {string} path 
 * @param {number} limit 
 * @param {number} windowMs 
 * @returns {boolean}
 */
function isRateLimited(ip, path, limit, windowMs = 60000) {
  const now = Date.now();
  if (!rateLimitMap.has(ip)) {
    rateLimitMap.set(ip, new Map());
  }
  const ipMap = rateLimitMap.get(ip);
  if (!ipMap.has(path)) {
    ipMap.set(path, []);
  }
  
  const timestamps = ipMap.get(path);
  // Remove old timestamps
  while (timestamps.length > 0 && timestamps[0] < now - windowMs) {
    timestamps.shift();
  }
  
  if (timestamps.length >= limit) {
    return true;
  }
  
  timestamps.push(now);
  return false;
}

export default async function authMiddleware(request, reply) {
  const { url, method, ip } = request;
  const path = url.split('?')[0];

  // 0. Auth required check
  const authRequired = process.env.AUTH_REQUIRED !== 'false';

  // 1. Rate Limiting
  let limit = 120; // Default
  if (path === '/control') limit = 10;
  
  if (isRateLimited(ip, path, limit)) {
    return reply.code(429).send({ 
      error: 'TOO_MANY_REQUESTS', 
      message: `Rate limit exceeded for ${path}. Limit: ${limit}/min` 
    });
  }

  // 2. Authentication
  // Health check or dev mode bypass
  if (path === '/health' || !authRequired) return;

  const authHeader = request.headers.authorization;
  let tokenValue;

  if (authHeader && authHeader.startsWith('Bearer ')) {
    tokenValue = authHeader.split(' ')[1];
  } else if (request.query.token) {
    tokenValue = request.query.token;
  }

  if (!tokenValue) {
    return reply.code(401).send({ error: 'UNAUTHORIZED', message: 'Token required (Bearer or query ?token=)' });
  }

  if (tokenValue !== TOKEN) {
    return reply.code(401).send({ error: 'UNAUTHORIZED', message: 'Invalid token' });
  }
}

