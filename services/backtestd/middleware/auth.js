/**
 * Auth Middleware for Backtestd
 * Handles Bearer Token + IP-based Rate Limiting
 */

const TOKEN = process.env.BACKTESTD_TOKEN;

const rateLimitMap = new Map();

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
  while (timestamps.length > 0 && timestamps[0] < now - windowMs) {
    timestamps.shift();
  }

  if (timestamps.length >= limit) return true;
  timestamps.push(now);
  return false;
}

export default async function authMiddleware(request, reply) {
  const { url, ip } = request;
  const path = url.split('?')[0];

  const authRequired = process.env.AUTH_REQUIRED !== 'false';

  let limit = 60;
  if (path.startsWith('/backtests')) limit = 20;

  if (isRateLimited(ip, path, limit)) {
    return reply.code(429).send({
      error: 'TOO_MANY_REQUESTS',
      message: `Rate limit exceeded for ${path}. Limit: ${limit}/min`
    });
  }

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

  if (!TOKEN || tokenValue !== TOKEN) {
    return reply.code(401).send({ error: 'UNAUTHORIZED', message: 'Invalid token' });
  }
}
