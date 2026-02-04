/**
 * Observer Middleware
 *
 * Auth and audit middleware for Observer API.
 * Used by core/observer/index.js createObserverApp().
 */

import { emitAudit } from '../audit/AuditWriter.js';

const TOKEN = process.env.OBSERVER_TOKEN || '';
const rateLimitMap = new Map();

function isRateLimited(ip, path, limit, windowMs = 60000) {
  const now = Date.now();
  if (!rateLimitMap.has(ip)) rateLimitMap.set(ip, new Map());
  const ipMap = rateLimitMap.get(ip);
  if (!ipMap.has(path)) ipMap.set(path, []);
  const timestamps = ipMap.get(path);
  while (timestamps.length > 0 && timestamps[0] < now - windowMs) timestamps.shift();
  if (timestamps.length >= limit) return true;
  timestamps.push(now);
  return false;
}

export function authGuard(req, res, next) {
  const path = req.path;
  const ip = req.ip || 'unknown';

  let limit = 120;
  if (path.includes('/stop')) limit = 30;
  if (isRateLimited(ip, path, limit)) {
    return res.status(429).json({ error: 'TOO_MANY_REQUESTS', message: `Limit ${limit}/min` });
  }

  const authHeader = req.headers.authorization;
  let token = '';
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }
  req.auditActor = token ? `user:${token}` : 'user:unknown';

  if (!TOKEN || token !== TOKEN) {
    return res.status(401).json({ error: 'UNAUTHORIZED', message: 'Bearer token required' });
  }

  return next();
}

export function auditLog(req, _res, next) {
  try {
    console.log(JSON.stringify({
      event: 'observer_control',
      method: req.method,
      path: req.path,
      ip: req.ip || 'unknown',
      time: new Date().toISOString()
    }));
  } catch {
    // ignore
  }
  emitAudit({
    actor: req.auditActor || 'user:unknown',
    action: 'API_CALL',
    target_type: 'system',
    target_id: req.path,
    reason: null,
    metadata: {
      method: req.method,
      path: req.path,
      ip: req.ip || 'unknown'
    }
  });
  next();
}
