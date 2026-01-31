import { Request, Response, NextFunction } from 'express';

const TOKEN = process.env.OBSERVER_TOKEN || '';
const rateLimitMap = new Map<string, Map<string, number[]>>();

function isRateLimited(ip: string, path: string, limit: number, windowMs = 60000) {
  const now = Date.now();
  if (!rateLimitMap.has(ip)) rateLimitMap.set(ip, new Map());
  const ipMap = rateLimitMap.get(ip)!;
  if (!ipMap.has(path)) ipMap.set(path, []);
  const timestamps = ipMap.get(path)!;
  while (timestamps.length > 0 && timestamps[0] < now - windowMs) timestamps.shift();
  if (timestamps.length >= limit) return true;
  timestamps.push(now);
  return false;
}

export function authGuard(req: Request, res: Response, next: NextFunction) {
  const path = req.path;
  const ip = req.ip || 'unknown';

  // Rate limit
  let limit = 120;
  if (path.startsWith('/jobs/enqueue')) limit = 30;
  if (isRateLimited(ip, path, limit)) {
    console.error(`[AUTH] code=TOO_MANY_REQUESTS ip=${ip} path=${path}`);
    return res.status(429).json({ error: 'TOO_MANY_REQUESTS', message: `Limit ${limit}/min` });
  }

  const authHeader = req.headers.authorization;
  let token = '';
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }

  if (!TOKEN || token !== TOKEN) {
    console.error(`[AUTH] code=UNAUTHORIZED ip=${ip} path=${path}`);
    return res.status(401).json({ error: 'UNAUTHORIZED', message: 'Bearer token required' });
  }

  return next();
}
