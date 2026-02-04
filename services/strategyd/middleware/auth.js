/**
 * Auth Middleware for Strategyd
 */
import { createAuthMiddleware } from '../../../core/common/authMiddlewareFactory.js';

export default createAuthMiddleware({
  tokenEnvVar: 'STRATEGYD_TOKEN',
  defaultLimit: 120,
  pathLimits: {
    '/control': 10,
    '/live/start': 30,        // Heavy operation (WS connection + runner instantiation)
    '/live/stop': 60,         // Medium operation
    '/live/kill-switch': 10,  // Critical - very limited
    '/v1/monitor/dashboard': 60,   // Frequent polling for dashboard
    '/v1/monitor/positions': 30,   // May trigger reconciliation lookup
    '/v1/monitor': 120             // Default for other monitor endpoints
  }
});
