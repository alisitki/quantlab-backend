/**
 * Auth Middleware for Strategyd
 */
import { createAuthMiddleware } from '../../../core/common/authMiddlewareFactory.js';

export default createAuthMiddleware({
  tokenEnvVar: 'STRATEGYD_TOKEN',
  defaultLimit: 120,
  pathLimits: {
    '/control': 10
  }
});
