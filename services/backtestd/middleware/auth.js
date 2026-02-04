/**
 * Auth Middleware for Backtestd
 */
import { createAuthMiddleware } from '../../../core/common/authMiddlewareFactory.js';

export default createAuthMiddleware({
  tokenEnvVar: 'BACKTESTD_TOKEN',
  defaultLimit: 60,
  pathLimits: {
    '/backtests': 20
  }
});
