/**
 * Auth Middleware for Replayd
 */
import { createAuthMiddleware } from '../../../core/common/authMiddlewareFactory.js';

export default createAuthMiddleware({
  tokenEnvVar: 'REPLAYD_TOKEN',
  defaultLimit: 120,
  pathLimits: {
    '/stream': 30
  }
});
