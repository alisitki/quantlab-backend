/**
 * Auth Middleware for Labeld
 */
import { createAuthMiddleware } from '../../../core/common/authMiddlewareFactory.js';

export default createAuthMiddleware({
  tokenEnvVar: 'LABELD_TOKEN',
  defaultLimit: 60,
  pathLimits: {
    '/labels': 20
  }
});
