/**
 * Auth Middleware for Featurexd
 */
import { createAuthMiddleware } from '../../../core/common/authMiddlewareFactory.js';

export default createAuthMiddleware({
  tokenEnvVar: 'FEATUREXD_TOKEN',
  defaultLimit: 60,
  pathLimits: {
    '/features': 20
  }
});
