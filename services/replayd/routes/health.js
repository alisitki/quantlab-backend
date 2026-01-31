/**
 * Replayd Health Route
 * GET /health
 */

import { ORDERING_COLUMNS } from '../../../core/replay/ORDERING_CONTRACT.js';
import { REPLAY_VERSION } from '../config.js';

export default async function healthRoutes(fastify) {
  fastify.get('/health', async () => {
    return {
      status: 'ok',
      replay_version: REPLAY_VERSION,
      ordering_contract: ORDERING_COLUMNS
    };
  });
}
