/**
 * Run Monitoring Routes for observer-api
 *
 * GET  /runs       - List all runs
 * GET  /runs/health - Get run health status
 * POST /runs/:id/stop - Stop a specific run
 *
 * Migrated from core/observer (deprecated)
 */

import { Router, Request, Response } from 'express';
import { observerRegistry } from '../../observer/ObserverRegistry.js';

const router = Router();

/**
 * GET /runs - List all runs
 */
router.get('/runs', (req: Request, res: Response) => {
  const runs = observerRegistry.listRuns();
  return res.json({ runs });
});

/**
 * GET /runs/health - Get run health status
 */
router.get('/runs/health', (req: Request, res: Response) => {
  const health = observerRegistry.getHealth();
  return res.json(health);
});

/**
 * POST /runs/:id/stop - Stop a specific run
 */
router.post('/runs/:id/stop', (req: Request, res: Response) => {
  const id = req.params.id;
  const ok = observerRegistry.stopRun(id, 'MANUAL_STOP');
  if (!ok) {
    return res.status(404).json({ error: 'RUN_NOT_FOUND' });
  }
  return res.json({ status: 'ok', id });
});

export default router;
