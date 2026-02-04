/**
 * @deprecated Use core/observer-api (port 3000) instead.
 * This module is kept for backwards compatibility only.
 */
console.warn('[DEPRECATED] core/observer is deprecated. Use core/observer-api (port 3000) instead.');

import express from 'express';
import { authGuard, auditLog } from './middleware.js';
import { observerRegistry } from './ObserverRegistry.js';

export function createObserverApp() {
  const app = express();
  app.use(express.json());
  app.use('/observer', authGuard);

  app.get('/observer/health', (_req, res) => {
    return res.json(observerRegistry.getHealth());
  });

  app.get('/observer/runs', (_req, res) => {
    return res.json({ runs: observerRegistry.listRuns() });
  });

  app.post('/observer/runs/:id/stop', auditLog, (req, res) => {
    const id = req.params.id;
    const ok = observerRegistry.stopRun(id, 'MANUAL_STOP');
    if (!ok) return res.status(404).json({ error: 'RUN_NOT_FOUND' });
    return res.json({ status: 'ok', id });
  });

  return app;
}

if (process.argv[1] && process.argv[1].endsWith('index.js')) {
  const port = Number(process.env.OBSERVER_PORT || 9150);
  const app = createObserverApp();
  app.listen(port, () => {
    console.log(`[observer] listening on ${port}`);
  });
}
