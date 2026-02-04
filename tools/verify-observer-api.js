#!/usr/bin/env node
/**
 * Verify observer API (runs + stop)
 */

import http from 'node:http';
import { createObserverApp } from '../core/observer/index.js';
import { observerRegistry } from '../core/observer/ObserverRegistry.js';

process.env.OBSERVER_TOKEN = 'test-token';

function request({ method, path, port }) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      method,
      hostname: '127.0.0.1',
      port,
      path,
      headers: { Authorization: 'Bearer test-token' }
    }, (res) => {
      let data = '';
      res.on('data', (c) => { data += c; });
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.end();
  });
}

async function main() {
  const app = createObserverApp();
  const server = app.listen(0);
  const port = server.address().port;

  observerRegistry.addRun({
    live_run_id: 'run_test',
    strategy_id: 'strat_test',
    started_at: new Date().toISOString(),
    stopFn: () => {}
  });

  const runsRes = await request({ method: 'GET', path: '/observer/runs', port });
  if (runsRes.status !== 200 || !runsRes.body.includes('run_test')) {
    console.error('FAIL: runs not visible');
    process.exit(1);
  }

  const stopRes = await request({ method: 'POST', path: '/observer/runs/run_test/stop', port });
  if (stopRes.status !== 200) {
    console.error('FAIL: stop failed');
    process.exit(1);
  }

  const after = observerRegistry.listRuns().find(r => r.live_run_id === 'run_test');
  if (!after || after.status !== 'STOPPED') {
    console.error('FAIL: stop did not update status');
    process.exit(1);
  }

  server.close();
  console.log('PASS');
}

main().catch((err) => {
  console.error('FAIL', err.message || String(err));
  process.exit(1);
});
