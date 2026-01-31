import { Router, Request, Response } from 'express';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { enqueueSweep, enqueuePromote, enqueuePack } from '../../scheduler/job_scheduler.js';

const router = Router();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LEDGER_PATH = path.resolve(__dirname, '../../research/jobs.jsonl');

async function readLedger() {
  try {
    const raw = await readFile(LEDGER_PATH, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  } catch {
    return [];
  }
}

router.get('/jobs', async (req: Request, res: Response) => {
  const status = req.query.status as string | undefined;
  const limit = Number(req.query.limit || 100);
  const entries = await readLedger();
  const latest = new Map<string, any>();
  for (const e of entries) latest.set(e.job_id, e);
  let list = Array.from(latest.values());
  if (status) list = list.filter(j => j.status === status);
  list.sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());
  const sliced = list.slice(-limit);
  (res.locals as any).resultCount = sliced.length;
  return res.json({ count: sliced.length, jobs: sliced });
});

router.get('/jobs/:job_id', async (req: Request, res: Response) => {
  const jobId = req.params.job_id;
  const entries = await readLedger();
  const timeline = entries.filter(e => e.job_id === jobId);
  timeline.sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());
  (res.locals as any).resultCount = timeline.length;
  return res.json({ job_id: jobId, timeline });
});

router.post('/jobs/enqueue/sweep', async (req: Request, res: Response) => {
  try {
    const body = req.body || {};
    const spec = body.spec || body;
    const exp_id = body.exp_id;
    const concurrency = body.concurrency;
    const result = await enqueueSweep(spec, { exp_id, concurrency });
    return res.json({ job_id: result.job_id, status: result.enqueued ? 'queued' : 'skip' });
  } catch (err: any) {
    console.error(`[JOBS] code=ENQUEUE_SWEEP_FAILED error=${err.message}`);
    return res.status(500).json({ error: 'ENQUEUE_SWEEP_FAILED', message: err.message });
  }
});

router.post('/jobs/enqueue/promote', async (req: Request, res: Response) => {
  try {
    const { exp_id } = req.body || {};
    const result = await enqueuePromote(exp_id);
    return res.json({ job_id: result.job_id, status: result.enqueued ? 'queued' : 'skip' });
  } catch (err: any) {
    console.error(`[JOBS] code=ENQUEUE_PROMOTE_FAILED error=${err.message}`);
    return res.status(500).json({ error: 'ENQUEUE_PROMOTE_FAILED', message: err.message });
  }
});

router.post('/jobs/enqueue/pack', async (req: Request, res: Response) => {
  try {
    const { candidate_id } = req.body || {};
    const result = await enqueuePack(candidate_id);
    return res.json({ job_id: result.job_id, status: result.enqueued ? 'queued' : 'skip' });
  } catch (err: any) {
    console.error(`[JOBS] code=ENQUEUE_PACK_FAILED error=${err.message}`);
    return res.status(500).json({ error: 'ENQUEUE_PACK_FAILED', message: err.message });
  }
});

export default router;
