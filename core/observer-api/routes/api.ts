import { Router, Request, Response } from 'express';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const router = Router();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LEDGER_PATH = path.resolve(__dirname, '../../research/jobs.jsonl');
const RUNS_DIR = path.resolve(__dirname, '../../../services/strategyd/runs');
const REGISTRY_DIR = path.resolve(__dirname, '../../../services/strategyd/registry');
const CAND_JSONL = path.join(REGISTRY_DIR, 'candidates.jsonl');
const EXP_JSONL = path.join(REGISTRY_DIR, 'experiments.jsonl');

async function readJsonl(filePath: string) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  } catch {
    return [];
  }
}

router.get('/state', (_req: Request, res: Response) => {
  (res.locals as any).resultCount = 1;
  return res.json({ status: 'ok', service: 'observer-api', time: new Date().toISOString() });
});

router.get('/jobs', async (req: Request, res: Response) => {
  const status = req.query.status as string | undefined;
  const limit = Number(req.query.limit || 100);
  const entries = await readJsonl(LEDGER_PATH);
  const latest = new Map<string, any>();
  for (const e of entries) latest.set(e.job_id, e);
  let list = Array.from(latest.values());
  if (status) list = list.filter(j => j.status === status);
  list.sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());
  const sliced = list.slice(-limit);
  (res.locals as any).resultCount = sliced.length;
  return res.json({ count: sliced.length, jobs: sliced });
});

router.get('/runs', async (req: Request, res: Response) => {
  const limit = Number(req.query.limit || 50);
  try {
    const files = (await readdir(RUNS_DIR)).filter(f => f.endsWith('.json')).sort().reverse();
    const subset = files.slice(0, limit);
    const runs = [];
    for (const file of subset) {
      const raw = await readFile(path.join(RUNS_DIR, file), 'utf8');
      runs.push(JSON.parse(raw));
    }
    (res.locals as any).resultCount = runs.length;
    return res.json({ count: runs.length, runs });
  } catch (err: any) {
    console.error(`[API] code=RUNS_READ_FAILED error=${err.message}`);
    return res.status(500).json({ error: 'RUNS_READ_FAILED', message: err.message });
  }
});

router.get('/metrics', async (_req: Request, res: Response) => {
  const jobs = await readJsonl(LEDGER_PATH);
  const experiments = await readJsonl(EXP_JSONL);
  const candidates = await readJsonl(CAND_JSONL);
  const latest = new Map<string, any>();
  for (const e of jobs) latest.set(e.job_id, e);
  const statusCounts = { queued: 0, running: 0, done: 0, failed: 0 };
  for (const j of latest.values()) {
    if (statusCounts[j.status as keyof typeof statusCounts] !== undefined) {
      statusCounts[j.status as keyof typeof statusCounts] += 1;
    }
  }
  const metrics = {
    jobs_total: latest.size,
    jobs_status: statusCounts,
    experiments_total: experiments.length,
    candidates_total: candidates.length
  };
  (res.locals as any).resultCount = 1;
  return res.json(metrics);
});

export default router;
