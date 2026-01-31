import { Router, Request, Response } from 'express';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const router = Router();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REGISTRY_DIR = path.resolve(__dirname, '../../../services/strategyd/registry');
const EXP_JSONL = path.join(REGISTRY_DIR, 'experiments.jsonl');
const INDEX_PATH = path.join(REGISTRY_DIR, 'index.json');
const EXP_DIR = path.resolve(__dirname, '../../../services/strategyd/experiments');

async function readJsonIfExists(filePath: string, fallback: any) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function readJsonl(filePath: string) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
  } catch {
    return [];
  }
}

router.get('/experiments', async (_req: Request, res: Response) => {
  const index = await readJsonIfExists(INDEX_PATH, {});
  const experiments = await readJsonl(EXP_JSONL);
  (res.locals as any).resultCount = experiments.length;
  return res.json({ count: experiments.length, index, experiments });
});

router.get('/experiments/:exp_id', async (req: Request, res: Response) => {
  const expId = req.params.exp_id;
  const leaderboardPath = path.join(EXP_DIR, expId, 'leaderboard.json');
  const leaderboard = await readJsonIfExists(leaderboardPath, null);
  if (!leaderboard) {
    return res.status(404).json({ error: 'NOT_FOUND', exp_id: expId });
  }
  return res.json(leaderboard);
});

export default router;
