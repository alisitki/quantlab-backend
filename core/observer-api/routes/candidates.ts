import { Router, Request, Response } from 'express';
import { readFile, access } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const router = Router();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REGISTRY_DIR = path.resolve(__dirname, '../../../services/strategyd/registry');
const CAND_JSONL = path.join(REGISTRY_DIR, 'candidates.jsonl');
const PACK_DIR = path.resolve(__dirname, '../../../services/strategyd/candidates');

async function readJsonl(filePath: string) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return raw.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
  } catch {
    return [];
  }
}

async function readJsonIfExists(filePath: string) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function exists(filePath: string) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

router.get('/candidates', async (_req: Request, res: Response) => {
  const candidates = await readJsonl(CAND_JSONL);
  (res.locals as any).resultCount = candidates.length;
  return res.json({ count: candidates.length, candidates });
});

router.get('/candidates/:candidate_id', async (req: Request, res: Response) => {
  const candidateId = req.params.candidate_id;
  const packPath = path.join(PACK_DIR, candidateId, 'candidate.json');
  const pack = await readJsonIfExists(packPath);
  if (pack) {
    return res.json({ candidate: pack, pack_path: packPath });
  }
  const candidates = await readJsonl(CAND_JSONL);
  const match = candidates.find(c => c.candidate_id === candidateId);
  if (!match) {
    return res.status(404).json({ error: 'NOT_FOUND', candidate_id: candidateId });
  }
  const packExists = await exists(path.join(PACK_DIR, candidateId));
  return res.json({ candidate: match, pack_path: packExists ? path.join(PACK_DIR, candidateId) : null });
});

export default router;
