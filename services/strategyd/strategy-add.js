#!/usr/bin/env node
/**
 * strategy-add.js — Add strategy artifact to registry.
 *
 * Usage:
 *   node strategy-add.js --strategy_id ema_cross --file /path/to/strategy.json [--version v1.0.0]
 */

import { readFile, writeFile, mkdir, appendFile, copyFile } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

const REGISTRY_DIR = path.resolve('./services/strategyd/strategy_registry');
const ART_DIR = path.join(REGISTRY_DIR, 'artifacts');
const INDEX_PATH = path.join(REGISTRY_DIR, 'index.json');
const JSONL_PATH = path.join(REGISTRY_DIR, 'strategies.jsonl');

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

async function fileHashSha256(filePath) {
  const buf = await readFile(filePath);
  return createHash('sha256').update(buf).digest('hex');
}

async function readJsonIfExists(filePath, fallback) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.strategy_id || !args.file) {
    console.error('Usage: node strategy-add.js --strategy_id <id> --file <path> [--version v1.0.0]');
    process.exit(1);
  }

  await mkdir(REGISTRY_DIR, { recursive: true });
  await mkdir(ART_DIR, { recursive: true });

  const strategyId = args.strategy_id;
  const src = args.file;
  const sha = await fileHashSha256(src);
  const version = args.version || `sha256:${sha.slice(0, 12)}`;

  const destDir = path.join(ART_DIR, strategyId);
  await mkdir(destDir, { recursive: true });
  const destPath = path.join(destDir, `${version}.json`);
  await copyFile(src, destPath);

  const meta = await readJsonIfExists(src, {});
  const record = {
    strategy_id: strategyId,
    strategy_version: version,
    created_at: new Date().toISOString(),
    artifact_path: destPath,
    artifact_sha256: sha,
    meta: {
      name: meta.name || null,
      description: meta.description || null,
      nodes_count: Array.isArray(meta.nodes) ? meta.nodes.length : null,
      edges_count: Array.isArray(meta.edges) ? meta.edges.length : null
    }
  };

  await appendFile(JSONL_PATH, JSON.stringify(record) + '\n');

  const index = await readJsonIfExists(INDEX_PATH, {});
  index[strategyId] = {
    latest_version: version,
    artifact_path: destPath,
    artifact_sha256: sha,
    updated_at: record.created_at
  };
  await writeFile(INDEX_PATH, JSON.stringify(index, null, 2));

  console.log(`[STRATEGY] strategy_id=${strategyId} version=${version} sha256=${sha}`);
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
