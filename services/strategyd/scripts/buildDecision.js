#!/usr/bin/env node
/**
 * buildDecision.js — deterministic promotion decision from triad report.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { DecisionBuilder } from '../runtime/DecisionBuilder.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNS_DIR = path.resolve(__dirname, '../runs');
const REPORT_DIR = path.join(RUNS_DIR, 'report');

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith('--')) continue;
    const name = key.slice(2);
    const value = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true;
    args[name] = value;
    if (value !== true) i++;
  }
  return args;
}

async function readJson(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function sanitizeFileName(name) {
  return String(name).replace(/[^a-zA-Z0-9._-]+/g, '_');
}

async function findReportBySeed(seed) {
  const files = await fs.readdir(REPORT_DIR);
  const candidates = files.filter((f) => f.includes(`_${seed}.json`) && f.startsWith('triad_'));
  if (!candidates.length) return null;
  candidates.sort();
  return path.join(REPORT_DIR, candidates[0]);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.seed && !args.report_path) {
    console.error('Usage: node buildDecision.js --seed <seed> | --report_path <path>');
    process.exit(1);
  }

  let reportPath = args.report_path || null;
  if (!reportPath && args.seed) {
    reportPath = await findReportBySeed(args.seed);
  }
  if (!reportPath) {
    console.error('[DecisionBuilder] action=error error=REPORT_NOT_FOUND');
    process.exit(1);
  }

  const report = await readJson(reportPath);
  if (!report) {
    console.error('[DecisionBuilder] action=error error=REPORT_INVALID');
    process.exit(1);
  }

  const strategyId = report?.identity?.strategy_id || 'unknown';
  const seed = report?.identity?.seed || 'unknown';
  const decisionFile = `decision_${sanitizeFileName(strategyId)}_${sanitizeFileName(seed)}.json`;
  const outPath = path.join(REPORT_DIR, decisionFile);

  try {
    await fs.access(outPath);
    console.log(`[DecisionBuilder] action=skipped reason=exists path=${outPath}`);
    return;
  } catch {
    // continue
  }

  const builder = new DecisionBuilder();
  const decision = builder.build(report);
  await fs.writeFile(outPath, JSON.stringify(decision, null, 2));
  console.log(`[DecisionBuilder] action=written path=${outPath}`);
}

main().catch((err) => {
  console.error(`[DecisionBuilder] action=error error=${err.message}`);
  process.exit(1);
});
