#!/usr/bin/env node
/**
 * buildActiveConfig.js — build ACTIVE config from decision + triad report.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PromotionGuard } from '../runtime/PromotionGuard.js';
import { ActiveConfigExporter } from '../runtime/ActiveConfigExporter.js';

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

async function findReportBySeed(seed) {
  const files = await fs.readdir(REPORT_DIR);
  const candidates = files.filter((f) => f.includes(`_${seed}.json`) && f.startsWith('triad_'));
  if (!candidates.length) return null;
  candidates.sort();
  return path.join(REPORT_DIR, candidates[0]);
}

async function findDecisionBySeed(seed) {
  const files = await fs.readdir(REPORT_DIR);
  const candidates = files.filter((f) => f.includes(`_${seed}.json`) && f.startsWith('decision_'));
  if (!candidates.length) return null;
  candidates.sort();
  return path.join(REPORT_DIR, candidates[0]);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.seed && !args.decision_path) {
    console.error('Usage: node buildActiveConfig.js --seed <seed> | --decision_path <path>');
    process.exit(1);
  }

  let decisionPath = args.decision_path || null;
  let reportPath = null;
  if (!decisionPath && args.seed) {
    decisionPath = await findDecisionBySeed(args.seed);
  }
  if (args.seed) {
    reportPath = await findReportBySeed(args.seed);
  }

  if (!decisionPath) {
    console.error('[ActiveConfig] action=error error=DECISION_NOT_FOUND');
    process.exit(1);
  }
  if (!reportPath) {
    console.error('[ActiveConfig] action=error error=REPORT_NOT_FOUND');
    process.exit(1);
  }

  const decision = await readJson(decisionPath);
  const report = await readJson(reportPath);
  if (!decision || !report) {
    console.error('[ActiveConfig] action=error error=INVALID_INPUT');
    process.exit(1);
  }

  const guard = new PromotionGuard();
  const guardResult = guard.evaluate(decision, report);
  if (!guardResult.allowed) {
    console.log(`NOT_PROMOTED:${guardResult.reason}`);
    process.exit(0);
  }

  const exporter = new ActiveConfigExporter();
  const result = await exporter.export({ decision, report, guardResult });
  if (result.written) {
    console.log(`[ActiveConfig] action=written path=${result.path}`);
  } else {
    console.log(`[ActiveConfig] action=skipped reason=${result.reason} path=${result.path || ''}`.trim());
  }
}

main().catch((err) => {
  console.error(`[ActiveConfig] action=error error=${err.message}`);
  process.exit(1);
});
