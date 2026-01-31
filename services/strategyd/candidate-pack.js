#!/usr/bin/env node
/**
 * candidate-pack.js — Build reproducible bundle for a candidate.
 *
 * Usage:
 *   node candidate-pack.js --candidate_id <id> [--force true]
 */

import { readFile, writeFile, mkdir, copyFile, access } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

const REGISTRY_DIR = path.resolve('./services/strategyd/registry');
const CAND_JSONL = path.join(REGISTRY_DIR, 'candidates.jsonl');
const INDEX_PATH = path.join(REGISTRY_DIR, 'index.json');
const OUT_BASE = path.resolve('./services/strategyd/candidates');

function parseArgs(argv) {
  const params = {};
  for (let i = 0; i < argv.length; i += 2) {
    params[argv[i].replace('--', '')] = argv[i + 1];
  }
  return params;
}

async function readJson(filePath) {
  const raw = await readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

async function readJsonl(filePath) {
  const raw = await readFile(filePath, 'utf8');
  return raw.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
}

async function exists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

function parseParamsShort(paramsShort) {
  const params = {};
  if (!paramsShort) return params;
  paramsShort.split(';').forEach(pair => {
    const [k, v] = pair.split('=');
    if (!k) return;
    const num = Number(v);
    params[k] = Number.isFinite(num) ? num : v;
  });
  return params;
}

async function fileHashSha256(filePath) {
  try {
    const buf = await readFile(filePath);
    return createHash('sha256').update(buf).digest('hex');
  } catch {
    return null;
  }
}

function buildReproScript(candidate, outDir) {
  const paramsJson = JSON.stringify(candidate.params, null, 0);
  const snapshotPath = path.join(outDir, 'artifacts', 'snapshot_report.json');
  const tickPath = path.join(outDir, 'artifacts', 'tick_report.json');
  const validationPath = path.join(outDir, 'artifacts', 'validation_report.json');
  const strategyPath = path.join(outDir, 'artifacts', 'strategy.json');

  return `#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
ART_DIR="$BASE_DIR/artifacts"
SNAPSHOT_REPORT="$ART_DIR/snapshot_report.json"
TICK_REPORT="$ART_DIR/tick_report.json"
VALIDATION_REPORT="$ART_DIR/validation_report.json"
STRATEGY_FILE="$ART_DIR/strategy.json"

echo "[REPRO] Running snapshot eval"
node "$BASE_DIR/../../eval-run.js" \\
  --mode snapshot \\
  --strategy ${candidate.strategy_id} \\
  --symbol ${candidate.dataset.symbol} \\
  --date ${candidate.dataset.date} \\
  --params '${paramsJson}' \\
  ${candidate.strategy_file ? '--strategy_file "$STRATEGY_FILE"' : ''} \\
  --out "$SNAPSHOT_REPORT"

node - <<'NODE'
const fs = require('fs');
const expected = JSON.parse(fs.readFileSync('${path.join(outDir, 'candidate.json')}', 'utf8'));
const report = JSON.parse(fs.readFileSync('${snapshotPath}', 'utf8'));
if (report.determinism.state_hash !== expected.expected.snapshot.state_hash) {
  console.error('SNAPSHOT state_hash mismatch');
  process.exit(1);
}
if (report.determinism.fills_hash !== expected.expected.snapshot.fills_hash) {
  console.error('SNAPSHOT fills_hash mismatch');
  process.exit(1);
}
console.log('SNAPSHOT hash match');
NODE

if [ -f "$STRATEGY_FILE" ]; then
  EXPECTED_SHA=$(node -e "const c=require('${path.join(outDir, 'candidate.json')}'); console.log(c.strategy_checksum || '');")
  ACTUAL_SHA=$(sha256sum "$STRATEGY_FILE" | awk '{print $1}')
  if [ "$EXPECTED_SHA" != "$ACTUAL_SHA" ]; then
    echo "STRATEGY checksum mismatch"
    exit 1
  fi
  echo "STRATEGY checksum match"
fi

if [ -f "$VALIDATION_REPORT" ]; then
  echo "[REPRO] Running tick eval"
  node "$BASE_DIR/../../eval-run.js" \\
    --mode tick \\
    --strategy ${candidate.strategy_id} \\
    --symbol ${candidate.dataset.symbol} \\
    --date ${candidate.dataset.date} \\
    --params '${paramsJson}' \\
    ${candidate.strategy_file ? '--strategy_file "$STRATEGY_FILE"' : ''} \\
    --out "$TICK_REPORT"

  node - <<'NODE'
const fs = require('fs');
const expected = JSON.parse(fs.readFileSync('${path.join(outDir, 'candidate.json')}', 'utf8'));
const report = JSON.parse(fs.readFileSync('${tickPath}', 'utf8'));
if (report.determinism.state_hash !== expected.expected.tick.state_hash) {
  console.error('TICK state_hash mismatch');
  process.exit(1);
}
if (report.determinism.fills_hash !== expected.expected.tick.fills_hash) {
  console.error('TICK fills_hash mismatch');
  process.exit(1);
}
console.log('TICK hash match');
NODE
fi
`;
}

function buildReproReadme(candidate, leaderboardHash) {
  return `# Candidate Repro Pack

## Candidate
- candidate_id: ${candidate.candidate_id}
- exp_id: ${candidate.exp_id}
- strategy_id: ${candidate.strategy_id}
- params_hash: ${candidate.params_hash}
- params_short: ${candidate.params_short}
- dataset: ${candidate.dataset.stream}/${candidate.dataset.symbol}/${candidate.dataset.date}

## Expected hashes
- snapshot.state_hash: ${candidate.expected.snapshot.state_hash}
- snapshot.fills_hash: ${candidate.expected.snapshot.fills_hash}
- tick.state_hash: ${candidate.expected.tick?.state_hash || 'n/a'}
- tick.fills_hash: ${candidate.expected.tick?.fills_hash || 'n/a'}
- leaderboard_hash: ${leaderboardHash || 'n/a'}

## Run
1) Ensure REPLAYD_URL and REPLAYD_TOKEN are set in env (if required).
2) Execute:
   ./repro.sh
`;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.candidate_id) {
    console.error('Usage: node candidate-pack.js --candidate_id <id> [--force true]');
    process.exit(1);
  }
  const force = args.force === 'true';
  const candidateId = args.candidate_id;

  const candidates = await readJsonl(CAND_JSONL);
  const candidate = candidates.find(c => c.candidate_id === candidateId);
  if (!candidate) {
    console.error(`Candidate not found: ${candidateId}`);
    process.exit(1);
  }

  const outDir = path.join(OUT_BASE, candidateId);
  if (await exists(outDir)) {
    if (!force) {
      console.error(`Pack exists: ${outDir}. Use --force true to overwrite.`);
      process.exit(1);
    }
  }

  await mkdir(path.join(outDir, 'artifacts'), { recursive: true });

  const snapshotReport = await readJson(candidate.link.report_path);
  const validationReport = candidate.link.validation_report_path
    ? await readJson(candidate.link.validation_report_path)
    : null;

  const tickReportPath = candidate.link.validation_report_path
    ? path.join(path.dirname(candidate.link.report_path), candidate.link.report_path.split('/').pop().replace('_snapshot_report.json', '_tick_report.json'))
    : null;
  const tickReport = tickReportPath && await exists(tickReportPath) ? await readJson(tickReportPath) : null;

  const candidateParams = parseParamsShort(candidate.params_short);
  const strategyArtifactPath = snapshotReport.provenance?.strategy_artifact_path || null;
  const strategyArtifactHash = snapshotReport.provenance?.strategy_artifact_sha256 || null;
  if (!strategyArtifactPath || !strategyArtifactHash) {
    console.error('Missing strategy artifact in provenance; ensure eval-run used registry or --strategy_file.');
    process.exit(1);
  }
  const resolved = {
    ...candidate,
    dataset: {
      stream: 'bbo',
      symbol: snapshotReport.dataset.symbol,
      date: snapshotReport.dataset.date_range
    },
    params: candidateParams,
    expected: {
      snapshot: {
        state_hash: snapshotReport.determinism.state_hash,
        fills_hash: snapshotReport.determinism.fills_hash
      },
      tick: tickReport
        ? {
            state_hash: tickReport.determinism.state_hash,
            fills_hash: tickReport.determinism.fills_hash
          }
        : null
    },
    provenance: snapshotReport.provenance || {},
    strategy_file: strategyArtifactPath,
    strategy_checksum: strategyArtifactHash,
    env: {
      REPLAYD_URL: 'required if not default',
      REPLAYD_TOKEN: 'required if auth enabled'
    }
  };

  const leaderboardHash = await (async () => {
    try {
      const index = await readJson(INDEX_PATH);
      return index[candidate.exp_id]?.leaderboard_hash || null;
    } catch {
      return null;
    }
  })();

  await writeFile(path.join(outDir, 'candidate.json'), JSON.stringify(resolved, null, 2));

  await copyFile(candidate.link.report_path, path.join(outDir, 'artifacts', 'snapshot_report.json'));
  if (validationReport) {
    await copyFile(candidate.link.validation_report_path, path.join(outDir, 'artifacts', 'validation_report.json'));
  }
  if (tickReport) {
    await copyFile(tickReportPath, path.join(outDir, 'artifacts', 'tick_report.json'));
  }
  const strategyOut = path.join(outDir, 'artifacts', 'strategy.json');
  await copyFile(strategyArtifactPath, strategyOut);

  const repro = buildReproScript(resolved, outDir);
  await writeFile(path.join(outDir, 'repro.sh'), repro, { mode: 0o755 });
  await writeFile(path.join(outDir, 'REPRO.md'), buildReproReadme(resolved, leaderboardHash));

  console.log(`[PACK] candidate_id=${candidateId} out_dir=${outDir}`);
}

run().catch(err => {
  console.error(err.message);
  process.exit(1);
});
