set -euo pipefail
EVD_ROOT="evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"

node - <<'NODE'
import fs from 'node:fs';
import path from 'node:path';

const root = 'evidence/sprint6-acceptance-full2day-20260117-20260118-20260209';

function readText(p) {
  return fs.readFileSync(p, 'utf8');
}
function readJSON(p) {
  return JSON.parse(readText(p));
}

function parseSha256sum(p) {
  const m = new Map();
  const lines = readText(p).trim().split(/\n+/).filter(Boolean);
  for (const line of lines) {
    const parts = line.trim().split(/\s+/);
    if (parts.length < 2) continue;
    const hash = parts[0];
    const file = parts.slice(1).join(' ');
    m.set(file, hash);
  }
  return m;
}

function parseTimeV(p) {
  const txt = readText(p);
  const mElapsed = txt.match(/Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*([0-9:.]+)/);
  const mRss = txt.match(/Maximum resident set size \(kbytes\):\s*([0-9]+)/);

  let wall_s = null;
  if (mElapsed) {
    const t = mElapsed[1];
    const parts = t.split(':').map(x => x.trim());
    if (parts.length === 2) {
      const [m, s] = parts;
      wall_s = Number(m) * 60 + Number(s);
    } else if (parts.length === 3) {
      const [h, m, s] = parts;
      wall_s = Number(h) * 3600 + Number(m) * 60 + Number(s);
    }
    if (!Number.isFinite(wall_s)) wall_s = null;
  }

  const max_rss_kb = mRss ? Number(mRss[1]) : null;
  return { wall_s, max_rss_kb };
}

function readIntOrNull(p) {
  try {
    const t = readText(p).trim();
    if (!t) return null;
    const n = Number.parseInt(t, 10);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}

const meta1 = readJSON(path.join(root, 'inputs/adausdt_20260117_meta.json'));
const meta2 = readJSON(path.join(root, 'inputs/adausdt_20260118_meta.json'));
const shaMap = parseSha256sum(path.join(root, 'sha256/sha256sum_inputs_parquet.txt'));

const h1 = shaMap.get('adausdt_20260117.parquet') ?? null;
const h2 = shaMap.get('adausdt_20260118.parquet') ?? null;

const smokeAttempts = [
  { try: 1, start: '20260117', end: '20260118', rel: 'inventory/smoke/try1_20260117_20260118' },
  { try: 2, start: '20260116', end: '20260117', rel: 'inventory/smoke/try2_20260116_20260117' },
  { try: 3, start: '20260110', end: '20260111', rel: 'inventory/smoke/try3_20260110_20260111' }
].map(x => {
  const base = path.join(root, x.rel);
  const exit = readIntOrNull(path.join(base, 'exit_code.txt'));
  const patterns_scanned = readIntOrNull(path.join(base, 'patterns_scanned.txt'));
  const tv = parseTimeV(path.join(base, 'time-v.log'));
  return {
    try: x.try,
    window: { start: x.start, end: x.end },
    timeout_s: 300,
    heapMB: 6144,
    perm_mode: 'DEFAULT_ON',
    exit,
    patterns_scanned,
    wall_s: tv.wall_s,
    max_rss_kb: tv.max_rss_kb,
    cmd_relpath: `${x.rel}/cmd.sh`,
    stdout_relpath: `${x.rel}/stdout.log`,
    stderr_relpath: `${x.rel}/stderr.log`,
    time_v_relpath: `${x.rel}/time-v.log`,
    exit_code_relpath: `${x.rel}/exit_code.txt`
  };
});

const summary = {
  status: 'FAIL',
  failure_reason: 'smoke_timeout_or_no_patterns_scanned',
  full_definition_applied: 'day_quality==GOOD',
  window: {
    start: '20260117',
    end: '20260118',
    note: 'Target window = candidate rank#1 by lowest rows_total; acceptance window not selected because smoke did not reach patterns_scanned>0 within 300s for any of 3 candidates.'
  },
  inputs: {
    day1: {
      date: '20260117',
      rows: meta1?.rows ?? null,
      sha256: h1,
      day_quality: meta1?.day_quality ?? null
    },
    day2: {
      date: '20260118',
      rows: meta2?.rows ?? null,
      sha256: h2,
      day_quality: meta2?.day_quality ?? null
    }
  },
  inventory: {
    inventory_relpath: 'inventory/adausdt_bbo_daily_inventory.tsv',
    smoke_candidates_relpath: 'inventory/smoke_aday_listesi.txt',
    candidate_sha256_proof_relpath: 'inventory/candidate_sha256_proof.txt',
    sha256sum_candidate_parquets_relpath: 'inventory/sha256sum_candidate_parquets.txt'
  },
  smoke_sweep: {
    attempts: smokeAttempts,
    selected_window: null
  },
  run_matrix: [],
  determinism: {
    on_vs_on_pass: false,
    off_vs_off_pass: false,
    stable_fingerprint_sha256: null,
    note: 'Not run: acceptance runs blocked because smoke did not produce patterns_scanned>0 within 300s (max 3 candidates)'
  }
};

fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');
console.log('summary_written=summary.json');
NODE

# Show top of summary
sed -n '1,200p' "$EVD_ROOT/summary.json"
