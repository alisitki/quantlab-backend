#!/usr/bin/env node
/**
 * Sprint-4 Multi-Day Discovery Runner (Official)
 *
 * Scope: CLI/runner only (no core refactors).
 *
 * Notes:
 * - Permutation test toggle is read at module-load time via DISCOVERY_CONFIG,
 *   so we set process.env.DISCOVERY_PERMUTATION_TEST BEFORE importing pipeline modules.
 * - Heap size cannot be changed for the current process; if heap is too low, we re-exec node
 *   with NODE_OPTIONS and --max-old-space-size to match --heapMB.
 */

import { spawn } from 'node:child_process';
import v8 from 'node:v8';
import path from 'node:path';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';

const SCRIPT_NAME = 'tools/run-multi-day-discovery.js';
const VALID_EXCHANGES = new Set(['binance', 'bybit', 'okx']);
const VALID_MODES = new Set(['smoke', 'acceptance']);
const VALID_PERMUTATION = new Set(['on', 'off']);
const VALID_SMOKE_SLICES = new Set(['head', 'tail', 'head_tail']);

function printHelp(exitCode = 0) {
  const msg = `
${SCRIPT_NAME}

Args:
  --exchange <binance|bybit|okx>       (default: binance)
  --symbol <ADA/USDT>                  (required)
  --stream <stream>                    (required)
  --start <YYYYMMDD>                   (required)
  --end <YYYYMMDD>                     (required)
  --heapMB <int>                       (default: 6144)
  --permutationTest <on|off>           (default: on)
  --mode <smoke|acceptance>            (default: smoke)
  --smokeMaxRowsPerDay <int>           (default: 200000; smoke only)
  --smokeSlice <head|tail|head_tail>   (default: head; smoke only)
  --progressEvery <N>                  (default: 1)
  --help

Examples:
  NODE_OPTIONS="--max-old-space-size=6144" node ${SCRIPT_NAME} \\
    --exchange binance --stream bbo --symbol ADA/USDT \\
    --start 20260110 --end 20260111 --mode acceptance
`.trim();
  console.log(msg);
  process.exit(exitCode);
}

function fatal(msg, exitCode = 1) {
  console.error(`[FATAL] ${msg}`);
  process.exit(exitCode);
}

function parseArgs(argv) {
  const out = {};
  const tokens = argv.slice(2);
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t === '--help' || t === '-h') {
      out.help = true;
      continue;
    }
    if (!t.startsWith('--')) {
      fatal(`Unexpected arg: ${t}`);
    }

    const eq = t.indexOf('=');
    if (eq !== -1) {
      const key = t.slice(2, eq);
      const value = t.slice(eq + 1);
      out[key] = value;
      continue;
    }

    const key = t.slice(2);
    const next = tokens[i + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    i++;
  }

  const allowed = new Set([
    'help',
    'exchange',
    'symbol',
    'stream',
    'start',
    'end',
    'heapMB',
    'permutationTest',
    'mode',
    'smokeMaxRowsPerDay',
    'smokeSlice',
    'progressEvery'
  ]);

  for (const k of Object.keys(out)) {
    if (!allowed.has(k)) fatal(`Unknown flag: --${k}`);
  }

  return out;
}

function toInt(name, value, { min = 1 } = {}) {
  const n = Number.parseInt(String(value), 10);
  if (!Number.isFinite(n) || String(n) !== String(value).trim()) {
    fatal(`Invalid --${name}: ${value} (expected integer)`);
  }
  if (n < min) fatal(`Invalid --${name}: ${value} (min ${min})`);
  return n;
}

function parseYYYYMMDD(s) {
  if (typeof s !== 'string' || !/^\d{8}$/.test(s)) return null;
  const year = Number.parseInt(s.slice(0, 4), 10);
  const month = Number.parseInt(s.slice(4, 6), 10);
  const day = Number.parseInt(s.slice(6, 8), 10);
  const d = new Date(Date.UTC(year, month - 1, day));
  if (
    d.getUTCFullYear() !== year ||
    d.getUTCMonth() !== month - 1 ||
    d.getUTCDate() !== day
  ) {
    return null;
  }
  return d;
}

function fmtYYYYMMDD(d) {
  const y = String(d.getUTCFullYear()).padStart(4, '0');
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${dd}`;
}

function listDates(startYYYYMMDD, endYYYYMMDD) {
  const startD = parseYYYYMMDD(startYYYYMMDD);
  const endD = parseYYYYMMDD(endYYYYMMDD);
  if (!startD) fatal(`Invalid --start: ${startYYYYMMDD} (expected YYYYMMDD)`);
  if (!endD) fatal(`Invalid --end: ${endYYYYMMDD} (expected YYYYMMDD)`);
  if (startD.getTime() > endD.getTime()) fatal(`Invalid date range: start > end (${startYYYYMMDD} > ${endYYYYMMDD})`);

  const out = [];
  for (let d = startD; d.getTime() <= endD.getTime(); d = new Date(d.getTime() + 86_400_000)) {
    out.push(fmtYYYYMMDD(d));
  }
  return out;
}

function normalizeSymbolForPath(symbol) {
  return String(symbol || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function currentHeapLimitMB() {
  const heapStats = v8.getHeapStatistics();
  return Math.floor(heapStats.heap_size_limit / 1024 / 1024);
}

function buildNodeOptions(existing, heapMB) {
  const parts = String(existing || '')
    .split(/\s+/)
    .map(s => s.trim())
    .filter(Boolean)
    .filter(s => !s.startsWith('--max-old-space-size='));
  parts.push(`--max-old-space-size=${heapMB}`);
  return parts.join(' ');
}

function resolveDayFiles({ exchange, stream, symbolSlug, date }) {
  const curatedDir = `data/curated/exchange=${exchange}/stream=${stream}/symbol=${symbolSlug}/date=${date}`;
  const legacyDir = `data/exchange=${exchange}/stream=${stream}/symbol=${symbolSlug}/date=${date}`;

  /** @type {Array<{parquetPath: string, metaCandidates: string[]}>} */
  const candidates = [
    {
      parquetPath: `${curatedDir}/data.parquet`,
      metaCandidates: [`${curatedDir}/meta.json`, `${curatedDir}/data.parquet.meta.json`]
    },
    {
      parquetPath: `${legacyDir}/data.parquet`,
      metaCandidates: [`${legacyDir}/meta.json`, `${legacyDir}/data.parquet.meta.json`]
    },
    {
      parquetPath: `data/test/${symbolSlug}_${date}.parquet`,
      metaCandidates: [
        `data/test/${symbolSlug}_${date}_meta.json`,
        `data/test/${symbolSlug}_${date}.parquet.meta.json`
      ]
    },
    {
      parquetPath: `data/sprint2/${symbolSlug}_${date}.parquet`,
      metaCandidates: [
        `data/sprint2/${symbolSlug}_${date}_meta.json`,
        `data/sprint2/${symbolSlug}_${date}.parquet.meta.json`
      ]
    },
    {
      parquetPath: `data/${symbolSlug}_${date}.parquet`,
      metaCandidates: [`data/${symbolSlug}_${date}_meta.json`, `data/${symbolSlug}_${date}.parquet.meta.json`]
    }
  ];

  for (const c of candidates) {
    if (!existsSync(c.parquetPath)) continue;
    for (const metaPath of c.metaCandidates) {
      if (existsSync(metaPath)) {
        return { parquetPath: c.parquetPath, metaPath };
      }
    }
  }

  return null;
}

async function readJSON(filePath) {
  const txt = await readFile(filePath, 'utf8');
  return JSON.parse(txt);
}

function safeTimestamp() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

async function main() {
  const diagTiming = process.env.QUANTLAB_DIAG_TIMING === 'true';
  const t0 = diagTiming ? Date.now() : 0;
  const checkpoint = diagTiming
    ? (name) => {
      const elapsed = Date.now() - t0;
      console.log(`[Timing] ${name} elapsed_ms_since_start=${elapsed}`);
    }
    : () => {};
  checkpoint('t0');

  const args = parseArgs(process.argv);
  if (args.help) printHelp(0);

  const exchange = String(args.exchange || 'binance').trim();
  const symbol = String(args.symbol || '').trim();
  const stream = String(args.stream || '').trim();
  const start = String(args.start || '').trim();
  const end = String(args.end || '').trim();
  const heapMB = args.heapMB === undefined ? 6144 : toInt('heapMB', args.heapMB, { min: 256 });
  const mode = String(args.mode || 'smoke').trim();
  const smokeMaxRowsPerDay =
    args.smokeMaxRowsPerDay === undefined
      ? 200000
      : toInt('smokeMaxRowsPerDay', args.smokeMaxRowsPerDay, { min: 1 });
  const smokeSlice = String(args.smokeSlice || 'head').trim();
  const progressEvery = args.progressEvery === undefined ? 1 : toInt('progressEvery', args.progressEvery, { min: 1 });

  if (!VALID_EXCHANGES.has(exchange)) fatal(`Invalid --exchange: ${exchange} (expected: binance|bybit|okx)`);
  if (!symbol) fatal('Missing required --symbol');
  if (!stream) fatal('Missing required --stream');
  if (!start) fatal('Missing required --start');
  if (!end) fatal('Missing required --end');
  if (!VALID_MODES.has(mode)) fatal(`Invalid --mode: ${mode} (expected: smoke|acceptance)`);
  if (!VALID_SMOKE_SLICES.has(smokeSlice)) fatal(`Invalid --smokeSlice: ${smokeSlice} (expected: head|tail|head_tail)`);

  // Date list + acceptance guardrail
  const dates = listDates(start, end);
  if (mode === 'acceptance' && dates.length < 2) {
    fatal(`--mode acceptance requires at least 2 days (got ${dates.length}: ${start}-${end})`);
  }

  // Permutation test: explicit flag overrides env; otherwise env can disable.
  let permutationEnabled = true;
  let permutationSource = 'DEFAULT';

  if (args.permutationTest !== undefined) {
    const v = String(args.permutationTest).trim().toLowerCase();
    if (!VALID_PERMUTATION.has(v)) fatal(`Invalid --permutationTest: ${args.permutationTest} (expected: on|off)`);
    permutationEnabled = v === 'on';
    permutationSource = 'CLI';
  } else if (process.env.DISCOVERY_PERMUTATION_TEST === 'false') {
    permutationEnabled = false;
    permutationSource = 'ENV';
  } else if (process.env.DISCOVERY_PERMUTATION_TEST === 'true') {
    permutationEnabled = true;
    permutationSource = 'ENV';
  }

  // Heap check and optional re-exec (only if current heap limit is below requested).
  const heapLimitMB = currentHeapLimitMB();
  if (heapLimitMB < heapMB && process.env.QUANTLAB_HEAP_REEXEC !== '1') {
    const nextNodeOptions = buildNodeOptions(process.env.NODE_OPTIONS, heapMB);

    // Preserve user execArgv but ensure heap is set. (time -v may not account child RSS;
    // recommended invocation is to set NODE_OPTIONS externally so this path is not used.)
    const execArgv = process.execArgv
      .filter(a => !a.startsWith('--max-old-space-size='))
      .concat([`--max-old-space-size=${heapMB}`]);

    const childEnv = {
      ...process.env,
      QUANTLAB_HEAP_REEXEC: '1',
      NODE_OPTIONS: nextNodeOptions
    };

    if (permutationSource === 'CLI') {
      childEnv.DISCOVERY_PERMUTATION_TEST = permutationEnabled ? 'true' : 'false';
    }

    console.log(`[Heap] current=${heapLimitMB}MB requested=${heapMB}MB -> re-exec with NODE_OPTIONS="${nextNodeOptions}"`);
    const child = spawn(process.execPath, [...execArgv, ...process.argv.slice(1)], {
      stdio: 'inherit',
      env: childEnv
    });
    child.on('exit', (code) => process.exit(code ?? 0));
    return;
  }

  // Ensure DISCOVERY_PERMUTATION_TEST is set BEFORE importing pipeline modules.
  // (DISCOVERY_CONFIG reads it at module-load time.)
  if (permutationSource === 'CLI') {
    process.env.DISCOVERY_PERMUTATION_TEST = permutationEnabled ? 'true' : 'false';
  }

  const symbolSlug = normalizeSymbolForPath(symbol);
  if (!symbolSlug) fatal(`Invalid --symbol: ${symbol} (cannot derive path slug)`);

  // Header log (required)
  console.log('='.repeat(80));
  console.log('MULTI-DAY DISCOVERY RUNNER');
  console.log('='.repeat(80));
  console.log(`mode:             ${mode}`);
  console.log(`exchange:          ${exchange}`);
  console.log(`stream:            ${stream}`);
  console.log(`symbol:            ${symbol}`);
  console.log(`date_range:        ${start}..${end} (${dates.length} day(s))`);
  console.log(`permutation_test:  ${permutationEnabled ? 'ON' : 'OFF'} (${permutationSource})`);
  if (process.env.DISCOVERY_PERMUTATION_TEST === 'false' && permutationSource !== 'CLI') {
    console.log(`permutation_note:  DISABLED via env DISCOVERY_PERMUTATION_TEST=false`);
  }
  console.log(`heapMB:            ${heapMB}`);
  console.log(`heap_limit_mb:     ${heapLimitMB}`);
  console.log(`progressEvery:     ${progressEvery}`);
  console.log('='.repeat(80));

  // Resolve parquet/meta paths for each day
  console.log('');
  console.log('[Dataset] Resolving day files...');
  /** @type {Array<{date: string, parquetPath: string, metaPath: string, meta: any}>} */
  const resolved = [];
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    const found = resolveDayFiles({ exchange, stream, symbolSlug, date });
    if (!found) {
      console.error('');
      console.error(`[Dataset] Missing files for date=${date}`);
      console.error(`[Dataset] Tried patterns (first existing parquet wins, requires meta):`);
      console.error(`  data/curated/exchange=${exchange}/stream=${stream}/symbol=${symbolSlug}/date=${date}/data.parquet + meta.json`);
      console.error(`  data/exchange=${exchange}/stream=${stream}/symbol=${symbolSlug}/date=${date}/data.parquet + meta.json`);
      console.error(`  data/test/${symbolSlug}_${date}.parquet + ${symbolSlug}_${date}_meta.json`);
      console.error(`  data/sprint2/${symbolSlug}_${date}.parquet + ${symbolSlug}_${date}_meta.json`);
      console.error(`  data/${symbolSlug}_${date}.parquet + ${symbolSlug}_${date}_meta.json`);
      process.exit(2);
    }

    const meta = await readJSON(found.metaPath);
    if (meta && typeof meta.stream_type === 'string' && meta.stream_type !== stream) {
      fatal(`Meta stream_type mismatch for date=${date}: meta.stream_type=${meta.stream_type} != --stream=${stream}`, 2);
    }

    resolved.push({ date, parquetPath: found.parquetPath, metaPath: found.metaPath, meta });

    const n = i + 1;
    if (n === 1 || n === dates.length || (n % progressEvery) === 0) {
      console.log(`[Dataset] progress ${n}/${dates.length}: date=${date} parquet=${path.basename(found.parquetPath)} meta=${path.basename(found.metaPath)}`);
    }
  }

  // Total rows from meta (best-effort; does not affect pipeline).
  const totalRows = resolved.reduce((acc, r) => acc + (Number.isFinite(r.meta?.rows) ? r.meta.rows : 0), 0);
  console.log(`[Dataset] resolved ${resolved.length} day(s); meta.rows_total=${totalRows || 'N/A'}`);
  checkpoint('t_resolve_files_done');

  if (mode === 'smoke') {
    console.log(`[SmokeCap] enabled=true maxRowsPerDay=${smokeMaxRowsPerDay} slice=${smokeSlice}`);
    for (const r of resolved) {
      const total = Number.isFinite(r.meta?.rows) ? r.meta.rows : null;
      const used = total === null ? smokeMaxRowsPerDay : Math.min(total, smokeMaxRowsPerDay);

      let usedHead = 0;
      let usedTail = 0;
      if (smokeSlice === 'head') {
        usedHead = used;
      } else if (smokeSlice === 'tail') {
        usedTail = used;
      } else {
        const headTarget = Math.floor(smokeMaxRowsPerDay / 2);
        const tailTarget = smokeMaxRowsPerDay - headTarget;
        if (total === null) {
          usedHead = headTarget;
          usedTail = tailTarget;
        } else if (total <= headTarget) {
          usedHead = total;
          usedTail = 0;
        } else if (total <= smokeMaxRowsPerDay) {
          usedHead = headTarget;
          usedTail = total - headTarget;
        } else {
          usedHead = headTarget;
          usedTail = tailTarget;
        }
      }

      console.log(`[SmokeCap] date=${r.date} rows_used_head=${usedHead} rows_used_tail=${usedTail} rows_total=${total === null ? 'UNKNOWN' : total}`);
    }
  }

  const outputRoot = 'runs/multi-day-discovery';
  const runTs = safeTimestamp();
  const permTag = permutationEnabled ? 'perm-on' : 'perm-off';
  const runDirName = `${runTs}_${exchange}_${stream}_${symbolSlug}_${start}_${end}_${mode}_${permTag}`;
  const outputDir = path.join(outputRoot, runDirName);
  await mkdir(outputDir, { recursive: true });

  const files = resolved.map(r => ({ parquetPath: r.parquetPath, metaPath: r.metaPath }));

  // Import pipeline modules AFTER env is ready
  const [{ EdgeDiscoveryPipeline }, { EdgeRegistry }] = await Promise.all([
    import('../core/edge/discovery/EdgeDiscoveryPipeline.js'),
    import('../core/edge/EdgeRegistry.js')
  ]);
  checkpoint('t_import_modules_done');

  console.log('');
  console.log(`[Run] output_dir=${outputDir}`);
  console.log(`[Run] starting EdgeDiscoveryPipeline.runMultiDayStreaming(files=${files.length})...`);
  console.log('');

  const registry = new EdgeRegistry();
  const pipeline = new EdgeDiscoveryPipeline({
    registry,
    loader: mode === 'smoke' ? { maxRowsPerDay: smokeMaxRowsPerDay, smokeSlice } : undefined
  });

  const startedAt = Date.now();
  let result;
  try {
    checkpoint('t_pipeline_start');
    result = await pipeline.runMultiDayStreaming(files, symbol);
  } catch (err) {
    console.error('');
    console.error('[Run] discovery_error');
    console.error(err && err.stack ? err.stack : String(err));
    if (err && (err.code === 'SMOKE_CAP_GUARD_FAIL' || err.exit_code === 2)) process.exit(2);
    process.exit(1);
  }
  checkpoint('t_pipeline_done');
  const durationMs = Date.now() - startedAt;

  console.log('');
  console.log('='.repeat(80));
  console.log('DISCOVERY SUMMARY');
  console.log('='.repeat(80));
  console.log(`patterns_scanned:          ${result.patternsScanned}`);
  console.log(`patterns_tested_significant:${result.patternsTestedSignificant}`);
  console.log(`edge_candidates_generated: ${result.edgeCandidatesGenerated}`);
  console.log(`edge_candidates_registered:${result.edgeCandidatesRegistered}`);
  console.log(`metadata.dataRowCount:     ${result.metadata?.dataRowCount ?? 'N/A'}`);
  console.log(`metadata.regimesUsed:      ${result.metadata?.regimesUsed ?? 'N/A'}`);
  console.log(`metadata.filesLoaded:      ${result.metadata?.filesLoaded ?? files.length}`);
  console.log(`duration_ms:              ${durationMs}`);
  console.log('='.repeat(80));

  // Persist minimal run report for evidence/repro.
  const reportPath = path.join(outputDir, `discovery-report-${runTs}.json`);
  const edgesPath = path.join(outputDir, `edges-discovered-${runTs}.json`);

  const diagnosticNotes = [];
  // NOTE: Artifact was created previously for diagnosis (not created by this runner).
  if (
    existsSync('data/test/adausdt_20260204.parquet') &&
    existsSync('data/test/adausdt_20260204_meta.json')
  ) {
    diagnosticNotes.push(
      'artifact_created_for_diag: data/test/adausdt_20260204.parquet + data/test/adausdt_20260204_meta.json (pre-existing)'
    );
  }

  if (result.patternsScanned === 0) {
    const minSupport = pipeline.scanner?.minSupport;
    const returnThreshold = pipeline.scanner?.returnThreshold;
    console.log(
      `[Runner] no_patterns: patterns_scanned=0 (scanner.minSupport=${minSupport} scanner.returnThreshold=${returnThreshold})`
    );
    console.log('[Runner] note: threshold/config may be restrictive');
    diagnosticNotes.push(
      `no_patterns: patterns_scanned=0 (scanner.minSupport=${minSupport} scanner.returnThreshold=${returnThreshold})`
    );
    diagnosticNotes.push('note: threshold/config may be restrictive');
  }

  const report = {
    step: 'multi_day_discovery',
    timestamp: new Date().toISOString(),
    mode,
    exchange,
    stream,
    symbol,
    symbolSlug,
    dateRange: { start, end, days: dates.length },
    permutationTest: { enabled: permutationEnabled, source: permutationSource },
    heap: { requestedMB: heapMB, heapLimitMB },
    progressEvery,
    diagnosticNotes,
    files: resolved.map(r => ({
      date: r.date,
      parquetPath: r.parquetPath,
      metaPath: r.metaPath,
      meta: {
        rows: r.meta?.rows,
        schema_version: r.meta?.schema_version,
        stream_type: r.meta?.stream_type,
        day_quality: r.meta?.day_quality,
        sha256: r.meta?.sha256
      }
    })),
    metaRowsTotal: totalRows || null,
    result,
    durationMs
  };

  await writeFile(reportPath, JSON.stringify(report, null, 2));
  console.log(`[Run] report_saved=${reportPath}`);

  if (Array.isArray(result.edges) && result.edges.length > 0) {
    const edgesData = {
      edges: result.edges.map(e => (typeof e.toJSON === 'function' ? e.toJSON() : e)),
      metadata: {
        discoveryTimestamp: new Date().toISOString(),
        symbol,
        daysProcessed: dates.length,
        totalCandidates: result.edges.length
      }
    };
    await writeFile(edgesPath, JSON.stringify(edgesData, null, 2));
    console.log(`[Run] edges_saved=${edgesPath}`);
  }
}

main().catch(err => {
  console.error('Fatal error:', err && err.stack ? err.stack : String(err));
  process.exit(1);
});
