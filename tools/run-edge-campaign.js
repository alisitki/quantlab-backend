#!/usr/bin/env node

import "dotenv/config";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";

const VALID_DAY_QUALITY = new Set(["GOOD"]);
const VALID_SMOKE_SLICE = new Set(["head", "tail", "head_tail"]);
const DEFAULT_STATE_BUCKET = process.env.S3_COMPACT_BUCKET || "quantlab-compact";
const DEFAULT_STATE_KEY = process.env.S3_COMPACT_STATE_KEY || "compacted/_state.json";

function printHelp(exitCode = 0) {
  const msg = `
tools/run-edge-campaign.js

Required:
  --exchange <name>
  --symbol <e.g. ADA/USDT>
  --stream <name>
  --dayQuality <GOOD>

Common:
  --maxCandidates <int>            (default: 3)
  --progressEvery <int>            (default: 1)

Smoke:
  --smokeTimeoutS <int>            (default: 300)
  --smokeHeapMB <int>              (default: 6144)
  --smokeMaxRowsPerDay <int>       (default: 200000)
  --smokeSlice <head|tail|head_tail> (default: head_tail)
  --smokeParallel <int>            (default: 2)

Acceptance:
  --acceptanceTimeoutS <int>       (default: 3600)
  --acceptanceHeapMB <int>         (default: 6144)
  --acceptanceParallel <int>       (default: 1)
  --forceAcceptanceTop1 <true|false> (default: true)

Determinism:
  --runDeterminism <true|false>    (default: false)

Examples:
  node tools/run-edge-campaign.js \
    --exchange binance --symbol ADA/USDT --stream bbo --dayQuality GOOD
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
    const token = tokens[i];
    if (token === "--help" || token === "-h") {
      out.help = true;
      continue;
    }
    if (!token.startsWith("--")) {
      fatal(`Unexpected arg: ${token}`);
    }

    const eqIdx = token.indexOf("=");
    if (eqIdx !== -1) {
      out[token.slice(2, eqIdx)] = token.slice(eqIdx + 1);
      continue;
    }

    const key = token.slice(2);
    const next = tokens[i + 1];
    if (!next || next.startsWith("--")) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    i++;
  }

  const allowed = new Set([
    "help",
    "exchange",
    "symbol",
    "stream",
    "dayQuality",
    "maxCandidates",
    "progressEvery",
    "smokeTimeoutS",
    "smokeHeapMB",
    "smokeMaxRowsPerDay",
    "smokeSlice",
    "smokeParallel",
    "acceptanceTimeoutS",
    "acceptanceHeapMB",
    "acceptanceParallel",
    "forceAcceptanceTop1",
    "runDeterminism"
  ]);

  for (const key of Object.keys(out)) {
    if (!allowed.has(key)) {
      fatal(`Unknown flag: --${key}`);
    }
  }

  return out;
}

function toInt(name, value, min = 1) {
  const n = Number.parseInt(String(value), 10);
  if (!Number.isFinite(n) || String(n) !== String(value).trim()) {
    fatal(`Invalid --${name}: ${value} (expected integer)`);
  }
  if (n < min) {
    fatal(`Invalid --${name}: ${value} (min ${min})`);
  }
  return n;
}

function toBool(name, value, defaultValue) {
  if (value === undefined) return defaultValue;
  const s = String(value).trim().toLowerCase();
  if (s === "true") return true;
  if (s === "false") return false;
  fatal(`Invalid --${name}: ${value} (expected true|false)`);
}

function normalizeSymbol(symbol) {
  return String(symbol || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function todayUTCYYYYMMDD() {
  const d = new Date();
  const y = String(d.getUTCFullYear()).padStart(4, "0");
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

function nowUTCHHMMSS() {
  const d = new Date();
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${hh}${mm}${ss}`;
}

function parseDateYYYYMMDD(s) {
  if (!/^\d{8}$/.test(s)) return null;
  const y = Number.parseInt(s.slice(0, 4), 10);
  const m = Number.parseInt(s.slice(4, 6), 10);
  const d = Number.parseInt(s.slice(6, 8), 10);
  const dt = new Date(Date.UTC(y, m - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== m - 1 || dt.getUTCDate() !== d) return null;
  return dt;
}

function isConsecutiveDay(a, b) {
  const da = parseDateYYYYMMDD(a);
  const db = parseDateYYYYMMDD(b);
  if (!da || !db) return false;
  return db.getTime() - da.getTime() === 86_400_000;
}

async function fileExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function mkdirp(p) {
  await fs.mkdir(p, { recursive: true });
}

async function readJSON(p) {
  const raw = await fs.readFile(p, "utf8");
  return JSON.parse(raw);
}

async function writeText(p, content) {
  await mkdirp(path.dirname(p));
  await fs.writeFile(p, content, "utf8");
}

async function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const h = createHash("sha256");
    const rs = createReadStream(filePath);
    rs.on("error", reject);
    rs.on("data", (chunk) => h.update(chunk));
    rs.on("end", () => resolve(h.digest("hex")));
  });
}

async function trySha256File(filePath) {
  if (!(await fileExists(filePath))) return null;
  try {
    return await sha256File(filePath);
  } catch {
    return null;
  }
}

function quoteBash(arg) {
  const s = String(arg);
  return `'${s.replace(/'/g, `"'"'`)}'`;
}

function createCompactS3Client() {
  const config = {
    endpoint: process.env.S3_COMPACT_ENDPOINT || undefined,
    region: process.env.S3_COMPACT_REGION || "us-east-1",
    forcePathStyle: true
  };

  if (process.env.S3_COMPACT_ACCESS_KEY && process.env.S3_COMPACT_SECRET_KEY) {
    config.credentials = {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
    };
  }

  return new S3Client(config);
}

async function readCompactionStateFromS3({ bucket, key }) {
  const s3 = createCompactS3Client();
  const res = await s3.send(new GetObjectCommand({
    Bucket: bucket,
    Key: key
  }));
  const body = await res.Body.transformToString();
  return JSON.parse(body);
}

async function runProcess({ cmd, args, cwd, stdoutPath, stderrPath }) {
  await mkdirp(path.dirname(stdoutPath));
  await mkdirp(path.dirname(stderrPath));

  const stdout = createWriteStream(stdoutPath, { flags: "w" });
  const stderr = createWriteStream(stderrPath, { flags: "w" });

  return new Promise((resolve) => {
    const child = spawn(cmd, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });
    child.stdout.pipe(stdout);
    child.stderr.pipe(stderr);

    child.on("error", (err) => {
      stderr.write(`spawn_error: ${String(err)}\n`);
      stdout.end(() => {
        stderr.end(() => resolve({ code: 1 }));
      });
    });

    child.on("close", (code) => {
      stdout.end(() => {
        stderr.end(() => resolve({ code: code ?? 1 }));
      });
    });
  });
}

function parseElapsedToSeconds(elapsedRaw) {
  if (!elapsedRaw) return null;
  const parts = elapsedRaw.trim().split(":").map((x) => Number.parseFloat(x));
  if (parts.some((x) => !Number.isFinite(x))) return null;
  if (parts.length === 3) return Number((parts[0] * 3600 + parts[1] * 60 + parts[2]).toFixed(2));
  if (parts.length === 2) return Number((parts[0] * 60 + parts[1]).toFixed(2));
  if (parts.length === 1) return Number(parts[0].toFixed(2));
  return null;
}

function parseRunLogs(stdoutText, timeVText) {
  const summaryReached = /^DISCOVERY SUMMARY$/m.test(stdoutText);

  const patternsMatch = stdoutText.match(/^patterns_scanned:\s*(\d+)/m);
  const patternsScanned = patternsMatch ? Number.parseInt(patternsMatch[1], 10) : null;

  const dataRowMatch = stdoutText.match(/^metadata\.dataRowCount:\s*(\d+)/m);
  const dataRowCount = dataRowMatch ? Number.parseInt(dataRowMatch[1], 10) : null;

  const reportMatch = stdoutText.match(/^\[Run\] report_saved=(.+)$/m);
  const reportSavedSource = reportMatch ? reportMatch[1].trim() : null;

  const edgesMatch = stdoutText.match(/^\[Run\] edges_saved=(.+)$/m);
  const edgesSavedSource = edgesMatch ? edgesMatch[1].trim() : null;

  const elapsedMatch = timeVText.match(/Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*(.+)$/m);
  const wallS = parseElapsedToSeconds(elapsedMatch ? elapsedMatch[1] : null);

  const rssMatch = timeVText.match(/Maximum resident set size \(kbytes\):\s*(\d+)/m);
  const maxRssKb = rssMatch ? Number.parseInt(rssMatch[1], 10) : null;

  const exitStatusMatch = timeVText.match(/Exit status:\s*(\d+)/m);
  const timeExitStatus = exitStatusMatch ? Number.parseInt(exitStatusMatch[1], 10) : null;

  return {
    summaryReached,
    patternsScanned,
    dataRowCount,
    reportSavedSource,
    edgesSavedSource,
    wallS,
    maxRssKb,
    timeExitStatus
  };
}

async function runDiscoveryWithEvidence({
  cwd,
  runDir,
  commandBody
}) {
  await mkdirp(runDir);

  const cmdPath = path.join(runDir, "cmd.sh");
  const stdoutPath = path.join(runDir, "stdout.log");
  const stderrPath = path.join(runDir, "stderr.log");
  const timeVPath = path.join(runDir, "time-v.log");
  const exitPath = path.join(runDir, "exit_code.txt");

  const cmdScript = `set -euo pipefail\n${commandBody}\n`;
  await writeText(cmdPath, cmdScript);
  await fs.chmod(cmdPath, 0o755);

  const { code } = await runProcess({
    cmd: "/usr/bin/time",
    args: ["-v", "-o", timeVPath, "--", "bash", cmdPath],
    cwd,
    stdoutPath,
    stderrPath
  });

  await writeText(exitPath, `${code}\n`);

  const [stdoutText, timeVText] = await Promise.all([
    fs.readFile(stdoutPath, "utf8"),
    fs.readFile(timeVPath, "utf8")
  ]);

  const parsed = parseRunLogs(stdoutText, timeVText);

  return {
    exitCode: code,
    cmdPath,
    stdoutPath,
    stderrPath,
    timeVPath,
    exitPath,
    ...parsed
  };
}

async function runQueue(items, concurrency, worker) {
  const limit = Math.max(1, concurrency);
  const results = new Array(items.length);
  let cursor = 0;

  async function next() {
    while (true) {
      const idx = cursor;
      cursor += 1;
      if (idx >= items.length) return;
      results[idx] = await worker(items[idx], idx);
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(limit, items.length); i++) {
    workers.push(next());
  }
  await Promise.all(workers);
  return results;
}

function toTsvLine(cols) {
  return cols.map((v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/\t|\n|"/.test(s)) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  }).join("\t");
}

function toRel(packRoot, absPath) {
  return path.relative(packRoot, absPath).replace(/\\/g, "/");
}

function boolWord(v) {
  return v ? "true" : "false";
}

function shaEqualWord(v) {
  if (v === true) return "true";
  if (v === false) return "false";
  return "UNKNOWN";
}

async function copyIfExists(src, dest) {
  if (!src) return false;
  if (!(await fileExists(src))) return false;
  await mkdirp(path.dirname(dest));
  await fs.copyFile(src, dest);
  return true;
}

function sanitizeWindow(start, end) {
  return `${start}_${end}`;
}

function scoreRowsTotal(rowsA, rowsB) {
  if (!Number.isFinite(rowsA) || !Number.isFinite(rowsB)) return Number.POSITIVE_INFINITY;
  return rowsA + rowsB;
}

function parsePartitionKey(rawKey) {
  const key = String(rawKey || "");
  const parts = key.split("/");
  if (parts.length !== 4) return null;

  if (parts[0].startsWith("exchange=")) {
    const [ex, st, sy, dt] = parts;
    const exchange = ex.startsWith("exchange=") ? ex.slice("exchange=".length) : "";
    const stream = st.startsWith("stream=") ? st.slice("stream=".length) : "";
    const symbol = sy.startsWith("symbol=") ? sy.slice("symbol=".length) : "";
    const date = dt.startsWith("date=") ? dt.slice("date=".length) : "";
    if (!exchange || !stream || !symbol || !parseDateYYYYMMDD(date)) return null;
    return { exchange, stream, symbol, date };
  }

  const [exchange, stream, symbol, date] = parts;
  if (!exchange || !stream || !symbol || !parseDateYYYYMMDD(date)) return null;
  return { exchange, stream, symbol, date };
}

function parseNumeric(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n;
}

async function gatherInventoryFromState({
  state,
  exchange,
  stream,
  symbolNorm,
  dayQuality,
  progressEvery
}) {
  const partitions = state?.partitions;
  if (!partitions || typeof partitions !== "object" || Array.isArray(partitions)) {
    throw new Error("state json missing partitions object");
  }

  const exchangeLc = exchange.toLowerCase();
  const streamLc = stream.toLowerCase();
  const symbolLc = symbolNorm.toLowerCase();

  const entries = Object.entries(partitions);
  const rows = [];
  let hit = 0;

  for (let i = 0; i < entries.length; i++) {
    const [partitionKey, metaRaw] = entries[i];
    const parsed = parsePartitionKey(partitionKey);
    if (!parsed) continue;
    if (parsed.exchange.toLowerCase() !== exchangeLc) continue;
    if (parsed.stream.toLowerCase() !== streamLc) continue;
    if (parsed.symbol.toLowerCase() !== symbolLc) continue;

    const meta = metaRaw && typeof metaRaw === "object" ? metaRaw : {};
    const dir = path.join(
      "data",
      "curated",
      `exchange=${parsed.exchange}`,
      `stream=${parsed.stream}`,
      `symbol=${parsed.symbol}`,
      `date=${parsed.date}`
    );
    const parquetPath = path.join(dir, "data.parquet");
    const metaPath = path.join(dir, "meta.json");

    const parquetExists = await fileExists(parquetPath);
    const metaExists = await fileExists(metaPath);

    const status = String(meta.status || "").toLowerCase();
    const dayQualityPost = meta.day_quality_post === undefined || meta.day_quality_post === null
      ? null
      : String(meta.day_quality_post);
    const eligible = status === "success" && dayQualityPost === dayQuality;

    rows.push({
      exchange: parsed.exchange,
      stream: parsed.stream,
      symbol: parsed.symbol,
      date: parsed.date,
      dir,
      parquetPath,
      metaPath,
      parquetExists,
      metaExists,
      status,
      dayQuality: dayQualityPost,
      rows: parseNumeric(meta.rows),
      totalSizeBytes: parseNumeric(meta.total_size_bytes),
      updatedAt: meta.updated_at ? String(meta.updated_at) : null,
      partitionKey,
      eligible
    });

    hit += 1;
    if (hit === 1 || hit % progressEvery === 0) {
      console.log(`[Inventory] progress matched=${hit}: date=${parsed.date} status=${status || "N/A"} dqp=${dayQualityPost || "N/A"} parquet=${parquetExists ? "Y" : "N"} meta=${metaExists ? "Y" : "N"}`);
    }
  }

  rows.sort((a, b) => a.date.localeCompare(b.date));
  return rows;
}

function buildConsecutiveWindows(inventoryRows) {
  const out = [];
  for (let i = 0; i < inventoryRows.length - 1; i++) {
    const a = inventoryRows[i];
    const b = inventoryRows[i + 1];
    if (!isConsecutiveDay(a.date, b.date)) continue;

    out.push({
      start: a.date,
      end: b.date,
      day1: a,
      day2: b,
      rowsTotal: scoreRowsTotal(a.rows, b.rows)
    });
  }
  return out;
}

function buildCampaignScopeDays(consecutiveWindows) {
  const s = new Set();
  for (const w of consecutiveWindows) {
    s.add(w.day1.date);
    s.add(w.day2.date);
  }
  return s;
}

async function buildCandidateRanking({ windows, maxCandidates }) {
  const sorted = [...windows].sort((x, y) => {
    if (x.rowsTotal !== y.rowsTotal) return x.rowsTotal - y.rowsTotal;
    return x.start.localeCompare(y.start);
  });

  const ranked = [];
  const proofs = [];

  for (const w of sorted) {
    const sha1 = await trySha256File(w.day1.parquetPath);
    const sha2 = await trySha256File(w.day2.parquetPath);
    const shaEqual = sha1 && sha2 ? sha1 === sha2 : null;

    const candidate = {
      start: w.start,
      end: w.end,
      day1: w.day1,
      day2: w.day2,
      rowsTotal: Number.isFinite(w.rowsTotal) ? w.rowsTotal : null,
      sha1,
      sha2,
      shaEqual,
      eligible: shaEqual !== true
    };
    ranked.push(candidate);

    proofs.push([
      `window=${w.start}..${w.end}`,
      `parquet1=${w.day1.parquetPath}`,
      `sha256_1=${sha1 || "N/A(local_missing_or_unreadable)"}`,
      `parquet2=${w.day2.parquetPath}`,
      `sha256_2=${sha2 || "N/A(local_missing_or_unreadable)"}`,
      `sha_equal=${shaEqual === null ? "UNKNOWN" : String(shaEqual)}`,
      ""
    ].join("\n"));
  }

  const selected = ranked.filter((r) => r.eligible).slice(0, maxCandidates);
  selected.forEach((c, idx) => {
    c.rank = idx + 1;
  });

  return { ranked, selected, proofText: proofs.join("\n") };
}

function deriveRiskNote({ campaignPass, selectedCandidates, acceptanceRecords }) {
  if (campaignPass) return "none";
  if (selectedCandidates.length === 0) return "no candidate window selected from state inventory";
  const anyAcceptance = acceptanceRecords.some((r) => r.acceptanceRan);
  if (!anyAcceptance) return "no acceptance run triggered by policy";
  return "no acceptance PASS window (patterns_scanned>0 and edges_saved not achieved)";
}

function safeValue(v) {
  if (v === null || v === undefined) return "";
  return v;
}

async function runShellCommandEvidence({ cwd, runDir, commandBody }) {
  const cmdPath = path.join(runDir, "cmd.sh");
  const cmdScript = `set -euo pipefail\n${commandBody}\n`;
  await writeText(cmdPath, cmdScript);
  await fs.chmod(cmdPath, 0o755);
  return runDiscoveryWithEvidence({ cwd, runDir, commandBody });
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    printHelp(0);
  }

  const exchange = String(args.exchange || "").trim();
  const symbol = String(args.symbol || "").trim();
  const stream = String(args.stream || "").trim();
  const dayQuality = String(args.dayQuality || "").trim();

  if (!exchange) fatal("Missing required --exchange");
  if (!symbol) fatal("Missing required --symbol");
  if (!stream) fatal("Missing required --stream");
  if (!dayQuality) fatal("Missing required --dayQuality");
  if (!VALID_DAY_QUALITY.has(dayQuality)) fatal(`Unsupported --dayQuality: ${dayQuality} (v0 supports GOOD only)`);

  const maxCandidates = args.maxCandidates === undefined ? 3 : toInt("maxCandidates", args.maxCandidates, 1);
  const progressEvery = args.progressEvery === undefined ? 1 : toInt("progressEvery", args.progressEvery, 1);

  const smokeTimeoutS = args.smokeTimeoutS === undefined ? 300 : toInt("smokeTimeoutS", args.smokeTimeoutS, 1);
  const smokeHeapMB = args.smokeHeapMB === undefined ? 6144 : toInt("smokeHeapMB", args.smokeHeapMB, 256);
  const smokeMaxRowsPerDay = args.smokeMaxRowsPerDay === undefined ? 200000 : toInt("smokeMaxRowsPerDay", args.smokeMaxRowsPerDay, 1);
  const smokeSlice = String(args.smokeSlice || "head_tail").trim();
  if (!VALID_SMOKE_SLICE.has(smokeSlice)) {
    fatal(`Invalid --smokeSlice: ${smokeSlice} (expected head|tail|head_tail)`);
  }
  const smokeParallel = args.smokeParallel === undefined ? 2 : toInt("smokeParallel", args.smokeParallel, 1);

  const acceptanceTimeoutS = args.acceptanceTimeoutS === undefined ? 3600 : toInt("acceptanceTimeoutS", args.acceptanceTimeoutS, 1);
  const acceptanceHeapMB = args.acceptanceHeapMB === undefined ? 6144 : toInt("acceptanceHeapMB", args.acceptanceHeapMB, 256);
  const acceptanceParallel = args.acceptanceParallel === undefined ? 1 : toInt("acceptanceParallel", args.acceptanceParallel, 1);
  const forceAcceptanceTop1 = toBool("forceAcceptanceTop1", args.forceAcceptanceTop1, true);

  const runDeterminism = toBool("runDeterminism", args.runDeterminism, false);

  const symbolNorm = normalizeSymbol(symbol);
  if (!symbolNorm) fatal(`Invalid --symbol: ${symbol}`);

  const ymd = todayUTCYYYYMMDD();
  const hms = nowUTCHHMMSS();
  const packName = `edge-campaign-v0-${exchange}_${stream}_${symbolNorm}_${ymd}_${hms}`;
  const packRoot = path.join("evidence", packName);

  if (await fileExists(packRoot)) {
    fatal(`Evidence pack already exists: ${packRoot}`);
  }

  const tarPath = path.join("evidence", `${packName}.tar.gz`);
  const shaPath = `${tarPath}.sha256`;
  const movedPath = path.join("evidence", `${packName}.moved_to.txt`);

  if (await fileExists(tarPath) || await fileExists(shaPath) || await fileExists(movedPath)) {
    fatal(`Finalize targets already exist for pack: ${packName}`);
  }

  await Promise.all([
    mkdirp(path.join(packRoot, "inventory")),
    mkdirp(path.join(packRoot, "sha256")),
    mkdirp(path.join(packRoot, "runs", "smoke")),
    mkdirp(path.join(packRoot, "runs", "acceptance")),
    mkdirp(path.join(packRoot, "determinism")),
    mkdirp(path.join(packRoot, "results")),
    mkdirp(path.join(packRoot, "artifacts", "acceptance"))
  ]);

  console.log("=".repeat(80));
  console.log("EDGE CAMPAIGN RUNNER v0");
  console.log("=".repeat(80));
  console.log(`exchange:                ${exchange}`);
  console.log(`stream:                  ${stream}`);
  console.log(`symbol:                  ${symbol}`);
  console.log(`symbolNorm:              ${symbolNorm}`);
  console.log(`dayQuality:              ${dayQuality}`);
  console.log(`maxCandidates:           ${maxCandidates}`);
  console.log(`smoke: timeout=${smokeTimeoutS}s heapMB=${smokeHeapMB} maxRowsPerDay=${smokeMaxRowsPerDay} slice=${smokeSlice} parallel=${smokeParallel}`);
  console.log(`acceptance: timeout=${acceptanceTimeoutS}s heapMB=${acceptanceHeapMB} parallel=${acceptanceParallel} forceTop1=${forceAcceptanceTop1}`);
  console.log(`runDeterminism:          ${runDeterminism}`);
  console.log(`packRoot:                ${packRoot}`);
  console.log("=".repeat(80));

  const stateBucket = DEFAULT_STATE_BUCKET;
  const stateKey = DEFAULT_STATE_KEY;
  const stateUri = `s3://${stateBucket}/${stateKey}`;
  console.log(`state_source:            ${stateUri}`);
  await writeText(path.join(packRoot, "inventory", "state_path.txt"), `${stateUri}\n`);

  let stateObj;
  try {
    stateObj = await readCompactionStateFromS3({ bucket: stateBucket, key: stateKey });
  } catch (err) {
    const msg = `FAIL state load error: ${err && err.message ? err.message : String(err)}`;
    await writeText(path.join(packRoot, "inventory", "daily_inventory.tsv"), "exchange\tstream\tsymbol\tdate\tstatus\tday_quality_post\teligible\trows\ttotal_size_bytes\tupdated_at\tparquet_exists\tmeta_exists\tparquet_relpath\tmeta_relpath\tpartition_key\n");
    await writeText(path.join(packRoot, "inventory", "candidates_ranked.tsv"), "rank\tstart\tend\trows_total\treason\n");
    await writeText(path.join(packRoot, "sha256", "candidate_sha256_proof.txt"), "N/A: state load failed\n");
    await writeText(path.join(packRoot, "results", "results.tsv"), "rank\twindow\tday_quality_ok\trows_total\tsha_equal\tsmoke_exit\tsmoke_patterns_scanned\tsmoke_dataRowCount\tsmoke_wall_s\tsmoke_max_rss_kb\tacceptance_ran\tacceptance_exit\tacceptance_patterns_scanned\tedges_saved\tacceptance_wall_s\tacceptance_max_rss_kb\treport_relpath\tedges_relpath\tdeterminism_on_on\tdeterminism_off_off\n");

    const summary = {
      pack: packName,
      campaign_pass: false,
      reason: msg,
      files: {
        state_path_relpath: "inventory/state_path.txt",
        inventory_relpath: "inventory/daily_inventory.tsv",
        candidates_relpath: "inventory/candidates_ranked.tsv",
        sha256_relpath: "sha256/candidate_sha256_proof.txt",
        results_relpath: "results/results.tsv"
      }
    };
    await writeText(path.join(packRoot, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);

    const readme = [
      "# Edge Campaign v0",
      "",
      "Campaign Result: FAIL",
      `Reason: ${msg}`,
      "",
      "## Evidence Relpaths",
      "- inventory/state_path.txt",
      "- inventory/daily_inventory.tsv",
      "- inventory/candidates_ranked.tsv",
      "- sha256/candidate_sha256_proof.txt",
      "- results/results.tsv",
      "- summary.json"
    ].join("\n");
    await writeText(path.join(packRoot, "README.md"), `${readme}\n`);

    await finalizePack({ packName, packRoot, tarPath, shaPath, movedPath });
    process.exit(2);
  }

  const inventoryRows = await gatherInventoryFromState({
    state: stateObj,
    exchange,
    stream,
    symbolNorm,
    dayQuality,
    progressEvery
  });
  const eligibleRows = inventoryRows.filter((r) => r.eligible);

  const inventoryHeader = [
    "exchange",
    "stream",
    "symbol",
    "date",
    "status",
    "day_quality_post",
    "eligible",
    "rows",
    "total_size_bytes",
    "updated_at",
    "parquet_exists",
    "meta_exists",
    "parquet_relpath",
    "meta_relpath",
    "partition_key"
  ];
  const inventoryLines = [toTsvLine(inventoryHeader)];
  for (const r of inventoryRows) {
    inventoryLines.push(toTsvLine([
      r.exchange,
      r.stream,
      r.symbol,
      r.date,
      safeValue(r.status),
      safeValue(r.dayQuality),
      boolWord(r.eligible),
      safeValue(r.rows),
      safeValue(r.totalSizeBytes),
      safeValue(r.updatedAt),
      boolWord(r.parquetExists),
      boolWord(r.metaExists),
      path.relative(process.cwd(), r.parquetPath).replace(/\\/g, "/"),
      path.relative(process.cwd(), r.metaPath).replace(/\\/g, "/"),
      r.partitionKey
    ]));
  }
  await writeText(path.join(packRoot, "inventory", "daily_inventory.tsv"), `${inventoryLines.join("\n")}\n`);

  if (eligibleRows.length === 0) {
    const msg = `FAIL no state-eligible days for ${exchange}/${stream}/${symbolNorm} (status=success AND day_quality_post=${dayQuality})`;

    await writeText(path.join(packRoot, "inventory", "candidates_ranked.tsv"), "rank\tstart\tend\trows_total\treason\n");
    await writeText(path.join(packRoot, "sha256", "candidate_sha256_proof.txt"), "N/A: no state-eligible days\n");
    await writeText(path.join(packRoot, "results", "results.tsv"), "rank\twindow\tday_quality_ok\trows_total\tsha_equal\tsmoke_exit\tsmoke_patterns_scanned\tsmoke_dataRowCount\tsmoke_wall_s\tsmoke_max_rss_kb\tacceptance_ran\tacceptance_exit\tacceptance_patterns_scanned\tedges_saved\tacceptance_wall_s\tacceptance_max_rss_kb\treport_relpath\tedges_relpath\tdeterminism_on_on\tdeterminism_off_off\n");

    const summary = {
      pack: packName,
      campaign_pass: false,
      reason: msg,
      files: {
        state_path_relpath: "inventory/state_path.txt",
        inventory_relpath: "inventory/daily_inventory.tsv",
        candidates_relpath: "inventory/candidates_ranked.tsv",
        sha256_relpath: "sha256/candidate_sha256_proof.txt",
        results_relpath: "results/results.tsv"
      }
    };
    await writeText(path.join(packRoot, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);

    const readme = [
      "# Edge Campaign v0",
      "",
      "Campaign Result: FAIL",
      `Reason: ${msg}`,
      "",
      "## Evidence Relpaths",
      "- inventory/state_path.txt",
      "- inventory/daily_inventory.tsv",
      "- inventory/candidates_ranked.tsv",
      "- sha256/candidate_sha256_proof.txt",
      "- results/results.tsv",
      "- summary.json"
    ].join("\n");
    await writeText(path.join(packRoot, "README.md"), `${readme}\n`);

    await finalizePack({ packName, packRoot, tarPath, shaPath, movedPath });
    process.exit(2);
  }

  const consecutiveWindows = buildConsecutiveWindows(eligibleRows);
  if (consecutiveWindows.length === 0) {
    const msg = `FAIL no 2-day consecutive windows in state-eligible days for ${exchange}/${stream}/${symbolNorm}`;

    await writeText(path.join(packRoot, "inventory", "candidates_ranked.tsv"), "rank\tstart\tend\trows_total\treason\n");
    await writeText(path.join(packRoot, "sha256", "candidate_sha256_proof.txt"), "N/A: no consecutive 2-day window\n");
    await writeText(path.join(packRoot, "results", "results.tsv"), "rank\twindow\tday_quality_ok\trows_total\tsha_equal\tsmoke_exit\tsmoke_patterns_scanned\tsmoke_dataRowCount\tsmoke_wall_s\tsmoke_max_rss_kb\tacceptance_ran\tacceptance_exit\tacceptance_patterns_scanned\tedges_saved\tacceptance_wall_s\tacceptance_max_rss_kb\treport_relpath\tedges_relpath\tdeterminism_on_on\tdeterminism_off_off\n");

    const summary = {
      pack: packName,
      campaign_pass: false,
      reason: msg,
      files: {
        state_path_relpath: "inventory/state_path.txt",
        inventory_relpath: "inventory/daily_inventory.tsv",
        candidates_relpath: "inventory/candidates_ranked.tsv",
        sha256_relpath: "sha256/candidate_sha256_proof.txt",
        results_relpath: "results/results.tsv"
      }
    };
    await writeText(path.join(packRoot, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);

    const readme = [
      "# Edge Campaign v0",
      "",
      "Campaign Result: FAIL",
      `Reason: ${msg}`,
      "",
      "## Evidence Relpaths",
      "- inventory/state_path.txt",
      "- inventory/daily_inventory.tsv",
      "- inventory/candidates_ranked.tsv",
      "- sha256/candidate_sha256_proof.txt",
      "- results/results.tsv",
      "- summary.json"
    ].join("\n");
    await writeText(path.join(packRoot, "README.md"), `${readme}\n`);

    await finalizePack({ packName, packRoot, tarPath, shaPath, movedPath });
    process.exit(2);
  }

  const qualityWindows = consecutiveWindows;

  const { ranked, selected, proofText } = await buildCandidateRanking({ windows: qualityWindows, maxCandidates });

  const candidatesHeader = [
    "rank",
    "start",
    "end",
    "rows_total",
    "day1_rows",
    "day2_rows",
    "sha_equal",
    "eligible",
    "selected",
    "day1_parquet_relpath",
    "day2_parquet_relpath"
  ];

  const candidatesLines = [toTsvLine(candidatesHeader)];
  for (let i = 0; i < ranked.length; i++) {
    const c = ranked[i];
    const selectedRank = selected.find((s) => s.start === c.start && s.end === c.end)?.rank ?? "";
    candidatesLines.push(toTsvLine([
      selectedRank,
      c.start,
      c.end,
      safeValue(c.rowsTotal),
      safeValue(c.day1.rows),
      safeValue(c.day2.rows),
      shaEqualWord(c.shaEqual),
      boolWord(c.eligible),
      boolWord(Boolean(selectedRank)),
      path.relative(process.cwd(), c.day1.parquetPath).replace(/\\/g, "/"),
      path.relative(process.cwd(), c.day2.parquetPath).replace(/\\/g, "/")
    ]));
  }

  await writeText(path.join(packRoot, "inventory", "candidates_ranked.tsv"), `${candidatesLines.join("\n")}\n`);
  await writeText(path.join(packRoot, "sha256", "candidate_sha256_proof.txt"), `${proofText.trim()}\n`);

  const smokeRecords = new Map();
  if (selected.length > 0) {
    const smokeJobs = selected.map((c) => ({ candidate: c }));

    await runQueue(smokeJobs, smokeParallel, async ({ candidate }) => {
      const windowTag = sanitizeWindow(candidate.start, candidate.end);
      const runDir = path.join(packRoot, "runs", "smoke", `${candidate.rank}_${windowTag}`);

      const cmd = [
        `timeout --signal=INT ${smokeTimeoutS} env -u DISCOVERY_PERMUTATION_TEST NODE_OPTIONS=${quoteBash(`--max-old-space-size=${smokeHeapMB}`)} \\
  node tools/run-multi-day-discovery.js \\
    --exchange ${quoteBash(exchange)} --symbol ${quoteBash(symbol)} --stream ${quoteBash(stream)} \\
    --start ${quoteBash(candidate.start)} --end ${quoteBash(candidate.end)} \\
    --heapMB ${quoteBash(String(smokeHeapMB))} \\
    --mode smoke \\
    --smokeMaxRowsPerDay ${quoteBash(String(smokeMaxRowsPerDay))} \\
    --smokeSlice ${quoteBash(smokeSlice)} \\
    --progressEvery ${quoteBash(String(progressEvery))}`
      ].join("\n");

      const result = await runDiscoveryWithEvidence({ cwd: process.cwd(), runDir, commandBody: cmd });

      smokeRecords.set(`${candidate.start}..${candidate.end}`, {
        candidate,
        runDir,
        ...result
      });

      console.log(`[Smoke] rank=${candidate.rank} window=${candidate.start}..${candidate.end} exit=${result.exitCode} patterns=${result.patternsScanned ?? "N/A"}`);
    });
  }

  const acceptanceCandidatesMap = new Map();
  for (const c of selected) {
    const key = `${c.start}..${c.end}`;
    const smoke = smokeRecords.get(key);
    if (smoke && Number.isFinite(smoke.patternsScanned) && smoke.patternsScanned > 0) {
      acceptanceCandidatesMap.set(key, c);
    }
  }
  if (forceAcceptanceTop1 && selected.length > 0) {
    const top1 = selected.find((c) => c.rank === 1) || selected[0];
    acceptanceCandidatesMap.set(`${top1.start}..${top1.end}`, top1);
  }

  const acceptanceCandidates = selected.filter((c) => acceptanceCandidatesMap.has(`${c.start}..${c.end}`));
  const acceptanceRecords = new Map();

  if (acceptanceCandidates.length > 0) {
    await runQueue(acceptanceCandidates, acceptanceParallel, async (candidate) => {
      const windowTag = sanitizeWindow(candidate.start, candidate.end);
      const runDir = path.join(packRoot, "runs", "acceptance", `${candidate.rank}_${windowTag}`);

      const cmd = [
        `timeout --signal=INT ${acceptanceTimeoutS} env -u DISCOVERY_PERMUTATION_TEST NODE_OPTIONS=${quoteBash(`--max-old-space-size=${acceptanceHeapMB}`)} \\
  node tools/run-multi-day-discovery.js \\
    --exchange ${quoteBash(exchange)} --symbol ${quoteBash(symbol)} --stream ${quoteBash(stream)} \\
    --start ${quoteBash(candidate.start)} --end ${quoteBash(candidate.end)} \\
    --heapMB ${quoteBash(String(acceptanceHeapMB))} \\
    --mode acceptance \\
    --progressEvery ${quoteBash(String(progressEvery))}`
      ].join("\n");

      const result = await runDiscoveryWithEvidence({ cwd: process.cwd(), runDir, commandBody: cmd });

      const artifactBase = path.join(packRoot, "artifacts", "acceptance", `${candidate.rank}_${windowTag}`);
      const reportDest = path.join(artifactBase, "discovery-report.json");
      const edgesDest = path.join(artifactBase, "edges-discovered.json");

      const copiedReport = await copyIfExists(result.reportSavedSource, reportDest);
      const copiedEdges = await copyIfExists(result.edgesSavedSource, edgesDest);

      const reportRelpath = copiedReport ? toRel(packRoot, reportDest) : null;
      const edgesRelpath = copiedEdges ? toRel(packRoot, edgesDest) : null;

      acceptanceRecords.set(`${candidate.start}..${candidate.end}`, {
        candidate,
        runDir,
        ...result,
        reportRelpath,
        edgesRelpath,
        edgesSaved: Boolean(copiedEdges && result.edgesSavedSource)
      });

      console.log(`[Acceptance] rank=${candidate.rank} window=${candidate.start}..${candidate.end} exit=${result.exitCode} patterns=${result.patternsScanned ?? "N/A"}`);
    });
  }

  const determinismRecords = new Map();

  if (runDeterminism) {
    const passWindows = [];
    for (const c of selected) {
      const key = `${c.start}..${c.end}`;
      const a = acceptanceRecords.get(key);
      if (!a) continue;
      const pass = a.exitCode === 0 && a.summaryReached && Number.isFinite(a.patternsScanned) && a.patternsScanned > 0 && a.edgesSaved;
      if (pass) passWindows.push(c);
    }

    for (const c of passWindows) {
      const key = `${c.start}..${c.end}`;
      const windowTag = sanitizeWindow(c.start, c.end);
      const detRoot = path.join(packRoot, "determinism", `${c.rank}_${windowTag}`);
      await mkdirp(detRoot);

      const onRuns = [];
      for (let i = 1; i <= 2; i++) {
        const runDir = path.join(detRoot, `on_run${i}`);
        const cmd = [
          `timeout --signal=INT ${acceptanceTimeoutS} env -u DISCOVERY_PERMUTATION_TEST NODE_OPTIONS=${quoteBash(`--max-old-space-size=${acceptanceHeapMB}`)} \\
  node tools/run-multi-day-discovery.js \\
    --exchange ${quoteBash(exchange)} --symbol ${quoteBash(symbol)} --stream ${quoteBash(stream)} \\
    --start ${quoteBash(c.start)} --end ${quoteBash(c.end)} \\
    --heapMB ${quoteBash(String(acceptanceHeapMB))} \\
    --mode acceptance \\
    --progressEvery ${quoteBash(String(progressEvery))}`
        ].join("\n");
        onRuns.push(await runDiscoveryWithEvidence({ cwd: process.cwd(), runDir, commandBody: cmd }));
      }

      const onCompareDir = path.join(detRoot, "on_compare");
      await mkdirp(onCompareDir);
      const onCompareCmd = `node tools/tmp-compare-discovery-reports.js ${quoteBash(onRuns[0].reportSavedSource || "")} ${quoteBash(onRuns[1].reportSavedSource || "")}`;
      const onCompare = await runDiscoveryWithEvidence({ cwd: process.cwd(), runDir: onCompareDir, commandBody: onCompareCmd });

      const offRuns = [];
      for (let i = 1; i <= 2; i++) {
        const runDir = path.join(detRoot, `off_run${i}`);
        const cmd = [
          `timeout --signal=INT ${acceptanceTimeoutS} env DISCOVERY_PERMUTATION_TEST=false NODE_OPTIONS=${quoteBash(`--max-old-space-size=${acceptanceHeapMB}`)} \\
  node tools/run-multi-day-discovery.js \\
    --exchange ${quoteBash(exchange)} --symbol ${quoteBash(symbol)} --stream ${quoteBash(stream)} \\
    --start ${quoteBash(c.start)} --end ${quoteBash(c.end)} \\
    --heapMB ${quoteBash(String(acceptanceHeapMB))} \\
    --mode acceptance \\
    --progressEvery ${quoteBash(String(progressEvery))}`
        ].join("\n");
        offRuns.push(await runDiscoveryWithEvidence({ cwd: process.cwd(), runDir, commandBody: cmd }));
      }

      const offCompareDir = path.join(detRoot, "off_compare");
      await mkdirp(offCompareDir);
      const offCompareCmd = `node tools/tmp-compare-discovery-reports.js ${quoteBash(offRuns[0].reportSavedSource || "")} ${quoteBash(offRuns[1].reportSavedSource || "")}`;
      const offCompare = await runDiscoveryWithEvidence({ cwd: process.cwd(), runDir: offCompareDir, commandBody: offCompareCmd });

      determinismRecords.set(key, {
        onOn: onCompare.exitCode === 0 ? "PASS" : "FAIL",
        offOff: offCompare.exitCode === 0 ? "PASS" : "FAIL"
      });
    }
  }

  const resultsHeader = [
    "rank",
    "window",
    "day_quality_ok",
    "rows_total",
    "sha_equal",
    "smoke_exit",
    "smoke_patterns_scanned",
    "smoke_dataRowCount",
    "smoke_wall_s",
    "smoke_max_rss_kb",
    "acceptance_ran",
    "acceptance_exit",
    "acceptance_patterns_scanned",
    "edges_saved",
    "acceptance_wall_s",
    "acceptance_max_rss_kb",
    "report_relpath",
    "edges_relpath",
    "determinism_on_on",
    "determinism_off_off"
  ];

  const resultRows = [toTsvLine(resultsHeader)];
  const summaryRows = [];

  for (const c of selected) {
    const key = `${c.start}..${c.end}`;
    const smoke = smokeRecords.get(key);
    const acceptance = acceptanceRecords.get(key);
    const det = determinismRecords.get(key);

    const dayQualityOk = String(c.day1.dayQuality) === dayQuality && String(c.day2.dayQuality) === dayQuality;

    resultRows.push(toTsvLine([
      c.rank,
      key,
      boolWord(dayQualityOk),
      safeValue(c.rowsTotal),
      shaEqualWord(c.shaEqual),
      safeValue(smoke?.exitCode),
      safeValue(smoke?.patternsScanned),
      safeValue(smoke?.dataRowCount),
      safeValue(smoke?.wallS),
      safeValue(smoke?.maxRssKb),
      boolWord(Boolean(acceptance)),
      safeValue(acceptance?.exitCode),
      safeValue(acceptance?.patternsScanned),
      boolWord(Boolean(acceptance?.edgesSaved)),
      safeValue(acceptance?.wallS),
      safeValue(acceptance?.maxRssKb),
      safeValue(acceptance?.reportRelpath),
      safeValue(acceptance?.edgesRelpath),
      det ? det.onOn : "SKIP",
      det ? det.offOff : "SKIP"
    ]));

    const acceptancePass = Boolean(
      acceptance &&
      acceptance.exitCode === 0 &&
      acceptance.summaryReached &&
      Number.isFinite(acceptance.patternsScanned) &&
      acceptance.patternsScanned > 0 &&
      acceptance.edgesSaved
    );

    summaryRows.push({
      rank: c.rank,
      window: key,
      rows_total: c.rowsTotal,
      sha_equal: c.shaEqual,
      smoke: smoke
        ? {
          ran: true,
          exit: smoke.exitCode,
          summary_reached: smoke.summaryReached,
          patterns_scanned: smoke.patternsScanned,
          data_row_count: smoke.dataRowCount,
          wall_s: smoke.wallS,
          max_rss_kb: smoke.maxRssKb,
          cmd_relpath: toRel(packRoot, smoke.cmdPath),
          stdout_relpath: toRel(packRoot, smoke.stdoutPath),
          stderr_relpath: toRel(packRoot, smoke.stderrPath),
          time_v_relpath: toRel(packRoot, smoke.timeVPath),
          exit_relpath: toRel(packRoot, smoke.exitPath)
        }
        : { ran: false },
      acceptance: acceptance
        ? {
          ran: true,
          exit: acceptance.exitCode,
          summary_reached: acceptance.summaryReached,
          patterns_scanned: acceptance.patternsScanned,
          edges_saved: acceptance.edgesSaved,
          wall_s: acceptance.wallS,
          max_rss_kb: acceptance.maxRssKb,
          report_relpath: acceptance.reportRelpath,
          edges_relpath: acceptance.edgesRelpath,
          cmd_relpath: toRel(packRoot, acceptance.cmdPath),
          stdout_relpath: toRel(packRoot, acceptance.stdoutPath),
          stderr_relpath: toRel(packRoot, acceptance.stderrPath),
          time_v_relpath: toRel(packRoot, acceptance.timeVPath),
          exit_relpath: toRel(packRoot, acceptance.exitPath)
        }
        : { ran: false },
      determinism: det
        ? { on_on: det.onOn, off_off: det.offOff }
        : { on_on: "SKIP", off_off: "SKIP" },
      acceptance_pass: acceptancePass
    });
  }

  await writeText(path.join(packRoot, "results", "results.tsv"), `${resultRows.join("\n")}\n`);

  const campaignPass = summaryRows.some((r) => r.acceptance_pass);
  const riskNote = deriveRiskNote({ campaignPass, selectedCandidates: selected, acceptanceRecords: summaryRows.map((r) => ({ acceptanceRan: r.acceptance?.ran })) });

  const summary = {
    pack: packName,
    campaign_pass: campaignPass,
    risk_note: riskNote,
    config: {
      exchange,
      symbol,
      stream,
      symbolNorm,
      dayQuality,
      maxCandidates,
      progressEvery,
      smoke: {
        timeout_s: smokeTimeoutS,
        heapMB: smokeHeapMB,
        max_rows_per_day: smokeMaxRowsPerDay,
        slice: smokeSlice,
        parallel: smokeParallel
      },
      acceptance: {
        timeout_s: acceptanceTimeoutS,
        heapMB: acceptanceHeapMB,
        parallel: acceptanceParallel,
        force_top1: forceAcceptanceTop1
      },
      determinism: {
        run: runDeterminism
      }
    },
    files: {
      state_path_relpath: "inventory/state_path.txt",
      inventory_relpath: "inventory/daily_inventory.tsv",
      candidates_relpath: "inventory/candidates_ranked.tsv",
      sha256_proof_relpath: "sha256/candidate_sha256_proof.txt",
      results_relpath: "results/results.tsv"
    },
    selected_candidate_count: selected.length,
    rows: summaryRows
  };

  await writeText(path.join(packRoot, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);

  const readme = [
    "# Edge Campaign v0",
    "",
    `Campaign Result: ${campaignPass ? "PASS" : "FAIL"}`,
    `Risk note: ${riskNote}`,
    "",
    "## Decision Rule",
    "Campaign PASS if >=1 window has acceptance PASS (exit=0 + summary + patterns_scanned>0 + edges_saved).",
    "",
    "## Results",
    "See `results/results.tsv`.",
    "",
    "## Evidence Relpaths",
    "- inventory/state_path.txt",
    "- inventory/daily_inventory.tsv",
    "- inventory/candidates_ranked.tsv",
    "- sha256/candidate_sha256_proof.txt",
    "- results/results.tsv",
    "- summary.json"
  ].join("\n");
  await writeText(path.join(packRoot, "README.md"), `${readme}\n`);

  const missing = await integrityCheck(packRoot);
  await writeText(path.join(packRoot, "integrity_check.txt"), `missing=${missing}\n`);

  if (missing !== 0) {
    fatal(`Integrity check failed: missing=${missing}`);
  }

  await finalizePack({ packName, packRoot, tarPath, shaPath, movedPath });

  console.log(`[Campaign] result=${campaignPass ? "PASS" : "FAIL"}`);
  process.exit(campaignPass ? 0 : 1);
}

async function integrityCheck(packRoot) {
  const readme = await fs.readFile(path.join(packRoot, "README.md"), "utf8");
  const summary = await fs.readFile(path.join(packRoot, "summary.json"), "utf8");
  const results = await fs.readFile(path.join(packRoot, "results", "results.tsv"), "utf8");

  const relpathRegex = /\b(?:inventory|sha256|runs|artifacts|results|determinism)\/[A-Za-z0-9_./-]+|\bsummary\.json\b/g;
  const set = new Set();

  const collect = (txt) => {
    const matches = txt.match(relpathRegex) || [];
    for (const m of matches) {
      if (m.startsWith("runs/multi-day-discovery/")) continue;
      set.add(m);
    }
  };

  collect(readme);
  collect(summary);
  collect(results);

  let missing = 0;
  const relpaths = [...set].sort();
  await writeText(path.join(packRoot, "integrity_relpaths.txt"), `${relpaths.join("\n")}\n`);

  for (const rel of relpaths) {
    const abs = path.join(packRoot, rel);
    if (!(await fileExists(abs))) {
      missing += 1;
    }
  }
  return missing;
}

async function finalizePack({ packName, packRoot, tarPath, shaPath, movedPath }) {
  const today = todayUTCYYYYMMDD();
  const archiveRoot = path.join("..", "quantlab-evidence-archive", `${today}_slim`);
  const archiveDest = path.join(archiveRoot, packName);

  if (await fileExists(archiveDest)) {
    fatal(`Archive destination already exists: ${archiveDest}`);
  }

  await mkdirp(path.dirname(tarPath));

  const tarCmd = `tar -cf - -C ${quoteBash("evidence")} ${quoteBash(packName)} | gzip -n > ${quoteBash(tarPath)}`;
  const tarResult = await runShellCommandEvidence({
    cwd: process.cwd(),
    runDir: path.join(packRoot, "finalize_tar"),
    commandBody: tarCmd
  });
  if (tarResult.exitCode !== 0) {
    fatal(`Tar finalize failed (exit=${tarResult.exitCode})`);
  }

  const shaCmd = `sha256sum ${quoteBash(tarPath)} > ${quoteBash(shaPath)}\nsha256sum -c ${quoteBash(shaPath)}`;
  const shaResult = await runShellCommandEvidence({
    cwd: process.cwd(),
    runDir: path.join(packRoot, "finalize_sha"),
    commandBody: shaCmd
  });
  if (shaResult.exitCode !== 0) {
    fatal(`SHA finalize failed (exit=${shaResult.exitCode})`);
  }

  await mkdirp(archiveRoot);
  await fs.rename(packRoot, archiveDest);
  await writeText(movedPath, `${archiveDest}\n`);
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
