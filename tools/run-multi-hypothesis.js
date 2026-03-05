#!/usr/bin/env node

const fs = require("fs/promises");
const path = require("path");
const { spawn } = require("child_process");
const { createHash } = require("crypto");

function usage() {
  console.error(
    [
      "usage: node tools/run-multi-hypothesis.js ",
      "--exchange <exchange> --stream <stream> --symbol <symbol> --start <YYYYMMDD> --end <YYYYMMDD>",
      "[--outDir <dir>] [--heapMB <n>] [--progressEvery <n>] [--objectKeysTsv <path>] [--downloadsDir <path>]",
      "[--evidenceOn <true|false>] [--rrDeltaMsList <csv>] [--rrHMsList <csv>] [--vcDeltaMsList <csv>] [--vcHMsList <csv>] [--srDeltaMsList <csv>] [--srHMsList <csv>] [--momDeltaMsList <csv>] [--momHMsList <csv>] [--vvlDeltaMsList <csv>] [--vvlHMsList <csv>] [--jrJumpThreshBpsList <csv>] [--jrHMsList <csv>] [--jrCooldownMs <int>]",
    ].join("\n"),
  );
}

function parseArgs(argv) {
  const out = {
    exchange: "binance",
    stream: "trade",
    symbol: "",
    start: "",
    end: "",
    outDir: "",
    heapMB: "6144",
    progressEvery: "1",
    objectKeysTsv: "",
    downloadsDir: "",
    evidenceOn: false,
    rrDeltaMsList: "100,250,500",
    rrHMsList: "100,250,500",
    vcDeltaMsList: "1000,5000",
    vcHMsList: "1000,5000",
    srDeltaMsList: "1000,5000",
    srHMsList: "1000,5000",
    momDeltaMsList: "1000,5000",
    momHMsList: "1000,5000",
    vvlDeltaMsList: "1000,5000",
    vvlHMsList: "1000,5000",
    jrJumpThreshBpsList: "5,10,20",
    jrHMsList: "1000,5000",
    jrCooldownMs: "0",
  };

  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const b = argv[i + 1];
    switch (a) {
      case "--exchange": out.exchange = b; i += 1; break;
      case "--stream": out.stream = b; i += 1; break;
      case "--symbol": out.symbol = b; i += 1; break;
      case "--start": out.start = b; i += 1; break;
      case "--end": out.end = b; i += 1; break;
      case "--outDir": out.outDir = b; i += 1; break;
      case "--heapMB": out.heapMB = b; i += 1; break;
      case "--progressEvery": out.progressEvery = b; i += 1; break;
      case "--objectKeysTsv": out.objectKeysTsv = b; i += 1; break;
      case "--downloadsDir": out.downloadsDir = b; i += 1; break;
      case "--evidenceOn": out.evidenceOn = parseBoolArg(b, "--evidenceOn"); i += 1; break;
      case "--rrDeltaMsList": out.rrDeltaMsList = b; i += 1; break;
      case "--rrHMsList": out.rrHMsList = b; i += 1; break;
      case "--vcDeltaMsList": out.vcDeltaMsList = b; i += 1; break;
      case "--vcHMsList": out.vcHMsList = b; i += 1; break;
      case "--srDeltaMsList": out.srDeltaMsList = b; i += 1; break;
      case "--srHMsList": out.srHMsList = b; i += 1; break;
      case "--momDeltaMsList": out.momDeltaMsList = b; i += 1; break;
      case "--momHMsList": out.momHMsList = b; i += 1; break;
      case "--vvlDeltaMsList": out.vvlDeltaMsList = b; i += 1; break;
      case "--vvlHMsList": out.vvlHMsList = b; i += 1; break;
      case "--jrJumpThreshBpsList": out.jrJumpThreshBpsList = b; i += 1; break;
      case "--jrHMsList": out.jrHMsList = b; i += 1; break;
      case "--jrCooldownMs": out.jrCooldownMs = b; i += 1; break;
      case "-h":
      case "--help":
        usage();
        process.exit(0);
      default:
        if (a.startsWith("-")) {
          throw new Error(`unknown argument: ${a}`);
        }
    }
  }

  if (!out.symbol || !out.start || !out.end) {
    usage();
    throw new Error("symbol/start/end are required");
  }

  if (!out.outDir) {
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    const slug = symbolToSlug(out.symbol);
    out.outDir = path.join("artifacts", "multi_hypothesis", `${ts}_${slug}_${out.start}_${out.end}`);
  }
  if (!out.downloadsDir) {
    out.downloadsDir = path.join(out.outDir, "downloads");
  }

  return out;
}

function parseBoolArg(raw, name) {
  const v = String(raw == null ? "" : raw).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(v)) return true;
  if (["false", "0", "no", "n", "off"].includes(v)) return false;
  throw new Error(`invalid boolean for ${name}: ${raw}`);
}

function symbolToCli(symbol) {
  if (symbol.includes("/")) return symbol.toUpperCase();
  const s = symbol.replace(/[-_]/g, "").toUpperCase();
  if (s.endsWith("USDT") && s.length > 4) return `${s.slice(0, -4)}/USDT`;
  return s;
}

function symbolToSlug(symbol) {
  return symbol.replace(/[\/_-]/g, "").toLowerCase();
}

function quoteBash(v) {
  return `'${String(v).replace(/'/g, `'"'"'`)}'`;
}

async function mkdirp(p) {
  await fs.mkdir(p, { recursive: true });
}

async function writeText(file, txt) {
  await mkdirp(path.dirname(file));
  await fs.writeFile(file, txt, "utf8");
}

function parseYmd(s) {
  if (!/^\d{8}$/.test(String(s || ""))) return null;
  const y = Number(s.slice(0, 4));
  const m = Number(s.slice(4, 6));
  const d = Number(s.slice(6, 8));
  const dt = new Date(Date.UTC(y, m - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== m - 1 || dt.getUTCDate() !== d) return null;
  return dt;
}

function formatYmd(d) {
  const y = String(d.getUTCFullYear()).padStart(4, "0");
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

function dateRangeInclusive(start, end) {
  const ds = parseYmd(start);
  const de = parseYmd(end);
  if (!ds || !de || ds.getTime() > de.getTime()) {
    throw new Error(`invalid date range: ${start}..${end}`);
  }
  const out = [];
  for (let cur = ds; cur.getTime() <= de.getTime(); cur = new Date(cur.getTime() + 86400000)) {
    out.push(formatYmd(cur));
  }
  return out;
}

function parseTsvLine(line) {
  const out = [];
  let cur = "";
  let i = 0;
  let inQuote = false;
  while (i < line.length) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i += 2;
          continue;
        }
        inQuote = false;
        i += 1;
        continue;
      }
      cur += ch;
      i += 1;
      continue;
    }
    if (ch === '"') {
      inQuote = true;
      i += 1;
      continue;
    }
    if (ch === "\t") {
      out.push(cur);
      cur = "";
      i += 1;
      continue;
    }
    cur += ch;
    i += 1;
  }
  out.push(cur);
  return out;
}

async function readObjectKeysByDate(tsvPath) {
  const raw = await fs.readFile(tsvPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((x) => x.trim().length > 0);
  if (lines.length < 2) return new Map();
  const header = parseTsvLine(lines[0]).map((x) => String(x).trim());
  const idx = new Map();
  header.forEach((name, i) => idx.set(name, i));

  const out = new Map();
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseTsvLine(lines[i]);
    const col = (name) => {
      const at = idx.get(name);
      if (at == null || at < 0 || at >= cols.length) return "";
      return String(cols[at] || "").trim();
    };

    const dataKey = col("data_key");
    if (!dataKey) continue;
    let date = col("date");
    if (!date) {
      const m = dataKey.match(/date=(\d{8})/);
      date = m ? m[1] : "";
    }
    if (!date) continue;
    const bucket = col("bucket") || "quantlab-compact";
    out.set(date, { bucket, dataKey });
  }
  return out;
}

function toInt(v, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function toFinite(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function fixed15(v) {
  return toFinite(v, 0).toFixed(15);
}

const SKIPPED_HASH_PLACEHOLDER = "-";

function parseTsvRows(raw, requiredHeader = null) {
  const lines = String(raw || "").split(/\r?\n/).filter((x) => x.trim().length > 0);
  if (!lines.length) {
    return [];
  }
  const header = parseTsvLine(lines[0]).map((x) => String(x).trim());
  if (requiredHeader) {
    const joined = header.join("\t");
    if (joined !== requiredHeader.join("\t")) {
      throw new Error(`tsv_header_mismatch expected=${requiredHeader.join("\t")} got=${joined}`);
    }
  }
  const idx = new Map();
  header.forEach((name, i) => idx.set(name, i));
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseTsvLine(lines[i]);
    const row = {};
    for (const name of header) {
      const at = idx.get(name);
      row[name] = at == null || at < 0 || at >= cols.length ? "" : cols[at];
    }
    rows.push(row);
  }
  return rows;
}

function compareBasisFromHeader(headerArr) {
  return headerArr.join(",");
}

const RR_HEADER = [
  "exchange",
  "symbol",
  "date",
  "stream",
  "delta_ms",
  "h_ms",
  "event_count",
  "mean_product",
  "t_stat",
];

function normalizeReturnReversalRows(rows) {
  return rows
    .map((r) => ({
      exchange: String(r.exchange || ""),
      symbol: String(r.symbol || ""),
      date: String(r.date || ""),
      stream: String(r.stream || ""),
      delta_ms: toInt(r.delta_ms, 0),
      h_ms: toInt(r.h_ms, 0),
      event_count: toInt(r.event_count, 0),
      mean_product: fixed15(r.mean_product),
      t_stat: fixed15(r.t_stat),
    }))
    .sort((a, b) => (
      a.exchange.localeCompare(b.exchange)
      || a.symbol.localeCompare(b.symbol)
      || a.date.localeCompare(b.date)
      || a.stream.localeCompare(b.stream)
      || (a.delta_ms - b.delta_ms)
      || (a.h_ms - b.h_ms)
    ));
}

function parseReturnReversalResults(tsvText) {
  const parsed = parseTsvRows(tsvText, RR_HEADER);
  return normalizeReturnReversalRows(parsed);
}

function hashCanonicalRows(rows) {
  const blob = JSON.stringify(rows);
  return createHash("sha256").update(blob).digest("hex");
}

function pickReturnReversalSelected(rows) {
  const valid = rows.filter((r) => r.event_count > 0);
  if (!valid.length) return null;
  const sorted = valid.slice().sort((a, b) => (
    (Number(a.mean_product) - Number(b.mean_product))
    || (Number(a.t_stat) - Number(b.t_stat))
    || (b.event_count - a.event_count)
    || a.date.localeCompare(b.date)
    || (a.delta_ms - b.delta_ms)
    || (a.h_ms - b.h_ms)
  ));
  return sorted[0];
}

const SR_HEADER = [
  "exchange",
  "symbol",
  "date",
  "stream",
  "delta_ms",
  "h_ms",
  "event_count",
  "mean_product",
  "t_stat",
];

function normalizeSpreadReversionRows(rows) {
  return rows
    .map((r) => ({
      exchange: String(r.exchange || ""),
      symbol: String(r.symbol || ""),
      date: String(r.date || ""),
      stream: String(r.stream || ""),
      delta_ms: toInt(r.delta_ms, 0),
      h_ms: toInt(r.h_ms, 0),
      event_count: toInt(r.event_count, 0),
      mean_product: fixed15(r.mean_product),
      t_stat: fixed15(r.t_stat),
    }))
    .sort((a, b) => (
      a.exchange.localeCompare(b.exchange)
      || a.symbol.localeCompare(b.symbol)
      || a.date.localeCompare(b.date)
      || a.stream.localeCompare(b.stream)
      || (a.delta_ms - b.delta_ms)
      || (a.h_ms - b.h_ms)
    ));
}

function parseSpreadReversionResults(tsvText) {
  const parsed = parseTsvRows(tsvText, SR_HEADER);
  return normalizeSpreadReversionRows(parsed);
}

function pickSpreadReversionSelected(rows) {
  const valid = rows.filter((r) => r.event_count > 0);
  if (!valid.length) return null;
  const sorted = valid.slice().sort((a, b) => (
    (Number(a.mean_product) - Number(b.mean_product))
    || (Number(a.t_stat) - Number(b.t_stat))
    || (b.event_count - a.event_count)
    || a.date.localeCompare(b.date)
    || (a.delta_ms - b.delta_ms)
    || (a.h_ms - b.h_ms)
  ));
  return sorted[0];
}

const MOM_HEADER = [
  "exchange",
  "symbol",
  "date",
  "stream",
  "delta_ms",
  "h_ms",
  "event_count",
  "mean_product",
  "t_stat",
];

function normalizeMomentumRows(rows) {
  return rows
    .map((r) => ({
      exchange: String(r.exchange || ""),
      symbol: String(r.symbol || ""),
      date: String(r.date || ""),
      stream: String(r.stream || ""),
      delta_ms: toInt(r.delta_ms, 0),
      h_ms: toInt(r.h_ms, 0),
      event_count: toInt(r.event_count, 0),
      mean_product: fixed15(r.mean_product),
      t_stat: fixed15(r.t_stat),
    }))
    .sort((a, b) => (
      a.exchange.localeCompare(b.exchange)
      || a.symbol.localeCompare(b.symbol)
      || a.date.localeCompare(b.date)
      || a.stream.localeCompare(b.stream)
      || (a.delta_ms - b.delta_ms)
      || (a.h_ms - b.h_ms)
    ));
}

function parseMomentumResults(tsvText) {
  const parsed = parseTsvRows(tsvText, MOM_HEADER);
  return normalizeMomentumRows(parsed);
}

function pickMomentumSelected(rows) {
  const valid = rows.filter((r) => r.event_count > 0);
  if (!valid.length) return null;
  const sorted = valid.slice().sort((a, b) => (
    (Number(b.mean_product) - Number(a.mean_product))
    || (Number(b.t_stat) - Number(a.t_stat))
    || (b.event_count - a.event_count)
    || a.date.localeCompare(b.date)
    || (a.delta_ms - b.delta_ms)
    || (a.h_ms - b.h_ms)
  ));
  return sorted[0];
}

const VVL_HEADER = [
  "exchange",
  "symbol",
  "date",
  "stream",
  "delta_ms",
  "h_ms",
  "sample_count",
  "mean_activity",
  "mean_rv_fwd",
  "corr",
  "t_stat",
];

function normalizeVolumeVolLinkRows(rows) {
  return rows
    .map((r) => ({
      exchange: String(r.exchange || ""),
      symbol: String(r.symbol || ""),
      date: String(r.date || ""),
      stream: String(r.stream || ""),
      delta_ms: toInt(r.delta_ms, 0),
      h_ms: toInt(r.h_ms, 0),
      sample_count: toInt(r.sample_count, 0),
      mean_activity: fixed15(r.mean_activity),
      mean_rv_fwd: fixed15(r.mean_rv_fwd),
      corr: fixed15(r.corr),
      t_stat: fixed15(r.t_stat),
    }))
    .sort((a, b) => (
      a.exchange.localeCompare(b.exchange)
      || a.symbol.localeCompare(b.symbol)
      || a.date.localeCompare(b.date)
      || a.stream.localeCompare(b.stream)
      || (a.delta_ms - b.delta_ms)
      || (a.h_ms - b.h_ms)
    ));
}

function parseVolumeVolLinkResults(tsvText) {
  const parsed = parseTsvRows(tsvText, VVL_HEADER);
  return normalizeVolumeVolLinkRows(parsed);
}

function pickVolumeVolLinkSelected(rows) {
  const valid = rows.filter((r) => r.sample_count > 0);
  if (!valid.length) return null;
  const sorted = valid.slice().sort((a, b) => (
    (Number(b.corr) - Number(a.corr))
    || (Number(b.t_stat) - Number(a.t_stat))
    || (b.sample_count - a.sample_count)
    || a.date.localeCompare(b.date)
    || (a.delta_ms - b.delta_ms)
    || (a.h_ms - b.h_ms)
  ));
  return sorted[0];
}

const JR_HEADER = [
  "exchange",
  "symbol",
  "date",
  "stream",
  "jump_thresh_bps",
  "h_ms",
  "jump_count",
  "mean_signed_reversal",
  "t_stat",
];

function normalizeJumpReversionRows(rows) {
  return rows
    .map((r) => ({
      exchange: String(r.exchange || ""),
      symbol: String(r.symbol || ""),
      date: String(r.date || ""),
      stream: String(r.stream || ""),
      jump_thresh_bps: toInt(r.jump_thresh_bps, 0),
      h_ms: toInt(r.h_ms, 0),
      jump_count: toInt(r.jump_count, 0),
      mean_signed_reversal: fixed15(r.mean_signed_reversal),
      t_stat: fixed15(r.t_stat),
    }))
    .sort((a, b) => (
      a.exchange.localeCompare(b.exchange)
      || a.symbol.localeCompare(b.symbol)
      || a.date.localeCompare(b.date)
      || a.stream.localeCompare(b.stream)
      || (a.jump_thresh_bps - b.jump_thresh_bps)
      || (a.h_ms - b.h_ms)
    ));
}

function parseJumpReversionResults(tsvText) {
  const parsed = parseTsvRows(tsvText, JR_HEADER);
  return normalizeJumpReversionRows(parsed);
}

function pickJumpReversionSelected(rows) {
  const valid = rows.filter((r) => r.jump_count > 0);
  if (!valid.length) return null;
  const sorted = valid.slice().sort((a, b) => (
    (Number(b.mean_signed_reversal) - Number(a.mean_signed_reversal))
    || (Number(b.t_stat) - Number(a.t_stat))
    || (b.jump_count - a.jump_count)
    || a.date.localeCompare(b.date)
    || (a.jump_thresh_bps - b.jump_thresh_bps)
    || (a.h_ms - b.h_ms)
  ));
  return sorted[0];
}

const VC_HEADER = [
  "exchange",
  "symbol",
  "date",
  "stream",
  "delta_ms",
  "h_ms",
  "sample_count",
  "mean_rv_past",
  "mean_rv_fwd",
  "corr",
  "t_stat",
];

function normalizeVolatilityClusteringRows(rows) {
  return rows
    .map((r) => ({
      exchange: String(r.exchange || ""),
      symbol: String(r.symbol || ""),
      date: String(r.date || ""),
      stream: String(r.stream || ""),
      delta_ms: toInt(r.delta_ms, 0),
      h_ms: toInt(r.h_ms, 0),
      sample_count: toInt(r.sample_count, 0),
      mean_rv_past: fixed15(r.mean_rv_past),
      mean_rv_fwd: fixed15(r.mean_rv_fwd),
      corr: fixed15(r.corr),
      t_stat: fixed15(r.t_stat),
    }))
    .sort((a, b) => (
      a.exchange.localeCompare(b.exchange)
      || a.symbol.localeCompare(b.symbol)
      || a.date.localeCompare(b.date)
      || a.stream.localeCompare(b.stream)
      || (a.delta_ms - b.delta_ms)
      || (a.h_ms - b.h_ms)
    ));
}

function parseVolatilityClusteringResults(tsvText) {
  const parsed = parseTsvRows(tsvText, VC_HEADER);
  return normalizeVolatilityClusteringRows(parsed);
}

function pickVolatilityClusteringSelected(rows) {
  const valid = rows.filter((r) => r.sample_count > 0);
  if (!valid.length) return null;
  const sorted = valid.slice().sort((a, b) => (
    (Number(b.corr) - Number(a.corr))
    || (Number(b.t_stat) - Number(a.t_stat))
    || (b.sample_count - a.sample_count)
    || a.date.localeCompare(b.date)
    || (a.delta_ms - b.delta_ms)
    || (a.h_ms - b.h_ms)
  ));
  return sorted[0];
}

function symbolToPathSlug(symbol) {
  return String(symbol || "").replace(/[\/_-]/g, "").toLowerCase();
}

async function runTimedStep({ cwd, runDir, commandBody }) {
  await mkdirp(runDir);
  const cmdPath = path.join(runDir, "cmd.sh");
  const stdoutPath = path.join(runDir, "stdout.log");
  const stderrPath = path.join(runDir, "stderr.log");
  const timePath = path.join(runDir, "time-v.log");
  const exitPath = path.join(runDir, "exit_code.txt");
  await writeText(cmdPath, `${commandBody}\n`);

  const outFd = await fs.open(stdoutPath, "w");
  const errFd = await fs.open(stderrPath, "w");

  const child = spawn(
    "/usr/bin/time",
    ["-v", "-o", timePath, "--", "bash", "-lc", commandBody],
    {
      cwd,
      stdio: ["ignore", outFd.fd, errFd.fd],
      env: process.env,
    },
  );

  const exitCode = await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code) => resolve(code == null ? 1 : code));
  });

  await outFd.close();
  await errFd.close();
  await writeText(exitPath, `${exitCode}\n`);

  return { exitCode, cmdPath, stdoutPath, stderrPath, timePath, exitPath };
}

function parsePatternInt(txt, regex) {
  const m = txt.match(regex);
  return m ? Number(m[1]) : 0;
}

async function fileExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function resolveParquetInputs({ args, outRoot }) {
  const s3Tool = "/tmp/s3_compact_tool.py";
  const exchange = args.exchange.toLowerCase();
  const stream = args.stream.toLowerCase();
  const symbolSlug = symbolToPathSlug(args.symbol);
  const dates = dateRangeInclusive(args.start, args.end);
  const downloadsRoot = path.resolve(process.cwd(), args.downloadsDir);
  await mkdirp(downloadsRoot);

  let objectKeysByDate = new Map();
  if (args.objectKeysTsv) {
    objectKeysByDate = await readObjectKeysByDate(path.resolve(process.cwd(), args.objectKeysTsv));
  }

  const resolved = [];
  const materializedCurated = [];

  for (const day of dates) {
    const curatedPath = path.resolve(
      process.cwd(),
      "data",
      "curated",
      `exchange=${exchange}`,
      `stream=${stream}`,
      `symbol=${symbolSlug}`,
      `date=${day}`,
      "data.parquet",
    );
    const curatedExists = await fileExists(curatedPath);
    let chosenPath = curatedPath;
    let source = "curated";
    let bucket = "";
    let s3Key = "";

    if (!curatedExists) {
      const row = objectKeysByDate.get(day);
      if (!row || !row.dataKey) {
        throw new Error(
          `runner_path_resolve_failed date=${day} reason=missing_curated_and_no_state_key curated=${curatedPath}`,
        );
      }
      if (!(await fileExists(s3Tool))) {
        throw new Error(`runner_path_resolve_failed date=${day} reason=missing_s3_tool path=${s3Tool}`);
      }
      bucket = row.bucket || "quantlab-compact";
      s3Key = row.dataKey;
      const downloadPath = path.resolve(downloadsRoot, `date=${day}`, "data.parquet");
      await mkdirp(path.dirname(downloadPath));
      const dlRun = await runTimedStep({
        cwd: process.cwd(),
        runDir: path.join(outRoot, "runs", "resolver", `download_${day}`),
        commandBody: `python3 ${quoteBash(s3Tool)} get ${quoteBash(bucket)} ${quoteBash(s3Key)} ${quoteBash(downloadPath)}`,
      });
      if (dlRun.exitCode !== 0 || !(await fileExists(downloadPath))) {
        throw new Error(
          `runner_path_resolve_failed date=${day} reason=state_download_failed bucket=${bucket} key=${s3Key}`,
        );
      }

      await mkdirp(path.dirname(curatedPath));
      await fs.copyFile(downloadPath, curatedPath);
      chosenPath = curatedPath;
      source = "state_fallback_download";
      materializedCurated.push(curatedPath);
    }

    let stat;
    try {
      stat = await fs.stat(chosenPath);
    } catch {
      stat = null;
    }
    const exists = stat != null;
    const fileSizeBytes = stat ? Number(stat.size || 0) : 0;
    if (!exists || fileSizeBytes <= 0) {
      throw new Error(
        `runner_path_resolve_failed date=${day} reason=resolved_missing_or_empty path=${chosenPath} size=${fileSizeBytes}`,
      );
    }

    resolved.push({
      day,
      resolvedPath: chosenPath,
      source,
      exists,
      fileSizeBytes,
      bucket,
      s3Key,
    });
  }

  const logPath = path.join(outRoot, "artifacts", "multi_hypothesis", "resolved_paths_log.txt");
  const lines = [
    "date\tresolved_parquet_path\tsource\texists\tfile_size_bytes\tbucket\ts3_key",
    ...resolved.map((r) => [
      r.day,
      r.resolvedPath,
      r.source,
      r.exists ? "true" : "false",
      String(r.fileSizeBytes),
      r.bucket || "",
      r.s3Key || "",
    ].join("\t")),
  ];
  await writeText(logPath, `${lines.join("\n")}\n`);

  console.log("[RunnerPath] resolved_parquet_paths_begin");
  for (const r of resolved) {
    console.log(
      `[RunnerPath] date=${r.day} path=${r.resolvedPath} exists=${r.exists ? "true" : "false"} file_size_bytes=${r.fileSizeBytes} source=${r.source}`,
    );
  }
  console.log("[RunnerPath] resolved_parquet_paths_end");

  return { resolved, materializedCurated, logRelpath: "artifacts/multi_hypothesis/resolved_paths_log.txt" };
}

async function cleanupMaterializedCurated(paths) {
  for (const p of paths || []) {
    try {
      await fs.unlink(p);
    } catch {
      // best-effort cleanup only
    }
  }
}

async function parseFamilyA({ args, outRoot }) {
  const familyDir = path.join(outRoot, "runs", "family_a");
  const symbolCli = symbolToCli(args.symbol);
  const cmd = [
    "timeout --signal=INT 3600 env -u DISCOVERY_PERMUTATION_TEST",
    `NODE_OPTIONS='--max-old-space-size=${args.heapMB}'`,
    "node tools/run-multi-day-discovery.js",
    `--exchange ${quoteBash(args.exchange)}`,
    `--symbol ${quoteBash(symbolCli)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    `--heapMB ${quoteBash(args.heapMB)}`,
    "--mode acceptance",
    `--progressEvery ${quoteBash(args.progressEvery)}`,
  ].join(" ");

  const run = await runTimedStep({ cwd: process.cwd(), runDir: familyDir, commandBody: cmd });
  const stdout = await fs.readFile(run.stdoutPath, "utf8");

  const patternsScanned = parsePatternInt(stdout, /^patterns_scanned:\s*([0-9]+)/m);
  const reportSaved = (stdout.match(/^\[Run\] report_saved=(.+)$/m) || ["", ""])[1].trim();
  const edgesSavedField = (stdout.match(/^\[Run\] edges_saved=(.+)$/m) || ["", ""])[1].trim();

  let reportSrc = "";
  if (reportSaved) {
    reportSrc = path.isAbsolute(reportSaved) ? reportSaved : path.resolve(process.cwd(), reportSaved);
  }

  let reportJson = {
    family_id: "family_a_patternscanner",
    source: "tools/run-multi-day-discovery.js",
    result: {
      patternsScanned,
      edgeCandidatesGenerated: 0,
      edgeCandidatesRegistered: 0,
      edgesCount: 0,
      edgesSaved: false,
    },
    diagnosticNotes: [],
  };

  if (reportSrc && (await fileExists(reportSrc))) {
    const raw = JSON.parse(await fs.readFile(reportSrc, "utf8"));
    const res = raw && raw.result ? raw.result : {};
    const edges = Array.isArray(res.edges) ? res.edges.length : 0;
    reportJson = {
      family_id: "family_a_patternscanner",
      source: "tools/run-multi-day-discovery.js",
      result: {
        patternsScanned: Number(res.patternsScanned || patternsScanned || 0),
        edgeCandidatesGenerated: Number(res.edgeCandidatesGenerated || 0),
        edgeCandidatesRegistered: Number(res.edgeCandidatesRegistered || 0),
        edgesCount: edges,
        edgesSaved: Boolean(edgesSavedField) || edges > 0,
      },
      diagnosticNotes: Array.isArray(raw.diagnosticNotes) ? raw.diagnosticNotes.slice(0, 5) : [],
      raw_report_relpath: reportSaved || "",
    };

    await fs.copyFile(reportSrc, path.join(outRoot, "artifacts", "multi_hypothesis", "family_A_discovery_report_raw.json"));
  } else {
    reportJson.diagnosticNotes.push("report_missing_or_unreadable");
  }

  const scannerLine = (stdout.split("\n").find((line) => line.includes("scanner.minSupport=") && line.includes("scanner.returnThreshold=")) || "").trim();
  await writeText(path.join(outRoot, "artifacts", "multi_hypothesis", "scanner_config_excerpt_family_A.txt"), `${scannerLine || "(not found)"}\n`);

  const reportPath = path.join(outRoot, "artifacts", "multi_hypothesis", "family_A_report.json");
  await writeText(reportPath, `${JSON.stringify(reportJson, null, 2)}\n`);

  return {
    familyId: "family_a_patternscanner",
    exitCode: run.exitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_A_report.json",
    patternsScanned: reportJson.result.patternsScanned,
    edgeCandidatesGenerated: reportJson.result.edgeCandidatesGenerated,
    edgesSaved: reportJson.result.edgesSaved,
    meanForwardReturn: "",
    tStat: "",
    signalSupport: "",
  };
}

async function parseFamilyB({ args, outRoot }) {
  const familyDir = path.join(outRoot, "runs", "family_b");
  const reportPath = path.join(outRoot, "artifacts", "multi_hypothesis", "family_B_report.json");

  const cmd = [
    "python3 tools/hypotheses/simple_momentum_family.py",
    `--exchange ${quoteBash(args.exchange)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--symbol ${quoteBash(args.symbol)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    "--lookback-minutes 5",
    "--forward-minutes 5",
    "--signal-quantile 0.90",
    "--min-support 200",
    `--output ${quoteBash(reportPath)}`,
  ].join(" ");

  const run = await runTimedStep({ cwd: process.cwd(), runDir: familyDir, commandBody: cmd });

  let json = {
    result: {
      valid_pairs: 0,
      signal_support: 0,
      mean_forward_return: 0,
      t_stat: 0,
      pass_signal: false,
    },
  };

  if (await fileExists(reportPath)) {
    json = JSON.parse(await fs.readFile(reportPath, "utf8"));
  }

  const result = json.result || {};
  return {
    familyId: "family_b_simple_momentum",
    exitCode: run.exitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_B_report.json",
    patternsScanned: Number(result.valid_pairs || 0),
    edgeCandidatesGenerated: 0,
    edgesSaved: Boolean(result.pass_signal || false),
    meanForwardReturn: result.mean_forward_return ?? "",
    tStat: result.t_stat ?? "",
    signalSupport: result.signal_support ?? 0,
  };
}

function buildUnsupportedReturnReversalReport(args) {
  return {
    family_id: "return_reversal_v1",
    status: "unsupported_stream",
    exchange: args.exchange,
    symbol: args.symbol,
    stream: args.stream,
    window: `${args.start}..${args.end}`,
    result: {
      rows_produced: 0,
      selected_cell: null,
      pass_signal: false,
    },
  };
}

async function runReturnReversalOnce({
  args,
  outRoot,
  runDirName,
  resultsPath,
  summaryPath,
  reportPath,
}) {
  const cmd = [
    "python3 tools/hypotheses/return_reversal_v1.py",
    `--exchange ${quoteBash(args.exchange)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--symbol ${quoteBash(args.symbol)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    `--delta-ms-list ${quoteBash(args.rrDeltaMsList)}`,
    `--h-ms-list ${quoteBash(args.rrHMsList)}`,
    `--results-out ${quoteBash(resultsPath)}`,
    `--summary-out ${quoteBash(summaryPath)}`,
    `--report-out ${quoteBash(reportPath)}`,
    `--downloads-dir ${quoteBash(args.downloadsDir || "")}`,
    `--object-keys-tsv ${quoteBash(args.objectKeysTsv || "")}`,
    `--exchange-order ${quoteBash(args.exchange || "")}`,
  ].join(" ");
  const run = await runTimedStep({
    cwd: process.cwd(),
    runDir: path.join(outRoot, "runs", runDirName),
    commandBody: cmd,
  });
  return run;
}

async function writeMinimalEvidenceFiles({
  outRoot,
  windowStr,
  entries,
  decisions,
}) {
  const baseRel = "artifacts/multi_hypothesis";
  const baseAbs = path.join(outRoot, "artifacts", "multi_hypothesis");
  const determinismRel = `${baseRel}/determinism_compare.tsv`;
  const manifestRel = `${baseRel}/artifact_manifest.tsv`;
  const labelRel = `${baseRel}/label_report.txt`;
  const integrityRel = `${baseRel}/integrity_check.txt`;

  const stableEntries = (entries || [])
    .map((entry) => {
      const status = String(entry && entry.determinismStatus ? entry.determinismStatus : "");
      const isSkipped = status.startsWith("SKIPPED_");
      return {
        familyId: String(entry && entry.familyId ? entry.familyId : ""),
        primaryHash: isSkipped
          ? SKIPPED_HASH_PLACEHOLDER
          : String(entry && entry.primaryHash ? entry.primaryHash : ""),
        replayHash: isSkipped
          ? SKIPPED_HASH_PLACEHOLDER
          : String(entry && entry.replayHash ? entry.replayHash : ""),
        determinismStatus: status,
        compareBasis: String(entry && entry.compareBasis ? entry.compareBasis : ""),
      };
    })
    .sort((a, b) => String(a.familyId || "").localeCompare(String(b.familyId || "")));
  const determinismLines = [
    "window\tfamily_id\tprimary_hash\treplay_hash\tdeterminism_status\tcompare_basis",
    ...stableEntries.map((e) => [
      windowStr,
      String(e.familyId || ""),
      String(e.primaryHash || ""),
      String(e.replayHash || ""),
      String(e.determinismStatus || ""),
      String(e.compareBasis || ""),
    ].join("\t")),
  ];
  await writeText(path.join(baseAbs, "determinism_compare.tsv"), `${determinismLines.join("\n")}\n`);

  const manifestLines = [
    "expected_relpath\tresolved_relpath\tstatus",
    `${determinismRel}\t${determinismRel}\tOK`,
    `${manifestRel}\t${manifestRel}\tOK`,
    `${labelRel}\t${labelRel}\tOK`,
    `${integrityRel}\t${integrityRel}\tOK`,
  ];
  await writeText(path.join(baseAbs, "artifact_manifest.tsv"), `${manifestLines.join("\n")}\n`);

  const statuses = stableEntries.map((e) => String(e.determinismStatus || ""));
  const anyFail = statuses.includes("FAIL");
  const allSkipped = statuses.length > 0 && statuses.every((s) => s.startsWith("SKIPPED_"));
  let label = "PASS/MULTI_HYPOTHESIS_READY";
  if (anyFail) {
    label = "FAIL/DETERMINISM_FAIL";
  } else if (allSkipped) {
    label = "PASS/UNSUPPORTED_STREAM_SKIPPED";
  }

  const decisionInputs = (decisions || []).length ? decisions.join("|") : "none";
  const compareBasis = stableEntries.length
    ? stableEntries.map((e) => `${e.familyId}:${e.compareBasis}`).join("|")
    : "none";
  const labelLines = [
    `label=${label}`,
    `decision_inputs=${decisionInputs}`,
    `compare_basis=${compareBasis}`,
    `run_id=${path.basename(outRoot)}`,
    "scope_guard=TSV_first;minimal_evidence_files_only;slim_finalize_deferred",
  ];
  await writeText(path.join(baseAbs, "label_report.txt"), `${labelLines.join("\n")}\n`);

  const required = [
    determinismRel,
    manifestRel,
    labelRel,
  ];
  const missing = [];
  for (const rel of required) {
    if (!(await fileExists(path.join(outRoot, rel)))) {
      missing.push(rel);
    }
  }
  const integrityLines = [
    `missing_count=${missing.length}`,
    ...missing,
  ];
  await writeText(path.join(baseAbs, "integrity_check.txt"), `${integrityLines.join("\n")}\n`);

  return {
    determinismRel,
    manifestRel,
    labelRel,
    integrityRel,
  };
}

async function parseFamilyReturnReversal({ args, outRoot }) {
  const baseAbs = path.join(outRoot, "artifacts", "multi_hypothesis");
  const primaryResultsPath = path.join(baseAbs, "family_return_reversal_primary_results.tsv");
  const primarySummaryPath = path.join(baseAbs, "family_return_reversal_primary_summary.tsv");
  const reportPath = path.join(baseAbs, "family_return_reversal_report.json");

  const streamNorm = String(args.stream || "").trim().toLowerCase();
  const supported = streamNorm === "trade" || streamNorm === "mark_price";
  const compareBasis = compareBasisFromHeader(RR_HEADER);

  let primaryExitCode = 0;
  let primaryRows = [];
  if (!supported) {
    await writeText(primaryResultsPath, `${RR_HEADER.join("\t")}\n`);
    await writeText(primarySummaryPath, `${RR_HEADER.join("\t")}\n`);
    await writeText(reportPath, `${JSON.stringify(buildUnsupportedReturnReversalReport(args), null, 2)}\n`);
  } else {
    const primaryRun = await runReturnReversalOnce({
      args,
      outRoot,
      runDirName: "family_return_reversal_primary",
      resultsPath: primaryResultsPath,
      summaryPath: primarySummaryPath,
      reportPath,
    });
    primaryExitCode = primaryRun.exitCode;
    if (await fileExists(primaryResultsPath)) {
      const primaryText = await fs.readFile(primaryResultsPath, "utf8");
      primaryRows = parseReturnReversalResults(primaryText);
    }
  }

  const selected = pickReturnReversalSelected(primaryRows);
  const selectedMean = selected ? Number(selected.mean_product) : 0;
  const selectedT = selected ? Number(selected.t_stat) : 0;
  const selectedSupport = selected ? Number(selected.event_count) : 0;
  const passSignal = supported
    && selected != null
    && selectedMean < 0
    && selectedT <= -2
    && selectedSupport >= 200;

  const out = {
    familyId: "return_reversal_v1",
    exitCode: primaryExitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_return_reversal_report.json",
    patternsScanned: primaryRows.length,
    edgeCandidatesGenerated: 0,
    edgesSaved: passSignal,
    meanForwardReturn: fixed15(selectedMean),
    tStat: fixed15(selectedT),
    signalSupport: selectedSupport,
    files: {
      family_return_reversal_primary_results_relpath: "artifacts/multi_hypothesis/family_return_reversal_primary_results.tsv",
      family_return_reversal_primary_summary_relpath: "artifacts/multi_hypothesis/family_return_reversal_primary_summary.tsv",
      family_return_reversal_report_relpath: "artifacts/multi_hypothesis/family_return_reversal_report.json",
    },
    evidenceEntry: null,
    evidenceDecision: selected
      ? `family=return_reversal_v1,date=${selected.date},delta_ms=${selected.delta_ms},h_ms=${selected.h_ms},event_count=${selected.event_count},mean_product=${selected.mean_product},t_stat=${selected.t_stat}`
      : "family=return_reversal_v1,selected_cell=NONE",
  };

  if (!args.evidenceOn) {
    return out;
  }

  let replayRows = [];
  let primaryHash = SKIPPED_HASH_PLACEHOLDER;
  let replayHash = SKIPPED_HASH_PLACEHOLDER;
  let determinismStatus = supported ? "FAIL" : "SKIPPED_UNSUPPORTED_STREAM";

  if (supported) {
    const replayResultsPath = path.join(outRoot, "runs", "family_return_reversal_replay_on", "results.tsv");
    const replaySummaryPath = path.join(outRoot, "runs", "family_return_reversal_replay_on", "summary.tsv");
    const replayReportPath = path.join(outRoot, "runs", "family_return_reversal_replay_on", "report.json");
    await runReturnReversalOnce({
      args,
      outRoot,
      runDirName: "family_return_reversal_replay_on",
      resultsPath: replayResultsPath,
      summaryPath: replaySummaryPath,
      reportPath: replayReportPath,
    });

    const primaryCanonical = primaryRows;
    if (await fileExists(replayResultsPath)) {
      const replayText = await fs.readFile(replayResultsPath, "utf8");
      replayRows = parseReturnReversalResults(replayText);
    }

    primaryHash = hashCanonicalRows(primaryCanonical);
    replayHash = hashCanonicalRows(replayRows);
    determinismStatus = primaryHash === replayHash ? "PASS" : "FAIL";
  }

  out.evidenceEntry = {
    familyId: "return_reversal_v1",
    primaryHash,
    replayHash,
    determinismStatus,
    compareBasis,
  };
  out.determinismStatus = determinismStatus;
  out.compareBasis = compareBasis;
  return out;
}

function buildUnsupportedMomentumReport(args) {
  return {
    family_id: "momentum_v1",
    status: "unsupported_stream",
    exchange: args.exchange,
    symbol: args.symbol,
    stream: args.stream,
    window: `${args.start}..${args.end}`,
    result: {
      rows_produced: 0,
      selected_cell: null,
      pass_signal: false,
    },
  };
}

async function runMomentumOnce({
  args,
  outRoot,
  runDirName,
  resultsPath,
  summaryPath,
  reportPath,
}) {
  const cmd = [
    "python3 tools/hypotheses/momentum_v1.py",
    `--exchange ${quoteBash(args.exchange)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--symbol ${quoteBash(args.symbol)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    `--momDeltaMsList ${quoteBash(args.momDeltaMsList)}`,
    `--momHMsList ${quoteBash(args.momHMsList)}`,
    `--results-out ${quoteBash(resultsPath)}`,
    `--summary-out ${quoteBash(summaryPath)}`,
    `--report-out ${quoteBash(reportPath)}`,
    `--downloads-dir ${quoteBash(args.downloadsDir || "")}`,
    `--object-keys-tsv ${quoteBash(args.objectKeysTsv || "")}`,
    `--exchange-order ${quoteBash(args.exchange || "")}`,
  ].join(" ");
  const run = await runTimedStep({
    cwd: process.cwd(),
    runDir: path.join(outRoot, "runs", runDirName),
    commandBody: cmd,
  });
  return run;
}

async function parseFamilyMomentum({ args, outRoot }) {
  const baseAbs = path.join(outRoot, "artifacts", "multi_hypothesis");
  const primaryResultsPath = path.join(baseAbs, "family_momentum_primary_results.tsv");
  const primarySummaryPath = path.join(baseAbs, "family_momentum_primary_summary.tsv");
  const reportPath = path.join(baseAbs, "family_momentum_report.json");

  const streamNorm = String(args.stream || "").trim().toLowerCase();
  const supported = streamNorm === "trade" || streamNorm === "mark_price";
  const compareBasis = compareBasisFromHeader(MOM_HEADER);

  let primaryExitCode = 0;
  let primaryRows = [];
  if (!supported) {
    await writeText(primaryResultsPath, `${MOM_HEADER.join("\t")}\n`);
    await writeText(primarySummaryPath, `${MOM_HEADER.join("\t")}\n`);
    await writeText(reportPath, `${JSON.stringify(buildUnsupportedMomentumReport(args), null, 2)}\n`);
  } else {
    const primaryRun = await runMomentumOnce({
      args,
      outRoot,
      runDirName: "family_momentum_primary",
      resultsPath: primaryResultsPath,
      summaryPath: primarySummaryPath,
      reportPath,
    });
    primaryExitCode = primaryRun.exitCode;
    if (await fileExists(primaryResultsPath)) {
      const primaryText = await fs.readFile(primaryResultsPath, "utf8");
      primaryRows = parseMomentumResults(primaryText);
    }
  }

  const selected = pickMomentumSelected(primaryRows);
  const selectedMean = selected ? Number(selected.mean_product) : 0;
  const selectedT = selected ? Number(selected.t_stat) : 0;
  const selectedSupport = selected ? Number(selected.event_count) : 0;
  const passSignal = supported
    && selected != null
    && selectedMean > 0
    && selectedT >= 2
    && selectedSupport >= 200;

  const out = {
    familyId: "momentum_v1",
    exitCode: primaryExitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_momentum_report.json",
    patternsScanned: primaryRows.length,
    edgeCandidatesGenerated: 0,
    edgesSaved: passSignal,
    meanForwardReturn: fixed15(selectedMean),
    tStat: fixed15(selectedT),
    signalSupport: selectedSupport,
    files: {
      family_momentum_primary_results_relpath: "artifacts/multi_hypothesis/family_momentum_primary_results.tsv",
      family_momentum_primary_summary_relpath: "artifacts/multi_hypothesis/family_momentum_primary_summary.tsv",
      family_momentum_report_relpath: "artifacts/multi_hypothesis/family_momentum_report.json",
    },
    evidenceEntry: null,
    evidenceDecision: selected
      ? `family=momentum_v1,date=${selected.date},delta_ms=${selected.delta_ms},h_ms=${selected.h_ms},event_count=${selected.event_count},mean_product=${selected.mean_product},t_stat=${selected.t_stat}`
      : "family=momentum_v1,selected_cell=NONE",
  };

  if (!args.evidenceOn) {
    return out;
  }

  let replayRows = [];
  let primaryHash = SKIPPED_HASH_PLACEHOLDER;
  let replayHash = SKIPPED_HASH_PLACEHOLDER;
  let determinismStatus = supported ? "FAIL" : "SKIPPED_UNSUPPORTED_STREAM";

  if (supported) {
    const replayResultsPath = path.join(outRoot, "runs", "family_momentum_replay_on", "results.tsv");
    const replaySummaryPath = path.join(outRoot, "runs", "family_momentum_replay_on", "summary.tsv");
    const replayReportPath = path.join(outRoot, "runs", "family_momentum_replay_on", "report.json");
    await runMomentumOnce({
      args,
      outRoot,
      runDirName: "family_momentum_replay_on",
      resultsPath: replayResultsPath,
      summaryPath: replaySummaryPath,
      reportPath: replayReportPath,
    });

    const primaryCanonical = primaryRows;
    if (await fileExists(replayResultsPath)) {
      const replayText = await fs.readFile(replayResultsPath, "utf8");
      replayRows = parseMomentumResults(replayText);
    }

    primaryHash = hashCanonicalRows(primaryCanonical);
    replayHash = hashCanonicalRows(replayRows);
    determinismStatus = primaryHash === replayHash ? "PASS" : "FAIL";
  }

  out.evidenceEntry = {
    familyId: "momentum_v1",
    primaryHash,
    replayHash,
    determinismStatus,
    compareBasis,
  };
  out.determinismStatus = determinismStatus;
  out.compareBasis = compareBasis;
  return out;
}

function buildUnsupportedVolumeVolLinkReport(args) {
  return {
    family_id: "volume_vol_link_v1",
    status: "unsupported_stream",
    exchange: args.exchange,
    symbol: args.symbol,
    stream: args.stream,
    window: `${args.start}..${args.end}`,
    result: {
      rows_produced: 0,
      selected_cell: null,
      pass_signal: false,
    },
  };
}

async function runVolumeVolLinkOnce({
  args,
  outRoot,
  runDirName,
  resultsPath,
  summaryPath,
  reportPath,
}) {
  const cmd = [
    "python3 tools/hypotheses/volume_vol_link_v1.py",
    `--exchange ${quoteBash(args.exchange)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--symbol ${quoteBash(args.symbol)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    `--vvlDeltaMsList ${quoteBash(args.vvlDeltaMsList)}`,
    `--vvlHMsList ${quoteBash(args.vvlHMsList)}`,
    `--results-out ${quoteBash(resultsPath)}`,
    `--summary-out ${quoteBash(summaryPath)}`,
    `--report-out ${quoteBash(reportPath)}`,
    `--downloads-dir ${quoteBash(args.downloadsDir || "")}`,
    `--object-keys-tsv ${quoteBash(args.objectKeysTsv || "")}`,
    `--exchange-order ${quoteBash(args.exchange || "")}`,
  ].join(" ");
  const run = await runTimedStep({
    cwd: process.cwd(),
    runDir: path.join(outRoot, "runs", runDirName),
    commandBody: cmd,
  });
  return run;
}

async function parseFamilyVolumeVolLink({ args, outRoot }) {
  const baseAbs = path.join(outRoot, "artifacts", "multi_hypothesis");
  const primaryResultsPath = path.join(baseAbs, "family_volume_vol_link_primary_results.tsv");
  const primarySummaryPath = path.join(baseAbs, "family_volume_vol_link_primary_summary.tsv");
  const reportPath = path.join(baseAbs, "family_volume_vol_link_report.json");

  const streamNorm = String(args.stream || "").trim().toLowerCase();
  const supported = streamNorm === "trade";
  const compareBasis = compareBasisFromHeader(VVL_HEADER);

  let primaryExitCode = 0;
  let primaryRows = [];
  if (!supported) {
    await writeText(primaryResultsPath, `${VVL_HEADER.join("\t")}\n`);
    await writeText(primarySummaryPath, `${VVL_HEADER.join("\t")}\n`);
    await writeText(reportPath, `${JSON.stringify(buildUnsupportedVolumeVolLinkReport(args), null, 2)}\n`);
  } else {
    const primaryRun = await runVolumeVolLinkOnce({
      args,
      outRoot,
      runDirName: "family_volume_vol_link_primary",
      resultsPath: primaryResultsPath,
      summaryPath: primarySummaryPath,
      reportPath,
    });
    primaryExitCode = primaryRun.exitCode;
    if (await fileExists(primaryResultsPath)) {
      const primaryText = await fs.readFile(primaryResultsPath, "utf8");
      primaryRows = parseVolumeVolLinkResults(primaryText);
    }
  }

  const selected = pickVolumeVolLinkSelected(primaryRows);
  const selectedCorr = selected ? Number(selected.corr) : 0;
  const selectedT = selected ? Number(selected.t_stat) : 0;
  const selectedSupport = selected ? Number(selected.sample_count) : 0;
  const passSignal = supported
    && selected != null
    && selectedCorr > 0
    && selectedT >= 2
    && selectedSupport >= 200;

  const out = {
    familyId: "volume_vol_link_v1",
    exitCode: primaryExitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_volume_vol_link_report.json",
    patternsScanned: primaryRows.length,
    edgeCandidatesGenerated: 0,
    edgesSaved: passSignal,
    meanForwardReturn: fixed15(selectedCorr),
    tStat: fixed15(selectedT),
    signalSupport: selectedSupport,
    files: {
      family_volume_vol_link_primary_results_relpath: "artifacts/multi_hypothesis/family_volume_vol_link_primary_results.tsv",
      family_volume_vol_link_primary_summary_relpath: "artifacts/multi_hypothesis/family_volume_vol_link_primary_summary.tsv",
      family_volume_vol_link_report_relpath: "artifacts/multi_hypothesis/family_volume_vol_link_report.json",
    },
    evidenceEntry: null,
    evidenceDecision: selected
      ? `family=volume_vol_link_v1,date=${selected.date},delta_ms=${selected.delta_ms},h_ms=${selected.h_ms},sample_count=${selected.sample_count},corr=${selected.corr},t_stat=${selected.t_stat}`
      : "family=volume_vol_link_v1,selected_cell=NONE",
  };

  if (!args.evidenceOn) {
    return out;
  }

  let replayRows = [];
  let primaryHash = SKIPPED_HASH_PLACEHOLDER;
  let replayHash = SKIPPED_HASH_PLACEHOLDER;
  let determinismStatus = supported ? "FAIL" : "SKIPPED_UNSUPPORTED_STREAM";

  if (supported) {
    const replayResultsPath = path.join(outRoot, "runs", "family_volume_vol_link_replay_on", "results.tsv");
    const replaySummaryPath = path.join(outRoot, "runs", "family_volume_vol_link_replay_on", "summary.tsv");
    const replayReportPath = path.join(outRoot, "runs", "family_volume_vol_link_replay_on", "report.json");
    await runVolumeVolLinkOnce({
      args,
      outRoot,
      runDirName: "family_volume_vol_link_replay_on",
      resultsPath: replayResultsPath,
      summaryPath: replaySummaryPath,
      reportPath: replayReportPath,
    });

    const primaryCanonical = primaryRows;
    if (await fileExists(replayResultsPath)) {
      const replayText = await fs.readFile(replayResultsPath, "utf8");
      replayRows = parseVolumeVolLinkResults(replayText);
    }

    primaryHash = hashCanonicalRows(primaryCanonical);
    replayHash = hashCanonicalRows(replayRows);
    determinismStatus = primaryHash === replayHash ? "PASS" : "FAIL";
  }

  out.evidenceEntry = {
    familyId: "volume_vol_link_v1",
    primaryHash,
    replayHash,
    determinismStatus,
    compareBasis,
  };
  out.determinismStatus = determinismStatus;
  out.compareBasis = compareBasis;
  return out;
}

function buildUnsupportedJumpReversionReport(args) {
  return {
    family_id: "jump_reversion_v1",
    status: "unsupported_stream",
    exchange: args.exchange,
    symbol: args.symbol,
    stream: args.stream,
    window: `${args.start}..${args.end}`,
    result: {
      rows_produced: 0,
      selected_cell: null,
      pass_signal: false,
    },
  };
}

async function runJumpReversionOnce({
  args,
  outRoot,
  runDirName,
  resultsPath,
  summaryPath,
  reportPath,
}) {
  const cmd = [
    "python3 tools/hypotheses/jump_reversion_v1.py",
    `--exchange ${quoteBash(args.exchange)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--symbol ${quoteBash(args.symbol)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    `--jrJumpThreshBpsList ${quoteBash(args.jrJumpThreshBpsList)}`,
    `--jrHMsList ${quoteBash(args.jrHMsList)}`,
    `--jrCooldownMs ${quoteBash(args.jrCooldownMs)}`,
    `--results-out ${quoteBash(resultsPath)}`,
    `--summary-out ${quoteBash(summaryPath)}`,
    `--report-out ${quoteBash(reportPath)}`,
    `--downloads-dir ${quoteBash(args.downloadsDir || "")}`,
    `--object-keys-tsv ${quoteBash(args.objectKeysTsv || "")}`,
    `--exchange-order ${quoteBash(args.exchange || "")}`,
  ].join(" ");
  const run = await runTimedStep({
    cwd: process.cwd(),
    runDir: path.join(outRoot, "runs", runDirName),
    commandBody: cmd,
  });
  return run;
}

async function parseFamilyJumpReversion({ args, outRoot }) {
  const baseAbs = path.join(outRoot, "artifacts", "multi_hypothesis");
  const primaryResultsPath = path.join(baseAbs, "family_jump_reversion_primary_results.tsv");
  const primarySummaryPath = path.join(baseAbs, "family_jump_reversion_primary_summary.tsv");
  const reportPath = path.join(baseAbs, "family_jump_reversion_report.json");

  const streamNorm = String(args.stream || "").trim().toLowerCase();
  const supported = streamNorm === "trade" || streamNorm === "mark_price";
  const compareBasis = compareBasisFromHeader(JR_HEADER);

  let primaryExitCode = 0;
  let primaryRows = [];
  if (!supported) {
    await writeText(primaryResultsPath, `${JR_HEADER.join("\t")}\n`);
    await writeText(primarySummaryPath, `${JR_HEADER.join("\t")}\n`);
    await writeText(reportPath, `${JSON.stringify(buildUnsupportedJumpReversionReport(args), null, 2)}\n`);
  } else {
    const primaryRun = await runJumpReversionOnce({
      args,
      outRoot,
      runDirName: "family_jump_reversion_primary",
      resultsPath: primaryResultsPath,
      summaryPath: primarySummaryPath,
      reportPath,
    });
    primaryExitCode = primaryRun.exitCode;
    if (await fileExists(primaryResultsPath)) {
      const primaryText = await fs.readFile(primaryResultsPath, "utf8");
      primaryRows = parseJumpReversionResults(primaryText);
    }
  }

  const selected = pickJumpReversionSelected(primaryRows);
  const selectedMean = selected ? Number(selected.mean_signed_reversal) : 0;
  const selectedT = selected ? Number(selected.t_stat) : 0;
  const selectedSupport = selected ? Number(selected.jump_count) : 0;
  const passSignal = supported
    && selected != null
    && selectedMean > 0
    && selectedT >= 2
    && selectedSupport >= 200;

  const out = {
    familyId: "jump_reversion_v1",
    exitCode: primaryExitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_jump_reversion_report.json",
    patternsScanned: primaryRows.length,
    edgeCandidatesGenerated: 0,
    edgesSaved: passSignal,
    meanForwardReturn: fixed15(selectedMean),
    tStat: fixed15(selectedT),
    signalSupport: selectedSupport,
    files: {
      family_jump_reversion_primary_results_relpath: "artifacts/multi_hypothesis/family_jump_reversion_primary_results.tsv",
      family_jump_reversion_primary_summary_relpath: "artifacts/multi_hypothesis/family_jump_reversion_primary_summary.tsv",
      family_jump_reversion_report_relpath: "artifacts/multi_hypothesis/family_jump_reversion_report.json",
    },
    evidenceEntry: null,
    evidenceDecision: selected
      ? `family=jump_reversion_v1,date=${selected.date},jump_thresh_bps=${selected.jump_thresh_bps},h_ms=${selected.h_ms},jump_count=${selected.jump_count},mean_signed_reversal=${selected.mean_signed_reversal},t_stat=${selected.t_stat}`
      : "family=jump_reversion_v1,selected_cell=NONE",
  };

  if (!args.evidenceOn) {
    return out;
  }

  let replayRows = [];
  let primaryHash = SKIPPED_HASH_PLACEHOLDER;
  let replayHash = SKIPPED_HASH_PLACEHOLDER;
  let determinismStatus = supported ? "FAIL" : "SKIPPED_UNSUPPORTED_STREAM";

  if (supported) {
    const replayResultsPath = path.join(outRoot, "runs", "family_jump_reversion_replay_on", "results.tsv");
    const replaySummaryPath = path.join(outRoot, "runs", "family_jump_reversion_replay_on", "summary.tsv");
    const replayReportPath = path.join(outRoot, "runs", "family_jump_reversion_replay_on", "report.json");
    await runJumpReversionOnce({
      args,
      outRoot,
      runDirName: "family_jump_reversion_replay_on",
      resultsPath: replayResultsPath,
      summaryPath: replaySummaryPath,
      reportPath: replayReportPath,
    });

    const primaryCanonical = primaryRows;
    if (await fileExists(replayResultsPath)) {
      const replayText = await fs.readFile(replayResultsPath, "utf8");
      replayRows = parseJumpReversionResults(replayText);
    }

    primaryHash = hashCanonicalRows(primaryCanonical);
    replayHash = hashCanonicalRows(replayRows);
    determinismStatus = primaryHash === replayHash ? "PASS" : "FAIL";
  }

  out.evidenceEntry = {
    familyId: "jump_reversion_v1",
    primaryHash,
    replayHash,
    determinismStatus,
    compareBasis,
  };
  out.determinismStatus = determinismStatus;
  out.compareBasis = compareBasis;
  return out;
}

function buildUnsupportedVolatilityReport(args) {
  return {
    family_id: "volatility_clustering_v1",
    status: "unsupported_stream",
    exchange: args.exchange,
    symbol: args.symbol,
    stream: args.stream,
    window: `${args.start}..${args.end}`,
    result: {
      rows_produced: 0,
      selected_cell: null,
      pass_signal: false,
    },
  };
}

async function runVolatilityOnce({
  args,
  outRoot,
  runDirName,
  resultsPath,
  summaryPath,
  reportPath,
}) {
  const cmd = [
    "python3 tools/hypotheses/volatility_clustering_v1.py",
    `--exchange ${quoteBash(args.exchange)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--symbol ${quoteBash(args.symbol)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    `--vcDeltaMsList ${quoteBash(args.vcDeltaMsList)}`,
    `--vcHMsList ${quoteBash(args.vcHMsList)}`,
    `--results-out ${quoteBash(resultsPath)}`,
    `--summary-out ${quoteBash(summaryPath)}`,
    `--report-out ${quoteBash(reportPath)}`,
    `--downloads-dir ${quoteBash(args.downloadsDir || "")}`,
    `--object-keys-tsv ${quoteBash(args.objectKeysTsv || "")}`,
    `--exchange-order ${quoteBash(args.exchange || "")}`,
  ].join(" ");
  const run = await runTimedStep({
    cwd: process.cwd(),
    runDir: path.join(outRoot, "runs", runDirName),
    commandBody: cmd,
  });
  return run;
}

async function parseFamilyVolClust({ args, outRoot }) {
  const baseAbs = path.join(outRoot, "artifacts", "multi_hypothesis");
  const primaryResultsPath = path.join(baseAbs, "family_volatility_clustering_primary_results.tsv");
  const primarySummaryPath = path.join(baseAbs, "family_volatility_clustering_primary_summary.tsv");
  const reportPath = path.join(baseAbs, "family_volatility_clustering_report.json");

  const streamNorm = String(args.stream || "").trim().toLowerCase();
  const supported = streamNorm === "trade" || streamNorm === "mark_price";
  const compareBasis = compareBasisFromHeader(VC_HEADER);

  let primaryExitCode = 0;
  let primaryRows = [];
  if (!supported) {
    await writeText(primaryResultsPath, `${VC_HEADER.join("\t")}\n`);
    await writeText(primarySummaryPath, `${VC_HEADER.join("\t")}\n`);
    await writeText(reportPath, `${JSON.stringify(buildUnsupportedVolatilityReport(args), null, 2)}\n`);
  } else {
    const primaryRun = await runVolatilityOnce({
      args,
      outRoot,
      runDirName: "family_volatility_clustering_primary",
      resultsPath: primaryResultsPath,
      summaryPath: primarySummaryPath,
      reportPath,
    });
    primaryExitCode = primaryRun.exitCode;
    if (await fileExists(primaryResultsPath)) {
      const primaryText = await fs.readFile(primaryResultsPath, "utf8");
      primaryRows = parseVolatilityClusteringResults(primaryText);
    }
  }

  const selected = pickVolatilityClusteringSelected(primaryRows);
  const selectedCorr = selected ? Number(selected.corr) : 0;
  const selectedT = selected ? Number(selected.t_stat) : 0;
  const selectedSupport = selected ? Number(selected.sample_count) : 0;
  const passSignal = supported
    && selected != null
    && selectedCorr > 0
    && selectedT >= 2
    && selectedSupport >= 200;

  const out = {
    familyId: "volatility_clustering_v1",
    exitCode: primaryExitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_volatility_clustering_report.json",
    patternsScanned: primaryRows.length,
    edgeCandidatesGenerated: 0,
    edgesSaved: passSignal,
    meanForwardReturn: fixed15(selectedCorr),
    tStat: fixed15(selectedT),
    signalSupport: selectedSupport,
    files: {
      family_volatility_clustering_primary_results_relpath: "artifacts/multi_hypothesis/family_volatility_clustering_primary_results.tsv",
      family_volatility_clustering_primary_summary_relpath: "artifacts/multi_hypothesis/family_volatility_clustering_primary_summary.tsv",
      family_volatility_clustering_report_relpath: "artifacts/multi_hypothesis/family_volatility_clustering_report.json",
    },
    evidenceEntry: null,
    evidenceDecision: selected
      ? `family=volatility_clustering_v1,date=${selected.date},delta_ms=${selected.delta_ms},h_ms=${selected.h_ms},sample_count=${selected.sample_count},corr=${selected.corr},t_stat=${selected.t_stat}`
      : "family=volatility_clustering_v1,selected_cell=NONE",
  };

  if (!args.evidenceOn) {
    return out;
  }

  let replayRows = [];
  let primaryHash = SKIPPED_HASH_PLACEHOLDER;
  let replayHash = SKIPPED_HASH_PLACEHOLDER;
  let determinismStatus = supported ? "FAIL" : "SKIPPED_UNSUPPORTED_STREAM";

  if (supported) {
    const replayResultsPath = path.join(outRoot, "runs", "family_volatility_clustering_replay_on", "results.tsv");
    const replaySummaryPath = path.join(outRoot, "runs", "family_volatility_clustering_replay_on", "summary.tsv");
    const replayReportPath = path.join(outRoot, "runs", "family_volatility_clustering_replay_on", "report.json");
    await runVolatilityOnce({
      args,
      outRoot,
      runDirName: "family_volatility_clustering_replay_on",
      resultsPath: replayResultsPath,
      summaryPath: replaySummaryPath,
      reportPath: replayReportPath,
    });

    const primaryCanonical = primaryRows;
    if (await fileExists(replayResultsPath)) {
      const replayText = await fs.readFile(replayResultsPath, "utf8");
      replayRows = parseVolatilityClusteringResults(replayText);
    }

    primaryHash = hashCanonicalRows(primaryCanonical);
    replayHash = hashCanonicalRows(replayRows);
    determinismStatus = primaryHash === replayHash ? "PASS" : "FAIL";
  }

  out.evidenceEntry = {
    familyId: "volatility_clustering_v1",
    primaryHash,
    replayHash,
    determinismStatus,
    compareBasis,
  };
  out.determinismStatus = determinismStatus;
  out.compareBasis = compareBasis;
  return out;
}

function buildUnsupportedSpreadReversionReport(args) {
  return {
    family_id: "spread_reversion_v1",
    status: "unsupported_stream",
    exchange: args.exchange,
    symbol: args.symbol,
    stream: args.stream,
    window: `${args.start}..${args.end}`,
    result: {
      rows_produced: 0,
      selected_cell: null,
      pass_signal: false,
    },
  };
}

async function runSpreadReversionOnce({
  args,
  outRoot,
  runDirName,
  resultsPath,
  summaryPath,
  reportPath,
}) {
  const cmd = [
    "python3 tools/hypotheses/spread_reversion_v1.py",
    `--exchange ${quoteBash(args.exchange)}`,
    `--stream ${quoteBash(args.stream)}`,
    `--symbol ${quoteBash(args.symbol)}`,
    `--start ${quoteBash(args.start)}`,
    `--end ${quoteBash(args.end)}`,
    `--srDeltaMsList ${quoteBash(args.srDeltaMsList)}`,
    `--srHMsList ${quoteBash(args.srHMsList)}`,
    `--results-out ${quoteBash(resultsPath)}`,
    `--summary-out ${quoteBash(summaryPath)}`,
    `--report-out ${quoteBash(reportPath)}`,
    `--downloads-dir ${quoteBash(args.downloadsDir || "")}`,
    `--object-keys-tsv ${quoteBash(args.objectKeysTsv || "")}`,
    `--exchange-order ${quoteBash(args.exchange || "")}`,
  ].join(" ");
  const run = await runTimedStep({
    cwd: process.cwd(),
    runDir: path.join(outRoot, "runs", runDirName),
    commandBody: cmd,
  });
  return run;
}

async function parseFamilySpreadReversion({ args, outRoot }) {
  const baseAbs = path.join(outRoot, "artifacts", "multi_hypothesis");
  const primaryResultsPath = path.join(baseAbs, "family_spread_reversion_primary_results.tsv");
  const primarySummaryPath = path.join(baseAbs, "family_spread_reversion_primary_summary.tsv");
  const reportPath = path.join(baseAbs, "family_spread_reversion_report.json");

  const streamNorm = String(args.stream || "").trim().toLowerCase();
  const supported = streamNorm === "bbo";
  const compareBasis = compareBasisFromHeader(SR_HEADER);

  let primaryExitCode = 0;
  let primaryRows = [];
  if (!supported) {
    await writeText(primaryResultsPath, `${SR_HEADER.join("\t")}\n`);
    await writeText(primarySummaryPath, `${SR_HEADER.join("\t")}\n`);
    await writeText(reportPath, `${JSON.stringify(buildUnsupportedSpreadReversionReport(args), null, 2)}\n`);
  } else {
    const primaryRun = await runSpreadReversionOnce({
      args,
      outRoot,
      runDirName: "family_spread_reversion_primary",
      resultsPath: primaryResultsPath,
      summaryPath: primarySummaryPath,
      reportPath,
    });
    primaryExitCode = primaryRun.exitCode;
    if (await fileExists(primaryResultsPath)) {
      const primaryText = await fs.readFile(primaryResultsPath, "utf8");
      primaryRows = parseSpreadReversionResults(primaryText);
    }
  }

  const selected = pickSpreadReversionSelected(primaryRows);
  const selectedMean = selected ? Number(selected.mean_product) : 0;
  const selectedT = selected ? Number(selected.t_stat) : 0;
  const selectedSupport = selected ? Number(selected.event_count) : 0;
  const passSignal = supported
    && selected != null
    && selectedMean < 0
    && selectedT <= -2
    && selectedSupport >= 200;

  const out = {
    familyId: "spread_reversion_v1",
    exitCode: primaryExitCode,
    reportRelpath: "artifacts/multi_hypothesis/family_spread_reversion_report.json",
    patternsScanned: primaryRows.length,
    edgeCandidatesGenerated: 0,
    edgesSaved: passSignal,
    meanForwardReturn: fixed15(selectedMean),
    tStat: fixed15(selectedT),
    signalSupport: selectedSupport,
    files: {
      family_spread_reversion_primary_results_relpath: "artifacts/multi_hypothesis/family_spread_reversion_primary_results.tsv",
      family_spread_reversion_primary_summary_relpath: "artifacts/multi_hypothesis/family_spread_reversion_primary_summary.tsv",
      family_spread_reversion_report_relpath: "artifacts/multi_hypothesis/family_spread_reversion_report.json",
    },
    evidenceEntry: null,
    evidenceDecision: selected
      ? `family=spread_reversion_v1,date=${selected.date},delta_ms=${selected.delta_ms},h_ms=${selected.h_ms},event_count=${selected.event_count},mean_product=${selected.mean_product},t_stat=${selected.t_stat}`
      : "family=spread_reversion_v1,selected_cell=NONE",
  };

  if (!args.evidenceOn) {
    return out;
  }

  let replayRows = [];
  let primaryHash = SKIPPED_HASH_PLACEHOLDER;
  let replayHash = SKIPPED_HASH_PLACEHOLDER;
  let determinismStatus = supported ? "FAIL" : "SKIPPED_UNSUPPORTED_STREAM";

  if (supported) {
    const replayResultsPath = path.join(outRoot, "runs", "family_spread_reversion_replay_on", "results.tsv");
    const replaySummaryPath = path.join(outRoot, "runs", "family_spread_reversion_replay_on", "summary.tsv");
    const replayReportPath = path.join(outRoot, "runs", "family_spread_reversion_replay_on", "report.json");
    await runSpreadReversionOnce({
      args,
      outRoot,
      runDirName: "family_spread_reversion_replay_on",
      resultsPath: replayResultsPath,
      summaryPath: replaySummaryPath,
      reportPath: replayReportPath,
    });

    const primaryCanonical = primaryRows;
    if (await fileExists(replayResultsPath)) {
      const replayText = await fs.readFile(replayResultsPath, "utf8");
      replayRows = parseSpreadReversionResults(replayText);
    }

    primaryHash = hashCanonicalRows(primaryCanonical);
    replayHash = hashCanonicalRows(replayRows);
    determinismStatus = primaryHash === replayHash ? "PASS" : "FAIL";
  }

  out.evidenceEntry = {
    familyId: "spread_reversion_v1",
    primaryHash,
    replayHash,
    determinismStatus,
    compareBasis,
  };
  out.determinismStatus = determinismStatus;
  out.compareBasis = compareBasis;
  return out;
}

const FAMILY_TABLE = Object.freeze([
  {
    familyId: "family_a_patternscanner",
    kind: "legacy",
    supportedStreams: [],
    headerConst: null,
    compareBasis: "",
    parseFn: parseFamilyA,
    selectionFn: null,
    evidenceEligible: false,
  },
  {
    familyId: "family_b_simple_momentum",
    kind: "legacy",
    supportedStreams: [],
    headerConst: null,
    compareBasis: "",
    parseFn: parseFamilyB,
    selectionFn: null,
    evidenceEligible: false,
  },
  {
    familyId: "return_reversal_v1",
    kind: "hypothesis",
    supportedStreams: ["trade", "mark_price"],
    headerConst: RR_HEADER,
    compareBasis: compareBasisFromHeader(RR_HEADER),
    parseFn: parseFamilyReturnReversal,
    selectionFn: pickReturnReversalSelected,
    evidenceEligible: true,
  },
  {
    familyId: "momentum_v1",
    kind: "hypothesis",
    supportedStreams: ["trade", "mark_price"],
    headerConst: MOM_HEADER,
    compareBasis: compareBasisFromHeader(MOM_HEADER),
    parseFn: parseFamilyMomentum,
    selectionFn: pickMomentumSelected,
    evidenceEligible: true,
  },
  {
    familyId: "volatility_clustering_v1",
    kind: "hypothesis",
    supportedStreams: ["trade", "mark_price"],
    headerConst: VC_HEADER,
    compareBasis: compareBasisFromHeader(VC_HEADER),
    parseFn: parseFamilyVolClust,
    selectionFn: pickVolatilityClusteringSelected,
    evidenceEligible: true,
  },
  {
    familyId: "spread_reversion_v1",
    kind: "hypothesis",
    supportedStreams: ["bbo"],
    headerConst: SR_HEADER,
    compareBasis: compareBasisFromHeader(SR_HEADER),
    parseFn: parseFamilySpreadReversion,
    selectionFn: pickSpreadReversionSelected,
    evidenceEligible: true,
  },
  {
    familyId: "volume_vol_link_v1",
    kind: "hypothesis",
    supportedStreams: ["trade"],
    headerConst: VVL_HEADER,
    compareBasis: compareBasisFromHeader(VVL_HEADER),
    parseFn: parseFamilyVolumeVolLink,
    selectionFn: pickVolumeVolLinkSelected,
    evidenceEligible: true,
  },
  {
    familyId: "jump_reversion_v1",
    kind: "hypothesis",
    supportedStreams: ["trade", "mark_price"],
    headerConst: JR_HEADER,
    compareBasis: compareBasisFromHeader(JR_HEADER),
    parseFn: parseFamilyJumpReversion,
    selectionFn: pickJumpReversionSelected,
    evidenceEligible: true,
  },
]);

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outRoot = path.resolve(process.cwd(), args.outDir);
  await mkdirp(path.join(outRoot, "artifacts", "multi_hypothesis"));
  await mkdirp(path.join(outRoot, "runs"));
  const resolved = await resolveParquetInputs({ args, outRoot });

  let rows = [];
  const familyRowsById = new Map();
  let rrRow = null;
  let vcRow = null;
  let srRow = null;
  let momRow = null;
  let vvlRow = null;
  let jrRow = null;
  let evidenceFiles = null;
  try {
    rows = [];
    familyRowsById.clear();
    for (const familyMeta of FAMILY_TABLE) {
      const familyRow = await familyMeta.parseFn({ args, outRoot });
      rows.push(familyRow);
      familyRowsById.set(familyMeta.familyId, familyRow);
    }

    rrRow = familyRowsById.get("return_reversal_v1") || null;
    momRow = familyRowsById.get("momentum_v1") || null;
    vcRow = familyRowsById.get("volatility_clustering_v1") || null;
    srRow = familyRowsById.get("spread_reversion_v1") || null;
    vvlRow = familyRowsById.get("volume_vol_link_v1") || null;
    jrRow = familyRowsById.get("jump_reversion_v1") || null;

    if (args.evidenceOn) {
      const evidenceEntries = [];
      const evidenceDecisions = [];
      for (const familyMeta of FAMILY_TABLE) {
        if (!familyMeta.evidenceEligible) continue;
        const familyRow = familyRowsById.get(familyMeta.familyId);
        if (familyRow && familyRow.evidenceEntry) evidenceEntries.push(familyRow.evidenceEntry);
        if (familyRow && familyRow.evidenceDecision) evidenceDecisions.push(familyRow.evidenceDecision);
      }
      evidenceFiles = await writeMinimalEvidenceFiles({
        outRoot,
        windowStr: `${args.start}..${args.end}`,
        entries: evidenceEntries,
        decisions: evidenceDecisions,
      });
    }
  } finally {
    await cleanupMaterializedCurated(resolved.materializedCurated);
  }

  const windowStr = `${args.start}..${args.end}`;
  const rollupLines = [
    "window\tfamily_id\texit_code\tpatterns_scanned\tedge_candidates_generated\tedges_saved\treport_relpath\tmetric_mean_forward_return\tmetric_t_stat\tsignal_support",
    ...rows.map((r) => [
      windowStr,
      r.familyId,
      String(r.exitCode),
      String(r.patternsScanned),
      String(r.edgeCandidatesGenerated),
      r.edgesSaved ? "true" : "false",
      r.reportRelpath,
      String(r.meanForwardReturn ?? ""),
      String(r.tStat ?? ""),
      String(r.signalSupport ?? ""),
    ].join("\t")),
  ];

  await writeText(path.join(outRoot, "artifacts", "multi_hypothesis", "rollup.tsv"), `${rollupLines.join("\n")}\n`);

  const summary = {
    exchange: args.exchange,
    stream: args.stream,
    symbol: args.symbol,
    window: windowStr,
    families: rows,
    resolved_paths_relpath: resolved.logRelpath,
    files: {
      rollup_relpath: "artifacts/multi_hypothesis/rollup.tsv",
      family_a_relpath: "artifacts/multi_hypothesis/family_A_report.json",
      family_b_relpath: "artifacts/multi_hypothesis/family_B_report.json",
      scanner_config_relpath: "artifacts/multi_hypothesis/scanner_config_excerpt_family_A.txt",
      resolved_paths_relpath: resolved.logRelpath,
      family_return_reversal_primary_results_relpath: rrRow && rrRow.files ? rrRow.files.family_return_reversal_primary_results_relpath : "",
      family_return_reversal_primary_summary_relpath: rrRow && rrRow.files ? rrRow.files.family_return_reversal_primary_summary_relpath : "",
      family_return_reversal_report_relpath: rrRow && rrRow.files ? rrRow.files.family_return_reversal_report_relpath : "",
      family_momentum_primary_results_relpath: momRow && momRow.files ? momRow.files.family_momentum_primary_results_relpath : "",
      family_momentum_primary_summary_relpath: momRow && momRow.files ? momRow.files.family_momentum_primary_summary_relpath : "",
      family_momentum_report_relpath: momRow && momRow.files ? momRow.files.family_momentum_report_relpath : "",
      family_volatility_clustering_primary_results_relpath: vcRow && vcRow.files ? vcRow.files.family_volatility_clustering_primary_results_relpath : "",
      family_volatility_clustering_primary_summary_relpath: vcRow && vcRow.files ? vcRow.files.family_volatility_clustering_primary_summary_relpath : "",
      family_volatility_clustering_report_relpath: vcRow && vcRow.files ? vcRow.files.family_volatility_clustering_report_relpath : "",
      family_spread_reversion_primary_results_relpath: srRow && srRow.files ? srRow.files.family_spread_reversion_primary_results_relpath : "",
      family_spread_reversion_primary_summary_relpath: srRow && srRow.files ? srRow.files.family_spread_reversion_primary_summary_relpath : "",
      family_spread_reversion_report_relpath: srRow && srRow.files ? srRow.files.family_spread_reversion_report_relpath : "",
      family_volume_vol_link_primary_results_relpath: vvlRow && vvlRow.files ? vvlRow.files.family_volume_vol_link_primary_results_relpath : "",
      family_volume_vol_link_primary_summary_relpath: vvlRow && vvlRow.files ? vvlRow.files.family_volume_vol_link_primary_summary_relpath : "",
      family_volume_vol_link_report_relpath: vvlRow && vvlRow.files ? vvlRow.files.family_volume_vol_link_report_relpath : "",
      family_jump_reversion_primary_results_relpath: jrRow && jrRow.files ? jrRow.files.family_jump_reversion_primary_results_relpath : "",
      family_jump_reversion_primary_summary_relpath: jrRow && jrRow.files ? jrRow.files.family_jump_reversion_primary_summary_relpath : "",
      family_jump_reversion_report_relpath: jrRow && jrRow.files ? jrRow.files.family_jump_reversion_report_relpath : "",
    },
  };

  if (args.evidenceOn && evidenceFiles) {
    summary.files.determinism_compare_relpath = evidenceFiles.determinismRel || "";
    summary.files.artifact_manifest_relpath = evidenceFiles.manifestRel || "";
    summary.files.label_report_relpath = evidenceFiles.labelRel || "";
    summary.files.integrity_check_relpath = evidenceFiles.integrityRel || "";
  }

  await writeText(path.join(outRoot, "artifacts", "multi_hypothesis", "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);

  console.log(`OUT_ROOT=${outRoot}`);
  console.log(`ROLLUP=${path.join(outRoot, "artifacts", "multi_hypothesis", "rollup.tsv")}`);
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
