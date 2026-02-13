#!/usr/bin/env node

const fs = require("fs/promises");
const path = require("path");
const { spawn } = require("child_process");

function usage() {
  console.error(
    [
      "usage: node tools/run-multi-hypothesis.js ",
      "--exchange <exchange> --stream <stream> --symbol <symbol> --start <YYYYMMDD> --end <YYYYMMDD>",
      "[--outDir <dir>] [--heapMB <n>] [--progressEvery <n>] [--objectKeysTsv <path>] [--downloadsDir <path>]",
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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outRoot = path.resolve(process.cwd(), args.outDir);
  await mkdirp(path.join(outRoot, "artifacts", "multi_hypothesis"));
  await mkdirp(path.join(outRoot, "runs"));
  const resolved = await resolveParquetInputs({ args, outRoot });

  let rows = [];
  try {
    rows = [];
    rows.push(await parseFamilyA({ args, outRoot }));
    rows.push(await parseFamilyB({ args, outRoot }));
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
    },
  };

  await writeText(path.join(outRoot, "artifacts", "multi_hypothesis", "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);

  console.log(`OUT_ROOT=${outRoot}`);
  console.log(`ROLLUP=${path.join(outRoot, "artifacts", "multi_hypothesis", "rollup.tsv")}`);
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
