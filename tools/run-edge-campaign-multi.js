#!/usr/bin/env node

import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";

const DEFAULT_EXCHANGE = "binance";
const DEFAULT_DAY_QUALITY = "GOOD";
const VALID_DAY_QUALITY = new Set(["GOOD"]);
const DEFAULT_SYMBOLS = [
  "BTC/USDT",
  "ETH/USDT",
  "SOL/USDT",
  "XRP/USDT",
  "BNB/USDT",
  "ADA/USDT"
];

function printHelp(exitCode = 0) {
  const msg = `
tools/run-edge-campaign-multi.js

Optional flags:
  --exchange <name>               (default: binance)
  --dayQuality <GOOD>             (default: GOOD)
  --streams <csv>                 (optional; ex: bbo,trades,aggTrades)

Behavior:
  - If --streams is not provided, streams are auto-discovered from:
    data/curated/exchange=<exchange>/stream=*
  - For each stream, one master pack is produced:
    edge-campaign-multi-v0-<exchange>_<stream>_<YYYYMMDD>_<HHMMSS>
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

  const allowed = new Set(["help", "exchange", "dayQuality", "streams"]);
  for (const key of Object.keys(out)) {
    if (!allowed.has(key)) {
      fatal(`Unknown flag: --${key}`);
    }
  }

  return out;
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseCsvList(raw) {
  const values = String(raw || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);

  const seen = new Set();
  const out = [];
  for (const v of values) {
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
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

async function writeText(p, content) {
  await mkdirp(path.dirname(p));
  await fs.writeFile(p, content, "utf8");
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

async function listSymbolTarballs({ exchange, stream, symbolNorm }) {
  const entries = await fs.readdir("evidence");
  const re = new RegExp(`^edge-campaign-v0-${escapeRegExp(exchange)}_${escapeRegExp(stream)}_${escapeRegExp(symbolNorm)}_[0-9]{8}_[0-9]{6}\\.tar\\.gz$`);
  return entries.filter((x) => re.test(x)).sort();
}

async function runTimed({ cwd, runDir, commandArgs }) {
  await mkdirp(runDir);
  const stdoutPath = path.join(runDir, "stdout.log");
  const stderrPath = path.join(runDir, "stderr.log");
  const timeVPath = path.join(runDir, "time-v.log");
  const exitPath = path.join(runDir, "exit_code.txt");
  const cmdPath = path.join(runDir, "cmd.txt");

  await writeText(cmdPath, `${commandArgs.join(" ")}\n`);

  const stdout = createWriteStream(stdoutPath, { flags: "w" });
  const stderr = createWriteStream(stderrPath, { flags: "w" });

  const exitCode = await new Promise((resolve) => {
    const child = spawn("/usr/bin/time", ["-v", "-o", timeVPath, "--", ...commandArgs], {
      cwd,
      stdio: ["ignore", "pipe", "pipe"]
    });

    child.stdout.pipe(stdout);
    child.stderr.pipe(stderr);

    child.on("error", (err) => {
      stderr.write(`spawn_error: ${String(err)}\n`);
      stdout.end(() => stderr.end(() => resolve(1)));
    });

    child.on("close", (code) => {
      stdout.end(() => stderr.end(() => resolve(code ?? 1)));
    });
  });

  await writeText(exitPath, `${exitCode}\n`);

  return { exitCode, stdoutPath, stderrPath, timeVPath, exitPath, cmdPath };
}

function parseWallSeconds(timeVText) {
  const m = timeVText.match(/Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*(.+)$/m);
  if (!m) return null;
  const parts = m[1].trim().split(":").map((x) => Number.parseFloat(x));
  if (parts.some((x) => !Number.isFinite(x))) return null;
  if (parts.length === 3) return Number((parts[0] * 3600 + parts[1] * 60 + parts[2]).toFixed(2));
  if (parts.length === 2) return Number((parts[0] * 60 + parts[1]).toFixed(2));
  if (parts.length === 1) return Number(parts[0].toFixed(2));
  return null;
}

async function parseRunMeta(runInfo) {
  const timeV = await fs.readFile(runInfo.timeVPath, "utf8");
  const wallS = parseWallSeconds(timeV);
  const rssMatch = timeV.match(/Maximum resident set size \(kbytes\):\s*(\d+)/m);
  const maxRssKb = rssMatch ? Number.parseInt(rssMatch[1], 10) : null;
  return { wallS, maxRssKb };
}

async function finalizeMasterPack({ packName, packRoot, tarPath, shaPath, movedPath }) {
  const today = todayUTCYYYYMMDD();
  const archiveRoot = path.join("..", "quantlab-evidence-archive", `${today}_slim`);
  const archiveDest = path.join(archiveRoot, packName);

  if (await fileExists(archiveDest)) {
    throw new Error(`Archive destination already exists: ${archiveDest}`);
  }

  await mkdirp(path.dirname(tarPath));

  const tarRes = await runTimed({
    cwd: process.cwd(),
    runDir: path.join(packRoot, "finalize_tar"),
    commandArgs: ["bash", "-lc", `set -euo pipefail\ntar -cf - -C evidence ${packName} | gzip -n > ${tarPath}`]
  });
  if (tarRes.exitCode !== 0) throw new Error(`master tar finalize failed: exit=${tarRes.exitCode}`);

  const shaRes = await runTimed({
    cwd: process.cwd(),
    runDir: path.join(packRoot, "finalize_sha"),
    commandArgs: ["bash", "-lc", `set -euo pipefail\nsha256sum ${tarPath} > ${shaPath}\nsha256sum -c ${shaPath}`]
  });
  if (shaRes.exitCode !== 0) throw new Error(`master sha finalize failed: exit=${shaRes.exitCode}`);

  await mkdirp(archiveRoot);
  await fs.rename(packRoot, archiveDest);
  await writeText(movedPath, `${archiveDest}\n`);
}

function toRel(packRoot, absPath) {
  return path.relative(packRoot, absPath).replace(/\\/g, "/");
}

async function integrityCheck(packRoot) {
  const readme = await fs.readFile(path.join(packRoot, "README.md"), "utf8");
  const summary = await fs.readFile(path.join(packRoot, "master_summary.json"), "utf8");
  const results = await fs.readFile(path.join(packRoot, "master_results.tsv"), "utf8");

  const relpathRegex = /\b(?:packs|runs)\/[A-Za-z0-9_./-]+|\b(?:master_results\.tsv|master_summary\.json)\b/g;
  const rels = new Set();

  for (const txt of [readme, summary, results]) {
    const matches = txt.match(relpathRegex) || [];
    for (const m of matches) rels.add(m);
  }

  const sorted = [...rels].sort();
  await writeText(path.join(packRoot, "integrity_relpaths.txt"), `${sorted.join("\n")}\n`);

  let missing = 0;
  for (const rel of sorted) {
    if (!(await fileExists(path.join(packRoot, rel)))) missing += 1;
  }

  await writeText(path.join(packRoot, "integrity_check.txt"), `missing=${missing}\n`);
  return missing;
}

function selectBestWindow(summaryRows) {
  if (!Array.isArray(summaryRows) || summaryRows.length === 0) return null;
  const pass = summaryRows.find((r) => r && r.acceptance_pass === true);
  if (pass) return pass;
  const rank1 = summaryRows.find((r) => Number(r?.rank) === 1);
  return rank1 || summaryRows[0];
}

async function discoverStreams(exchange) {
  const exchangeRoot = path.join("data", "curated", `exchange=${exchange}`);
  if (!(await fileExists(exchangeRoot))) {
    throw new Error(`exchange curated root not found: ${exchangeRoot}`);
  }

  const entries = await fs.readdir(exchangeRoot, { withFileTypes: true });
  const streams = entries
    .filter((e) => e.isDirectory() && e.name.startsWith("stream="))
    .map((e) => e.name.slice("stream=".length))
    .filter(Boolean)
    .sort();

  return streams;
}

async function allocateMasterTargets({ exchange, stream }) {
  for (;;) {
    const ymd = todayUTCYYYYMMDD();
    const hms = nowUTCHHMMSS();
    const masterPackName = `edge-campaign-multi-v0-${exchange}_${stream}_${ymd}_${hms}`;
    const masterRoot = path.join("evidence", masterPackName);
    const masterTar = path.join("evidence", `${masterPackName}.tar.gz`);
    const masterSha = `${masterTar}.sha256`;
    const masterMoved = path.join("evidence", `${masterPackName}.moved_to.txt`);

    if (!(await fileExists(masterRoot)) && !(await fileExists(masterTar)) && !(await fileExists(masterSha)) && !(await fileExists(masterMoved))) {
      return { masterPackName, masterRoot, masterTar, masterSha, masterMoved };
    }

    await sleep(1000);
  }
}

async function runStreamCampaign({ exchange, stream, dayQuality, symbols }) {
  const { masterPackName, masterRoot, masterTar, masterSha, masterMoved } = await allocateMasterTargets({ exchange, stream });

  await Promise.all([
    mkdirp(path.join(masterRoot, "packs")),
    mkdirp(path.join(masterRoot, "runs"))
  ]);

  const rows = [];

  for (const symbol of symbols) {
    const symbolNorm = normalizeSymbol(symbol);
    const runDir = path.join(masterRoot, "runs", symbolNorm);
    await mkdirp(runDir);

    const before = new Set(await listSymbolTarballs({ exchange, stream, symbolNorm }));

    const cmdArgs = [
      "node",
      "tools/run-edge-campaign.js",
      "--exchange", exchange,
      "--symbol", symbol,
      "--stream", stream,
      "--dayQuality", dayQuality,
      "--maxCandidates", "3",
      "--progressEvery", "1",
      "--smokeTimeoutS", "300",
      "--smokeMaxRowsPerDay", "200000",
      "--smokeSlice", "head_tail",
      "--smokeParallel", "2",
      "--acceptanceTimeoutS", "3600",
      "--acceptanceHeapMB", "6144",
      "--acceptanceParallel", "1",
      "--forceAcceptanceTop1", "true",
      "--runDeterminism", "false"
    ];

    const runInfo = await runTimed({ cwd: process.cwd(), runDir, commandArgs: cmdArgs });
    const runMeta = await parseRunMeta(runInfo);

    const after = await listSymbolTarballs({ exchange, stream, symbolNorm });
    const newTarCandidates = after.filter((x) => !before.has(x));

    let tarFile = null;
    if (newTarCandidates.length === 1) {
      tarFile = newTarCandidates[0];
    } else if (newTarCandidates.length > 1) {
      const stats = await Promise.all(newTarCandidates.map(async (f) => {
        const st = await fs.stat(path.join("evidence", f));
        return { f, m: st.mtimeMs };
      }));
      stats.sort((a, b) => b.m - a.m);
      tarFile = stats[0].f;
    }

    let packName = "";
    let campaignDecision = "FAIL";
    let bestWindow = "";
    let bestPatterns = "";
    let bestEdgesSaved = "";
    let bestWall = "";
    let bestRss = "";
    let notes = "";
    let tarRel = "";
    let shaRel = "";
    let movedRel = "";

    if (!tarFile) {
      notes = `no symbol pack generated (runner_exit=${runInfo.exitCode})`;
    } else {
      packName = tarFile.replace(/\.tar\.gz$/, "");
      const shaFile = `${tarFile}.sha256`;
      const movedFile = `${packName}.moved_to.txt`;

      const tarSrc = path.join("evidence", tarFile);
      const shaSrc = path.join("evidence", shaFile);
      const movedSrc = path.join("evidence", movedFile);

      const tarDst = path.join(masterRoot, "packs", tarFile);
      const shaDst = path.join(masterRoot, "packs", shaFile);
      const movedDst = path.join(masterRoot, "packs", movedFile);

      if (await fileExists(tarSrc)) {
        await fs.copyFile(tarSrc, tarDst);
        tarRel = toRel(masterRoot, tarDst);
      }
      if (await fileExists(shaSrc)) {
        await fs.copyFile(shaSrc, shaDst);
        shaRel = toRel(masterRoot, shaDst);
      }
      if (await fileExists(movedSrc)) {
        await fs.copyFile(movedSrc, movedDst);
        movedRel = toRel(masterRoot, movedDst);
      }

      if (await fileExists(movedSrc)) {
        const archiveDir = (await fs.readFile(movedSrc, "utf8")).trim();
        const summaryPath = path.join(archiveDir, "summary.json");
        if (await fileExists(summaryPath)) {
          const summary = JSON.parse(await fs.readFile(summaryPath, "utf8"));
          campaignDecision = summary.campaign_pass ? "PASS" : "FAIL";
          notes = summary.risk_note || summary.reason || "";
          const best = selectBestWindow(summary.rows || []);
          if (best) {
            bestWindow = best.window || "";
            bestPatterns = best.acceptance?.patterns_scanned ?? "";
            bestEdgesSaved = best.acceptance?.edges_saved ?? false;
            bestWall = best.acceptance?.wall_s ?? "";
            bestRss = best.acceptance?.max_rss_kb ?? "";
          }
        } else {
          notes = `summary missing in archive (${archiveDir})`;
        }
      } else {
        notes = `moved_to missing for ${packName}`;
      }
    }

    rows.push({
      symbol,
      symbolNorm,
      packName,
      campaignDecision,
      bestWindow,
      bestPatterns,
      bestEdgesSaved,
      bestWall,
      bestRss,
      notes,
      tarRel,
      shaRel,
      movedRel,
      runnerExit: runInfo.exitCode,
      runnerWallS: runMeta.wallS,
      runnerMaxRssKb: runMeta.maxRssKb,
      runStdoutRel: toRel(masterRoot, runInfo.stdoutPath),
      runStderrRel: toRel(masterRoot, runInfo.stderrPath),
      runTimeVRel: toRel(masterRoot, runInfo.timeVPath),
      runExitRel: toRel(masterRoot, runInfo.exitPath)
    });

    console.log(`[Multi] stream=${stream} symbol=${symbol} campaign=${campaignDecision} pack=${packName || "N/A"} runner_exit=${runInfo.exitCode}`);
  }

  const header = [
    "symbol",
    "pack_name",
    "campaign_decision",
    "best_window",
    "best_acceptance_patterns_scanned",
    "best_edges_saved",
    "best_acceptance_wall_s",
    "best_acceptance_max_rss_kb",
    "notes",
    "pack_tar_relpath",
    "pack_sha_relpath",
    "pack_moved_relpath",
    "runner_exit",
    "runner_wall_s",
    "runner_max_rss_kb",
    "run_stdout_relpath",
    "run_stderr_relpath",
    "run_time_v_relpath",
    "run_exit_relpath"
  ];

  const lines = [toTsvLine(header)];
  for (const r of rows) {
    lines.push(toTsvLine([
      r.symbol,
      r.packName,
      r.campaignDecision,
      r.bestWindow,
      r.bestPatterns,
      r.bestEdgesSaved,
      r.bestWall,
      r.bestRss,
      r.notes,
      r.tarRel,
      r.shaRel,
      r.movedRel,
      r.runnerExit,
      r.runnerWallS,
      r.runnerMaxRssKb,
      r.runStdoutRel,
      r.runStderrRel,
      r.runTimeVRel,
      r.runExitRel
    ]));
  }
  await writeText(path.join(masterRoot, "master_results.tsv"), `${lines.join("\n")}\n`);

  const passRows = rows.filter((r) => r.campaignDecision === "PASS");
  const masterPass = passRows.length > 0;

  let bestPassSymbol = null;
  if (passRows.length > 0) {
    const sortedPass = [...passRows].sort((a, b) => {
      const pa = Number.isFinite(Number(a.bestPatterns)) ? Number(a.bestPatterns) : -1;
      const pb = Number.isFinite(Number(b.bestPatterns)) ? Number(b.bestPatterns) : -1;
      return pb - pa;
    });
    bestPassSymbol = sortedPass[0];
  }

  const masterSummary = {
    pack: masterPackName,
    campaign_pass: masterPass,
    exchange,
    stream,
    symbols,
    pass_symbols: passRows.map((r) => r.symbol),
    best_pass_symbol: bestPassSymbol
      ? {
        symbol: bestPassSymbol.symbol,
        pack_name: bestPassSymbol.packName,
        best_window: bestPassSymbol.bestWindow,
        best_acceptance_patterns_scanned: bestPassSymbol.bestPatterns,
        best_edges_saved: bestPassSymbol.bestEdgesSaved,
        pack_tar_relpath: bestPassSymbol.tarRel,
        pack_sha_relpath: bestPassSymbol.shaRel,
        pack_moved_relpath: bestPassSymbol.movedRel
      }
      : null,
    rows,
    files: {
      master_results_relpath: "master_results.tsv",
      master_summary_relpath: "master_summary.json"
    }
  };
  await writeText(path.join(masterRoot, "master_summary.json"), `${JSON.stringify(masterSummary, null, 2)}\n`);

  const readme = [
    "# Edge Campaign Multi v0",
    "",
    `Exchange: ${exchange}`,
    `Stream: ${stream}`,
    `Campaign Result: ${masterPass ? "PASS" : "FAIL"}`,
    `Pass symbols: ${passRows.length > 0 ? passRows.map((r) => r.symbol).join(", ") : "none"}`,
    `Best symbol: ${bestPassSymbol ? bestPassSymbol.symbol : "N/A"}`,
    "",
    "## Evidence Relpaths",
    "- master_results.tsv",
    "- master_summary.json"
  ];

  for (const r of rows) {
    if (r.tarRel) readme.push(`- ${r.tarRel}`);
    if (r.shaRel) readme.push(`- ${r.shaRel}`);
    if (r.movedRel) readme.push(`- ${r.movedRel}`);
  }

  await writeText(path.join(masterRoot, "README.md"), `${readme.join("\n")}\n`);

  const missing = await integrityCheck(masterRoot);
  if (missing !== 0) throw new Error(`master integrity failed: missing=${missing}`);

  await finalizeMasterPack({
    packName: masterPackName,
    packRoot: masterRoot,
    tarPath: masterTar,
    shaPath: masterSha,
    movedPath: masterMoved
  });

  console.log(`[Multi] stream=${stream} result=${masterPass ? "PASS" : "FAIL"}`);

  return {
    stream,
    masterPass,
    masterPackName,
    masterTar,
    masterSha,
    masterMoved
  };
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    printHelp(0);
  }

  const exchange = args.exchange ? String(args.exchange).trim() : DEFAULT_EXCHANGE;
  const dayQuality = args.dayQuality ? String(args.dayQuality).trim().toUpperCase() : DEFAULT_DAY_QUALITY;
  if (!VALID_DAY_QUALITY.has(dayQuality)) {
    fatal(`Unsupported --dayQuality: ${dayQuality} (v0 supports GOOD only)`);
  }

  let streams;
  if (args.streams !== undefined) {
    streams = parseCsvList(args.streams);
    if (streams.length === 0) {
      fatal("--streams provided but empty after parsing");
    }
  } else {
    streams = await discoverStreams(exchange);
    if (streams.length === 0) {
      fatal(`No streams discovered under data/curated/exchange=${exchange}`);
    }
  }

  console.log(`[Multi] exchange=${exchange} dayQuality=${dayQuality}`);
  console.log(`[Multi] discovered_streams=${streams.join(",")}`);

  const outcomes = [];
  for (const stream of streams) {
    try {
      const result = await runStreamCampaign({
        exchange,
        stream,
        dayQuality,
        symbols: DEFAULT_SYMBOLS
      });
      outcomes.push({ stream, ok: true, ...result });
    } catch (err) {
      const msg = err && err.stack ? err.stack : String(err);
      console.error(`[Multi] stream=${stream} fatal=${msg}`);
      outcomes.push({ stream, ok: false, error: msg });
    }
  }

  const passStreams = outcomes.filter((x) => x.ok && x.masterPass).map((x) => x.stream);
  const anyPass = passStreams.length > 0;

  console.log(`[Multi] pass_streams=${passStreams.length > 0 ? passStreams.join(",") : "none"}`);
  for (const o of outcomes) {
    if (o.ok) {
      console.log(`[Multi] stream=${o.stream} pack=${o.masterPackName} decision=${o.masterPass ? "PASS" : "FAIL"}`);
    } else {
      console.log(`[Multi] stream=${o.stream} pack=N/A decision=FAIL(error)`);
    }
  }

  process.exit(anyPass ? 0 : 1);
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
