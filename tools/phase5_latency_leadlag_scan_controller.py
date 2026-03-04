#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import os
import re
import shlex
import statistics
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


REPO = Path("/home/deploy/quantlab-backend")
MAX_WALL = 7200.0
EXCHANGES = ["binance", "okx", "bybit"]
STREAMS = ["bbo", "trade", "mark_price"]
TOL_MS = 20
DELTA_LIST = "0,10,25,50,100"
TOP_K = 15
N_TARGET = 6
CORE_SYMBOLS = [
    "adausdt",
    "avaxusdt",
    "bnbusdt",
    "btcusdt",
    "ethusdt",
    "linkusdt",
    "ltcusdt",
    "maticusdt",
    "solusdt",
    "xrpusdt",
]


@dataclass
class PackCtx:
    name: str
    dir: Path
    cmd_index: Path
    time_summary: Path


def normalize_symbol(sym: str) -> str:
    return str(sym).replace("/", "").replace("-", "").strip().lower()


def metric_contract(stream: str) -> Tuple[str, str]:
    stream = stream.strip().lower()
    if stream == "bbo":
        return "log_return", "mid"
    if stream == "trade":
        return "log_return", "last"
    if stream == "mark_price":
        return "log_return", "mark"
    if stream == "funding":
        return "diff_bps", "funding_rate"
    raise ValueError(f"unsupported stream: {stream}")


def parse_elapsed_seconds(time_v_text: str) -> float:
    m = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*([^\n]+)", time_v_text)
    if not m:
        return 0.0
    raw = m.group(1).strip()
    parts = [float(x) for x in raw.split(":")]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 1:
        return parts[0]
    return 0.0


def parse_max_rss_kb(time_v_text: str) -> int:
    m = re.search(r"Maximum resident set size \(kbytes\):\s*([0-9]+)", time_v_text)
    if not m:
        return 0
    return int(m.group(1))


class StepRunner:
    def __init__(self, repo: Path, max_wall_s: float) -> None:
        self.repo = repo
        self.max_wall_s = max_wall_s
        self.cumulative_wall_s = 0.0
        self.timebox_hit = False

    def init_pack(self, name: str) -> PackCtx:
        pack_dir = self.repo / "evidence" / name
        (pack_dir / "analysis").mkdir(parents=True, exist_ok=True)
        (pack_dir / "selection_proof").mkdir(parents=True, exist_ok=True)
        (pack_dir / "attempts").mkdir(parents=True, exist_ok=True)
        (pack_dir / "controller_commands").mkdir(parents=True, exist_ok=True)
        (pack_dir / "finalize").mkdir(parents=True, exist_ok=True)

        cmd_index = pack_dir / "command_index.tsv"
        time_summary = pack_dir / "time_v_summary.tsv"
        cmd_index.write_text(
            "step\texit_code\tmax_rss_kb\telapsed_s\tcumulative_wall_s\t"
            "cmd_relpath\tstdout_relpath\tstderr_relpath\ttime_v_relpath\texit_relpath\n",
            encoding="utf-8",
        )
        time_summary.write_text(
            "step\texit_code\telapsed_s\tmax_rss_kb\tcumulative_wall_s\n",
            encoding="utf-8",
        )
        return PackCtx(name=name, dir=pack_dir, cmd_index=cmd_index, time_summary=time_summary)

    def _moved_to_file(self, pack_name: str) -> Path:
        return self.repo / "evidence" / f"{pack_name}.moved_to.txt"

    def _refresh_ctx_if_moved(self, ctx: PackCtx) -> None:
        moved_file = self._moved_to_file(ctx.name)
        if not moved_file.exists():
            return
        moved = Path(moved_file.read_text(encoding="utf-8").strip())
        if not moved.exists():
            return
        if ctx.dir != moved:
            ctx.dir = moved
            ctx.cmd_index = moved / "command_index.tsv"
            ctx.time_summary = moved / "time_v_summary.tsv"

    def run_step(self, ctx: PackCtx, step: str, cmd: str) -> None:
        self._refresh_ctx_if_moved(ctx)
        step_dir = ctx.dir / "controller_commands" / step
        step_dir.mkdir(parents=True, exist_ok=True)

        cmdf = step_dir / "cmd.sh"
        outf = step_dir / "stdout.log"
        errf = step_dir / "stderr.log"
        timef = step_dir / "time-v.log"
        exitf = step_dir / "exit_code.txt"

        wrapped = (
            "set -euo pipefail\n"
            f"export PACK_DIR={shlex.quote(str(ctx.dir))}\n"
            f"export PACK_NAME={shlex.quote(ctx.name)}\n"
            f"{cmd}\n"
        )
        cmdf.write_text(wrapped, encoding="utf-8")
        cmdf.chmod(0o755)

        with outf.open("w", encoding="utf-8") as fo, errf.open("w", encoding="utf-8") as fe:
            proc = subprocess.run(
                ["/usr/bin/time", "-v", "-o", str(timef), "--", "bash", str(cmdf)],
                cwd=str(self.repo),
                stdout=fo,
                stderr=fe,
                check=False,
            )
        ec = proc.returncode

        # After slim finalize, pack dir moves to archive. Remap paths if needed.
        if not timef.exists():
            self._refresh_ctx_if_moved(ctx)
            step_dir = ctx.dir / "controller_commands" / step
            cmdf = step_dir / "cmd.sh"
            outf = step_dir / "stdout.log"
            errf = step_dir / "stderr.log"
            timef = step_dir / "time-v.log"
            exitf = step_dir / "exit_code.txt"

        exitf.write_text(f"{ec}\n", encoding="utf-8")
        time_txt = timef.read_text(encoding="utf-8", errors="replace") if timef.exists() else ""
        elapsed = parse_elapsed_seconds(time_txt)
        rss = parse_max_rss_kb(time_txt)

        self.cumulative_wall_s += elapsed
        if self.cumulative_wall_s >= self.max_wall_s:
            self.timebox_hit = True

        def rel(p: Path) -> str:
            try:
                return str(p.relative_to(ctx.dir))
            except Exception:
                return str(p)

        with ctx.cmd_index.open("a", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(
                [
                    step,
                    ec,
                    rss,
                    f"{elapsed:.6f}",
                    f"{self.cumulative_wall_s:.6f}",
                    rel(cmdf),
                    rel(outf),
                    rel(errf),
                    rel(timef),
                    rel(exitf),
                ]
            )
        with ctx.time_summary.open("a", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow([step, ec, f"{elapsed:.6f}", rss, f"{self.cumulative_wall_s:.6f}"])

        if ec != 0:
            raise RuntimeError(f"step_failed:{step}:exit={ec}")


def read_state(state_path: Path) -> dict:
    state = json.loads(state_path.read_text(encoding="utf-8"))
    parts = state.get("partitions")
    if not isinstance(parts, dict):
        raise RuntimeError("state_missing_partitions")
    return state


def build_meta(state: dict, streams: Iterable[str], exchanges: List[str], symbols: List[str]) -> Dict[Tuple[str, str, str, str], dict]:
    out: Dict[Tuple[str, str, str, str], dict] = {}
    stream_set = set(streams) | {"funding"}
    symbol_set = set(symbols)
    for key, val in state["partitions"].items():
        sp = key.split("/")
        if len(sp) != 4:
            continue
        ex, stream, sym, date = sp
        if ex not in exchanges:
            continue
        if stream not in stream_set:
            continue
        if sym not in symbol_set:
            continue
        if str(val.get("status", "")).lower() != "success":
            continue
        dqp = val.get("day_quality_post")
        if dqp not in {"GOOD", "DEGRADED"}:
            continue
        out[(ex, stream, sym, date)] = {
            "rows": int(val.get("rows") or 0),
            "quality": dqp,
        }
    return out


def build_symbol_windows(meta: Dict[Tuple[str, str, str, str], dict], exchanges: List[str], streams: List[str], symbols: List[str]) -> Dict[str, Dict[str, List[dict]]]:
    out: Dict[str, Dict[str, List[dict]]] = {st: {sym: [] for sym in symbols} for st in streams}
    for st in streams:
        for sym in symbols:
            date_sets = []
            for ex in exchanges:
                ds = {d for (e, s, y, d) in meta if e == ex and s == st and y == sym}
                date_sets.append(ds)
            common = sorted(set.intersection(*date_sets)) if date_sets else []
            for i in range(len(common) - 1):
                d1, d2 = common[i], common[i + 1]
                t1 = dt.datetime.strptime(d1, "%Y%m%d")
                t2 = dt.datetime.strptime(d2, "%Y%m%d")
                if (t2 - t1).days != 1:
                    continue
                q1 = "GOOD"
                q2 = "GOOD"
                rows_total = 0
                for ex in exchanges:
                    m1 = meta[(ex, st, sym, d1)]
                    m2 = meta[(ex, st, sym, d2)]
                    rows_total += int(m1["rows"]) + int(m2["rows"])
                    if m1["quality"] != "GOOD":
                        q1 = "DEGRADED"
                    if m2["quality"] != "GOOD":
                        q2 = "DEGRADED"
                out[st][sym].append(
                    {
                        "start": d1,
                        "end": d2,
                        "window_id": f"{d1}..{d2}",
                        "day1_quality": q1,
                        "day2_quality": q2,
                        "rows_total": rows_total,
                    }
                )
    return out


def write_tsv(path: Path, header: List[str], rows: Iterable[Iterable[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        for r in rows:
            w.writerow(list(r))


def stage1_prepare_selection(pack_dir: Path, state: dict) -> List[dict]:
    meta = build_meta(state=state, streams=STREAMS, exchanges=EXCHANGES, symbols=CORE_SYMBOLS)
    sym_windows = build_symbol_windows(meta=meta, exchanges=EXCHANGES, streams=STREAMS, symbols=CORE_SYMBOLS)

    sel_dir = pack_dir / "selection_proof"
    stream_selection_rows: List[List[object]] = []
    selected_window: Dict[str, Tuple[str, str, str]] = {}

    for st in STREAMS:
        w2syms: Dict[str, dict] = {}
        for sym in CORE_SYMBOLS:
            for w in sym_windows[st][sym]:
                rec = w2syms.setdefault(
                    w["window_id"],
                    {"start": w["start"], "end": w["end"], "symbols": set()},
                )
                rec["symbols"].add(sym)
        if not w2syms:
            raise RuntimeError(f"no_3ex_window_for_stream:{st}")
        ranked = sorted(
            [(wid, rec["start"], rec["end"], len(rec["symbols"]), sorted(rec["symbols"])) for wid, rec in w2syms.items()],
            key=lambda x: (-x[3], x[0]),
        )
        wid, start, end, cnt, syms = ranked[0]
        selected_window[st] = (start, end, wid)
        stream_selection_rows.append([st, "SELECTED", "MAX_SYMBOL_COVERAGE", wid, cnt, "|".join(syms)])

    stream_selection_rows.append(["funding", "SKIPPED", "EXCLUDED_BY_SCOPE_FIX", "N/A", 0, ""])
    stream_selection_rows.sort(key=lambda x: str(x[0]))
    write_tsv(
        sel_dir / "stream_window_selection.tsv",
        ["stream", "status", "reason", "window_id", "covered_symbol_count", "covered_symbols"],
        stream_selection_rows,
    )

    ranked_symbols: Dict[str, List[Tuple[str, int, int]]] = {}
    eligible_rows: List[List[object]] = []
    for st in STREAMS:
        _, _, wid = selected_window[st]
        rows = []
        for sym in CORE_SYMBOLS:
            wins = sym_windows[st][sym]
            if not wins:
                continue
            coverage_count = len(wins)
            rep_rows = None
            for w in wins:
                if w["window_id"] == wid:
                    rep_rows = int(w["rows_total"])
                    break
            if rep_rows is None:
                continue
            rows.append((sym, coverage_count, rep_rows))
        rows.sort(key=lambda x: (-x[1], x[2], x[0]))
        if len(rows) < 3:
            raise RuntimeError(f"insufficient_symbols_for_stream:{st}")
        ranked_symbols[st] = rows
        for rank, (sym, cov, rr) in enumerate(rows, start=1):
            eligible_rows.append([st, wid, sym, cov, rr, rank])

    write_tsv(
        sel_dir / "eligible_symbols_by_stream_window.tsv",
        ["stream", "window_id", "symbol", "coverage_windows_count", "rows_total", "symbol_rank"],
        sorted(eligible_rows, key=lambda x: (str(x[0]), int(x[5]), str(x[2]))),
    )

    jobs: List[dict] = []
    for st in STREAMS:
        start, end, wid = selected_window[st]
        mk, vd = metric_contract(st)
        sym1 = ranked_symbols[st][0][0]
        sym2 = ranked_symbols[st][1][0]
        jobs.append(
            {
                "phase": "base",
                "stream": st,
                "symbol": sym1,
                "start": start,
                "end": end,
                "window_id": wid,
                "h_list": "250,500,1000",
                "cell_count": 90,
                "metric_kind": mk,
                "value_def": vd,
            }
        )
        jobs.append(
            {
                "phase": "base",
                "stream": st,
                "symbol": sym2,
                "start": start,
                "end": end,
                "window_id": wid,
                "h_list": "500,1000",
                "cell_count": 60,
                "metric_kind": mk,
                "value_def": vd,
            }
        )

    fill_candidates = []
    for st in sorted(STREAMS):
        for rank, (sym, _cov, _rows) in enumerate(ranked_symbols[st], start=1):
            if rank < 3:
                continue
            fill_candidates.append((st, rank, sym))
    fill_candidates.sort(key=lambda x: (x[0], x[1], x[2]))
    if len(fill_candidates) < 5:
        raise RuntimeError("insufficient_fill_candidates_for_exact_600")

    for st, _rank, sym in fill_candidates[:5]:
        start, end, wid = selected_window[st]
        mk, vd = metric_contract(st)
        jobs.append(
            {
                "phase": "fill_tier3",
                "stream": st,
                "symbol": sym,
                "start": start,
                "end": end,
                "window_id": wid,
                "h_list": "1000",
                "cell_count": 30,
                "metric_kind": mk,
                "value_def": vd,
            }
        )

    if sum(int(j["cell_count"]) for j in jobs) != 600:
        raise RuntimeError("stage1_cell_count_mismatch")

    for i, j in enumerate(jobs, start=1):
        j["job_id"] = f"job_{i:03d}"

    write_tsv(
        pack_dir / "stage1_schedule.tsv",
        [
            "job_id",
            "phase",
            "stream",
            "symbol",
            "start",
            "end",
            "window_id",
            "delta_list",
            "h_list",
            "tolerance_ms",
            "pair_mode",
            "metric_kind",
            "value_def",
            "cell_count",
        ],
        [
            [
                j["job_id"],
                j["phase"],
                j["stream"],
                j["symbol"],
                j["start"],
                j["end"],
                j["window_id"],
                DELTA_LIST,
                j["h_list"],
                TOL_MS,
                "all6",
                j["metric_kind"],
                j["value_def"],
                j["cell_count"],
            ]
            for j in jobs
        ],
    )

    obj_rows: List[List[object]] = []
    for j in jobs:
        obj_file = pack_dir / "attempts" / j["job_id"] / "object_keys_window.tsv"
        rows = []
        for day, label in [(j["start"], "day1"), (j["end"], "day2")]:
            for ex in EXCHANGES:
                pkey = f"{ex}/{j['stream']}/{j['symbol']}/{day}"
                if (ex, j["stream"], j["symbol"], day) not in meta:
                    raise RuntimeError(f"missing_meta_for_job:{j['job_id']}:{pkey}")
                data_key = f"exchange={ex}/stream={j['stream']}/symbol={j['symbol']}/date={day}/data.parquet"
                meta_key = f"exchange={ex}/stream={j['stream']}/symbol={j['symbol']}/date={day}/meta.json"
                rows.append([label, ex, day, pkey, data_key, meta_key, "quantlab-compact"])
                obj_rows.append([j["job_id"], j["stream"], j["symbol"], j["window_id"], label, ex, day, pkey, data_key, meta_key, "quantlab-compact"])
        write_tsv(
            obj_file,
            ["label", "exchange", "date", "partition_key", "data_key", "meta_key", "bucket"],
            rows,
        )

    write_tsv(
        sel_dir / "object_keys_selected.tsv",
        [
            "job_id",
            "stream",
            "symbol",
            "window_id",
            "label",
            "exchange",
            "date",
            "partition_key",
            "data_key",
            "meta_key",
            "bucket",
        ],
        obj_rows,
    )
    return jobs


def stage1_aggregate(pack_dir: Path, top_k: int) -> str:
    schedule = []
    with (pack_dir / "stage1_schedule.tsv").open("r", encoding="utf-8", newline="") as f:
        schedule = list(csv.DictReader(f, delimiter="\t"))

    rows = []
    for job in schedule:
        job_id = job["job_id"]
        p = pack_dir / "attempts" / job_id / "run_primary" / "results_rollup.tsv"
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                mean_bps = float(r["mean_forward_return_bps"])
                t_stat = float(r["t_stat"])
                event_count = int(r["event_count"])
                score = abs(t_stat) * max(mean_bps, 0.0)
                cell_id = (
                    f"{job['stream']}|{job['symbol']}|{r['pair']}|dt={int(r['delta_t_ms'])}|H={int(r['h_ms'])}|"
                    f"tol={int(job['tolerance_ms'])}|{r['metric_kind']}|{r['value_def']}"
                )
                rows.append(
                    {
                        "job_id": job_id,
                        "stream": job["stream"],
                        "symbol": job["symbol"],
                        "window_id": job["window_id"],
                        "pair": r["pair"],
                        "delta_t_ms": int(r["delta_t_ms"]),
                        "h_ms": int(r["h_ms"]),
                        "event_count": event_count,
                        "mean_forward_return_bps": mean_bps,
                        "t_stat": t_stat,
                        "metric_kind": r["metric_kind"],
                        "value_def": r["value_def"],
                        "score": score,
                        "cell_id": cell_id,
                    }
                )

    rows.sort(key=lambda x: (x["stream"], x["symbol"], x["pair"], x["delta_t_ms"], x["h_ms"]))
    write_tsv(
        pack_dir / "scan_results_stage1.tsv",
        [
            "job_id",
            "stream",
            "symbol",
            "window_id",
            "pair",
            "delta_t_ms",
            "h_ms",
            "event_count",
            "mean_forward_return_bps",
            "t_stat",
            "metric_kind",
            "value_def",
            "score",
            "cell_id",
        ],
        [
            [
                r["job_id"],
                r["stream"],
                r["symbol"],
                r["window_id"],
                r["pair"],
                r["delta_t_ms"],
                r["h_ms"],
                r["event_count"],
                f"{r['mean_forward_return_bps']:.15f}",
                f"{r['t_stat']:.15f}",
                r["metric_kind"],
                r["value_def"],
                f"{r['score']:.15f}",
                r["cell_id"],
            ]
            for r in rows
        ],
    )

    cand = [r for r in rows if r["event_count"] >= 200 and abs(r["t_stat"]) >= 3.0]
    cand.sort(
        key=lambda x: (
            -x["score"],
            -abs(x["t_stat"]),
            -x["event_count"],
            -abs(x["mean_forward_return_bps"]),
            x["stream"],
            x["symbol"],
            x["pair"],
            x["delta_t_ms"],
            x["h_ms"],
        )
    )
    top = cand[:top_k]
    write_tsv(
        pack_dir / "stage1_topk.tsv",
        [
            "rank",
            "cell_id",
            "stream",
            "symbol",
            "pair",
            "dt_ms",
            "h_ms",
            "tolerance_ms",
            "event_count",
            "mean_bps",
            "t_stat",
            "score",
            "metric_kind",
            "value_def",
            "window_id",
        ],
        [
            [
                i,
                r["cell_id"],
                r["stream"],
                r["symbol"],
                r["pair"],
                r["delta_t_ms"],
                r["h_ms"],
                20,
                r["event_count"],
                f"{r['mean_forward_return_bps']:.15f}",
                f"{r['t_stat']:.15f}",
                f"{r['score']:.15f}",
                r["metric_kind"],
                r["value_def"],
                r["window_id"],
            ]
            for i, r in enumerate(top, start=1)
        ],
    )

    result = f"PASS/STAGE1_TOPK_READY:{len(top)}" if top else "FAIL/STAGE1_NO_CANDIDATE"
    (pack_dir / "result.txt").write_text(result + "\n", encoding="utf-8")
    (pack_dir / "analysis" / "stage1_summary.json").write_text(
        json.dumps(
            {
                "stage": "stage1",
                "rows_total": len(rows),
                "candidate_count": len(cand),
                "top_k": len(top),
                "result": result,
            },
            indent=2,
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return result


def write_stage1_label_manifest(pack_dir: Path) -> None:
    result = (pack_dir / "result.txt").read_text(encoding="utf-8").strip()
    code_ref = (pack_dir / "analysis" / "code_version_ref.txt").read_text(encoding="utf-8").strip()
    hash_inputs = hashlib.sha256(
        ((pack_dir / "stage1_schedule.tsv").read_text(encoding="utf-8") + (pack_dir / "selection_proof" / "stream_window_selection.tsv").read_text(encoding="utf-8")).encode()
    ).hexdigest()
    hash_outputs = hashlib.sha256(
        (
            (pack_dir / "scan_results_stage1.tsv").read_text(encoding="utf-8")
            + (pack_dir / "stage1_topk.tsv").read_text(encoding="utf-8")
            + (pack_dir / "result.txt").read_text(encoding="utf-8")
        ).encode()
    ).hexdigest()
    lines = [
        f"label={result.split(':')[0]}",
        "artifact_manifest=stage1_schedule.tsv,scan_results_stage1.tsv,stage1_topk.tsv,label_report",
        f"decision_inputs=stage1_result:{result}",
        f"hash_inputs={hash_inputs}",
        f"hash_outputs={hash_outputs}",
        f"run_id={pack_dir.name}",
        f"code_version_ref={code_ref}",
        'metric_contract_note="cross-stream results not comparable unless metric_kind/value_def match"',
        "scope_guard=No new fields beyond listed",
    ]
    (pack_dir / "label_report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    write_tsv(
        pack_dir / "artifact_manifest.tsv",
        ["expected_relpath", "resolved_relpath", "status"],
        [
            ["selection_proof/stream_window_selection.tsv", "selection_proof/stream_window_selection.tsv", "OK"],
            ["selection_proof/eligible_symbols_by_stream_window.tsv", "selection_proof/eligible_symbols_by_stream_window.tsv", "OK"],
            ["selection_proof/object_keys_selected.tsv", "selection_proof/object_keys_selected.tsv", "OK"],
            ["stage1_schedule.tsv", "stage1_schedule.tsv", "OK"],
            ["scan_results_stage1.tsv", "scan_results_stage1.tsv", "OK"],
            ["stage1_topk.tsv", "stage1_topk.tsv", "OK"],
            ["label_report.txt", "label_report.txt", "OK"],
            ["result.txt", "result.txt", "OK"],
            ["command_index.tsv", "command_index.tsv", "OK"],
            ["time_v_summary.tsv", "time_v_summary.tsv", "OK"],
        ],
    )


def write_integrity(pack_dir: Path, required: List[str]) -> None:
    missing = [x for x in required if not (pack_dir / x).exists()]
    (pack_dir / "integrity_check.txt").write_text(
        "missing_count=" + str(len(missing)) + "\n" + "\n".join(missing) + "\n",
        encoding="utf-8",
    )
    if missing:
        raise RuntimeError(f"missing_artifacts:{','.join(missing)}")


def stage2_plan(pack_dir: Path, state: dict, topk_path: Path) -> List[dict]:
    cells = []
    with topk_path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            cells.append(
                {
                    "cell_id": r["cell_id"],
                    "stream": r["stream"],
                    "symbol": normalize_symbol(r["symbol"]),
                    "pair": r["pair"],
                    "dt_ms": int(r["dt_ms"]),
                    "h_ms": int(r["h_ms"]),
                    "tolerance_ms": int(r["tolerance_ms"]),
                    "metric_kind": r["metric_kind"],
                    "value_def": r["value_def"],
                }
            )

    meta = build_meta(state=state, streams=STREAMS, exchanges=EXCHANGES, symbols=CORE_SYMBOLS)
    sel_dir = pack_dir / "selection_proof"
    sel_dir.mkdir(parents=True, exist_ok=True)

    cell_windows: List[dict] = []
    q_order = {"GG": 0, "GD": 1, "DD": 2}
    for c in cells:
        st = c["stream"]
        sym = c["symbol"]
        date_sets = []
        for ex in EXCHANGES:
            date_sets.append({d for (e, s, y, d) in meta if e == ex and s == st and y == sym})
        common = sorted(set.intersection(*date_sets)) if date_sets else []
        wins = []
        for i in range(len(common) - 1):
            d1, d2 = common[i], common[i + 1]
            t1 = dt.datetime.strptime(d1, "%Y%m%d")
            t2 = dt.datetime.strptime(d2, "%Y%m%d")
            if (t2 - t1).days != 1:
                continue
            q1 = "GOOD"
            q2 = "GOOD"
            for ex in EXCHANGES:
                if meta[(ex, st, sym, d1)]["quality"] != "GOOD":
                    q1 = "DEGRADED"
                if meta[(ex, st, sym, d2)]["quality"] != "GOOD":
                    q2 = "DEGRADED"
            if q1 == "GOOD" and q2 == "GOOD":
                qtag = "GG"
            elif q1 == "DEGRADED" and q2 == "DEGRADED":
                qtag = "DD"
            else:
                qtag = "GD"
            wins.append({"start": d1, "end": d2, "window_id": f"{d1}..{d2}", "day1_quality": q1, "day2_quality": q2, "quality_tag": qtag})
        wins.sort(key=lambda w: (q_order[w["quality_tag"]], w["window_id"]))
        for rank, w in enumerate(wins[:N_TARGET], start=1):
            rec = dict(c)
            rec.update(
                {
                    "window_rank": rank,
                    "window_id": w["window_id"],
                    "start": w["start"],
                    "end": w["end"],
                    "day1_quality": w["day1_quality"],
                    "day2_quality": w["day2_quality"],
                    "quality_tag": w["quality_tag"],
                }
            )
            cell_windows.append(rec)

    write_tsv(
        pack_dir / "stage2_cell_windows.tsv",
        [
            "cell_id",
            "stream",
            "symbol",
            "pair",
            "dt_ms",
            "h_ms",
            "tolerance_ms",
            "metric_kind",
            "value_def",
            "window_id",
            "start",
            "end",
            "day1_quality",
            "day2_quality",
            "quality_tag",
            "window_rank",
        ],
        [
            [
                r["cell_id"],
                r["stream"],
                r["symbol"],
                r["pair"],
                r["dt_ms"],
                r["h_ms"],
                r["tolerance_ms"],
                r["metric_kind"],
                r["value_def"],
                r["window_id"],
                r["start"],
                r["end"],
                r["day1_quality"],
                r["day2_quality"],
                r["quality_tag"],
                r["window_rank"],
            ]
            for r in sorted(cell_windows, key=lambda x: (x["cell_id"], x["window_rank"], x["window_id"]))
        ],
    )

    jobs_map: Dict[Tuple[str, str, str, str, str], List[dict]] = {}
    for r in cell_windows:
        key = (r["stream"], r["symbol"], r["start"], r["end"], r["window_id"])
        jobs_map.setdefault(key, []).append(r)

    jobs: List[dict] = []
    obj_rows: List[List[object]] = []
    for idx, key in enumerate(sorted(jobs_map.keys(), key=lambda x: (x[0], x[1], x[4])), start=1):
        st, sym, start, end, window_id = key
        job_id = f"job_{idx:03d}"
        rows = sorted(jobs_map[key], key=lambda x: x["cell_id"])
        attempt_dir = pack_dir / "attempts" / job_id
        attempt_dir.mkdir(parents=True, exist_ok=True)
        write_tsv(
            attempt_dir / "cells_file_window.tsv",
            ["stream", "symbol", "pair", "dt_ms", "h_ms", "tolerance_ms", "metric_kind", "value_def", "cell_id"],
            [[r["stream"], r["symbol"], r["pair"], r["dt_ms"], r["h_ms"], r["tolerance_ms"], r["metric_kind"], r["value_def"], r["cell_id"]] for r in rows],
        )

        key_rows = []
        for day, label in [(start, "day1"), (end, "day2")]:
            for ex in EXCHANGES:
                pkey = f"{ex}/{st}/{sym}/{day}"
                if (ex, st, sym, day) not in meta:
                    raise RuntimeError(f"missing_meta:{job_id}:{pkey}")
                data_key = f"exchange={ex}/stream={st}/symbol={sym}/date={day}/data.parquet"
                meta_key = f"exchange={ex}/stream={st}/symbol={sym}/date={day}/meta.json"
                row = [label, ex, day, pkey, data_key, meta_key, "quantlab-compact"]
                key_rows.append(row)
                obj_rows.append([job_id, st, sym, window_id, *row])
        write_tsv(
            attempt_dir / "object_keys_window.tsv",
            ["label", "exchange", "date", "partition_key", "data_key", "meta_key", "bucket"],
            key_rows,
        )
        jobs.append(
            {
                "job_id": job_id,
                "stream": st,
                "symbol": sym,
                "start": start,
                "end": end,
                "window_id": window_id,
                "cell_count": len(rows),
                "object_key_count": len(key_rows),
            }
        )

    write_tsv(
        sel_dir / "object_keys_selected.tsv",
        [
            "job_id",
            "stream",
            "symbol",
            "window_id",
            "label",
            "exchange",
            "date",
            "partition_key",
            "data_key",
            "meta_key",
            "bucket",
        ],
        obj_rows,
    )
    write_tsv(
        pack_dir / "stage2_jobs.tsv",
        ["job_id", "stream", "symbol", "start", "end", "window_id", "cell_count", "object_key_count"],
        [[j["job_id"], j["stream"], j["symbol"], j["start"], j["end"], j["window_id"], j["cell_count"], j["object_key_count"]] for j in jobs],
    )
    return jobs


def stage2_aggregate(pack_dir: Path, n_target: int, timebox_hit: bool) -> str:
    cell_windows = list(csv.DictReader((pack_dir / "stage2_cell_windows.tsv").open("r", encoding="utf-8", newline=""), delimiter="\t"))
    jobs = list(csv.DictReader((pack_dir / "stage2_jobs.tsv").open("r", encoding="utf-8", newline=""), delimiter="\t"))
    job_by_window = {(r["stream"], r["symbol"], r["window_id"]): r for r in jobs}
    job_obj_count = {r["job_id"]: int(r.get("object_key_count") or 0) for r in jobs}

    compare_basis = "pair,delta_t_ms,h_ms,event_count,mean_forward_return_bps,t_stat,metric_kind,value_def"
    job_det: Dict[str, str] = {}
    job_primary_map: Dict[str, Dict[Tuple[str, int, int, str, str], dict]] = {}
    job_rows_loaded: Dict[str, Dict[str, int]] = {}
    det_rows = []

    for j in jobs:
        job_id = j["job_id"]
        p_primary = pack_dir / "attempts" / job_id / "run_primary" / "results_rollup.tsv"
        p_replay = pack_dir / "attempts" / job_id / "run_replay_on" / "results_rollup.tsv"
        s_primary = pack_dir / "attempts" / job_id / "run_primary" / "summary.json"

        ph = ""
        rh = ""
        status = "FAIL"
        primary_rows = []
        replay_rows = []
        if p_primary.exists() and p_replay.exists():
            primary_rows = list(csv.DictReader(p_primary.open("r", encoding="utf-8", newline=""), delimiter="\t"))
            replay_rows = list(csv.DictReader(p_replay.open("r", encoding="utf-8", newline=""), delimiter="\t"))

            def canon(rows: List[dict]) -> List[dict]:
                out = []
                for r in sorted(rows, key=lambda x: (x["pair"], int(x["delta_t_ms"]), int(x["h_ms"]))):
                    out.append(
                        {
                            "pair": r["pair"],
                            "delta_t_ms": int(r["delta_t_ms"]),
                            "h_ms": int(r["h_ms"]),
                            "event_count": int(r["event_count"]),
                            "mean_forward_return_bps": f"{float(r['mean_forward_return_bps']):.15f}",
                            "t_stat": f"{float(r['t_stat']):.15f}",
                            "metric_kind": r["metric_kind"],
                            "value_def": r["value_def"],
                        }
                    )
                return out

            c1 = canon(primary_rows)
            c2 = canon(replay_rows)
            ph = hashlib.sha256(json.dumps(c1, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
            rh = hashlib.sha256(json.dumps(c2, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
            status = "PASS" if c1 == c2 else "FAIL"

            pm = {}
            for r in primary_rows:
                pm[(r["pair"], int(r["delta_t_ms"]), int(r["h_ms"]), r["metric_kind"], r["value_def"])] = r
            job_primary_map[job_id] = pm

        if s_primary.exists():
            sj = json.loads(s_primary.read_text(encoding="utf-8"))
            job_rows_loaded[job_id] = {k: int(v) for k, v in (sj.get("inputs", {}).get("rows_loaded_by_exchange", {}) or {}).items()}
        else:
            job_rows_loaded[job_id] = {}

        job_det[job_id] = status
        det_rows.append([job_id, j["stream"], j["symbol"], j["window_id"], ph, rh, status, compare_basis])

    write_tsv(
        pack_dir / "determinism_compare.tsv",
        ["job_id", "stream", "symbol", "window_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
        det_rows,
    )

    window_rows = []
    for cw in sorted(cell_windows, key=lambda x: (x["cell_id"], int(x["window_rank"]), x["window_id"])):
        job = job_by_window.get((cw["stream"], cw["symbol"], cw["window_id"]))
        if not job:
            continue
        job_id = job["job_id"]
        det = job_det.get(job_id, "FAIL")
        obj_count = job_obj_count.get(job_id, 0)
        rows_loaded = job_rows_loaded.get(job_id, {})
        rows_guard = all(int(rows_loaded.get(ex, 0)) >= 200 for ex in EXCHANGES)
        coverage_pass = obj_count == 6 and rows_guard

        event_count = 0
        mean_bps = ""
        t_stat = ""
        label = ""
        if det != "PASS":
            label = "FAIL/DETERMINISM_FAIL"
        else:
            key = (cw["pair"], int(cw["dt_ms"]), int(cw["h_ms"]), cw["metric_kind"], cw["value_def"])
            row = job_primary_map.get(job_id, {}).get(key)
            if row is None:
                label = "FAIL/DETERMINISM_FAIL"
            else:
                event_count = int(row["event_count"])
                mean = float(row["mean_forward_return_bps"])
                t = float(row["t_stat"])
                mean_bps = f"{mean:.15f}"
                t_stat = f"{t:.15f}"
                if event_count < 200 or not coverage_pass:
                    label = "INSUFFICIENT_SUPPORT"
                elif abs(t) >= 3.0 and mean > 0:
                    label = "DIRECTIONAL"
                elif abs(t) >= 3.0 and mean < 0:
                    label = "ANTI_EDGE"
                else:
                    label = "NO_EDGE"
        window_rows.append(
            [
                cw["cell_id"],
                cw["stream"],
                cw["symbol"],
                cw["pair"],
                int(cw["dt_ms"]),
                int(cw["h_ms"]),
                int(cw["tolerance_ms"]),
                cw["metric_kind"],
                cw["value_def"],
                cw["window_id"],
                event_count,
                mean_bps,
                t_stat,
                det,
                label,
                cw["day1_quality"],
                cw["day2_quality"],
            ]
        )

    write_tsv(
        pack_dir / "stage2_results_windows.tsv",
        [
            "cell_id",
            "stream",
            "symbol",
            "pair",
            "dt_ms",
            "h_ms",
            "tolerance_ms",
            "metric_kind",
            "value_def",
            "window_id",
            "event_count",
            "mean_bps",
            "t_stat",
            "determinism_status",
            "window_label",
            "day1_quality",
            "day2_quality",
        ],
        window_rows,
    )

    by_cell: Dict[str, List[list]] = {}
    for r in window_rows:
        by_cell.setdefault(str(r[0]), []).append(r)

    def p10(values: List[float]) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        idx = int((len(s) - 1) * 0.10)
        return s[idx]

    summary_rows = []
    for cell_id, rows in sorted(by_cell.items(), key=lambda x: x[0]):
        sample = rows[0]
        means = [float(r[11]) for r in rows if str(r[11]) != ""]
        n_actual = len(rows)
        directional = sum(1 for r in rows if r[14] == "DIRECTIONAL")
        anti = sum(1 for r in rows if r[14] == "ANTI_EDGE")
        noedge = sum(1 for r in rows if r[14] == "NO_EDGE")
        insuff = sum(1 for r in rows if r[14] == "INSUFFICIENT_SUPPORT")
        detfail = sum(1 for r in rows if r[14] == "FAIL/DETERMINISM_FAIL")

        median_mean = statistics.median(means) if means else 0.0
        p10_mean = p10(means)
        min_mean = min(means) if means else 0.0
        directional_rate = (directional / n_actual) if n_actual > 0 else 0.0

        all_det_pass = detfail == 0
        all_support_pass = insuff == 0 and all(int(r[10]) >= 200 for r in rows if str(r[10]) != "")
        promising = (
            n_actual == n_target
            and all_det_pass
            and all_support_pass
            and median_mean >= 2.0
            and p10_mean >= 1.5
            and directional_rate >= 0.8
        )

        summary_rows.append(
            [
                cell_id,
                sample[1],
                sample[2],
                sample[3],
                sample[4],
                sample[5],
                sample[6],
                sample[7],
                sample[8],
                n_actual,
                f"{median_mean:.15f}",
                f"{p10_mean:.15f}",
                f"{min_mean:.15f}",
                f"{directional_rate:.6f}",
                detfail,
                directional,
                anti,
                noedge,
                insuff,
                detfail,
                "PROMISING" if promising else "THIN_EDGE",
            ]
        )

    write_tsv(
        pack_dir / "stage2_cell_summary.tsv",
        [
            "cell_id",
            "stream",
            "symbol",
            "pair",
            "dt_ms",
            "h_ms",
            "tolerance_ms",
            "metric_kind",
            "value_def",
            "N_actual",
            "median_mean_bps",
            "p10_mean_bps",
            "min_mean_bps",
            "directional_rate",
            "detfail_count",
            "count_DIRECTIONAL",
            "count_ANTI_EDGE",
            "count_NO_EDGE",
            "count_INSUFFICIENT_SUPPORT",
            "count_FAIL_DETERMINISM",
            "cell_class",
        ],
        summary_rows,
    )

    promising_rows = [r for r in summary_rows if r[-1] == "PROMISING"]

    def best_key(r: list) -> tuple:
        return (-float(r[10]), -float(r[11]), -float(r[13]), int(r[14]), str(r[0]))

    if promising_rows:
        promising_rows.sort(key=best_key)
        best = promising_rows[0]
        result = f"PASS/PROMISING:{best[0]}"
    else:
        if summary_rows:
            summary_rows.sort(key=best_key)
            best = summary_rows[0]
            result = f"FAIL/THIN_EDGE:best={best[0]}"
        else:
            best = ["NONE", "", "", "", 0, 0, 20, "", "", 0, "0", "0", "0", "0", 0, 0, 0, 0, 0, 0, "THIN_EDGE"]
            result = "FAIL/THIN_EDGE:best=NONE"

    if timebox_hit and not result.startswith("PASS/"):
        result = result + ";detail=timebox_partial"

    (pack_dir / "result.txt").write_text(result + "\n", encoding="utf-8")
    write_tsv(
        pack_dir / "final_top_cell.txt",
        [
            "cell_id",
            "stream",
            "symbol",
            "pair",
            "dt_ms",
            "h_ms",
            "metric_kind",
            "value_def",
            "median_mean_bps",
            "p10_mean_bps",
            "directional_rate",
            "class",
        ],
        [[best[0], best[1], best[2], best[3], best[4], best[5], best[7], best[8], best[10], best[11], best[13], best[-1]]],
    )

    code_ref = (pack_dir / "analysis" / "code_version_ref.txt").read_text(encoding="utf-8").strip()
    hash_inputs = hashlib.sha256(((pack_dir / "stage2_cell_windows.tsv").read_text(encoding="utf-8") + (pack_dir / "stage2_jobs.tsv").read_text(encoding="utf-8")).encode()).hexdigest()
    hash_outputs = hashlib.sha256(
        (
            (pack_dir / "stage2_results_windows.tsv").read_text(encoding="utf-8")
            + (pack_dir / "stage2_cell_summary.tsv").read_text(encoding="utf-8")
            + (pack_dir / "result.txt").read_text(encoding="utf-8")
        ).encode()
    ).hexdigest()
    lines = [
        f"label={result.split(':')[0]}",
        "artifact_manifest=stage2_results_windows.tsv,stage2_cell_summary.tsv,determinism_compare.tsv,final_top_cell.txt,label_report",
        f"decision_inputs=N_cells:{len(summary_rows)},PROMISING_count:{len(promising_rows)}",
        f"hash_inputs={hash_inputs}",
        f"hash_outputs={hash_outputs}",
        f"run_id={pack_dir.name}",
        f"code_version_ref={code_ref}",
        'metric_contract_note="cross-stream results not comparable unless metric_kind/value_def match"',
        "scope_guard=No new fields beyond listed",
    ]
    (pack_dir / "label_report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return result


def write_stage2_manifest(pack_dir: Path) -> None:
    write_tsv(
        pack_dir / "artifact_manifest.tsv",
        ["expected_relpath", "resolved_relpath", "status"],
        [
            ["selection_proof/object_keys_selected.tsv", "selection_proof/object_keys_selected.tsv", "OK"],
            ["stage2_cell_windows.tsv", "stage2_cell_windows.tsv", "OK"],
            ["stage2_jobs.tsv", "stage2_jobs.tsv", "OK"],
            ["stage2_results_windows.tsv", "stage2_results_windows.tsv", "OK"],
            ["stage2_cell_summary.tsv", "stage2_cell_summary.tsv", "OK"],
            ["determinism_compare.tsv", "determinism_compare.tsv", "OK"],
            ["final_top_cell.txt", "final_top_cell.txt", "OK"],
            ["label_report.txt", "label_report.txt", "OK"],
            ["result.txt", "result.txt", "OK"],
            ["command_index.tsv", "command_index.tsv", "OK"],
            ["time_v_summary.tsv", "time_v_summary.tsv", "OK"],
        ],
    )


def q(v: object) -> str:
    return shlex.quote(str(v))


def main() -> int:
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archive_root = Path(f"/home/deploy/quantlab-evidence-archive/{dt.datetime.utcnow().strftime('%Y%m%d')}_slim")
    state_json = Path(f"/tmp/quantlab_compacted_state_phase5_latency_scan_{ts}.json")
    stage1_pack = f"phase5-latency-scan-stage1-{ts}"
    stage2_pack = f"phase5-latency-scan-stage2-{ts}"

    runner = StepRunner(REPO, MAX_WALL)

    # Stage-1
    stage1 = runner.init_pack(stage1_pack)
    runner.run_step(stage1, "pre_note", 'cat > "$PACK_DIR/analysis/pre_note.txt" <<NOTE\nPhase-5 bounded scan stage-1.\nGoal: exact 600-cell bounded scan over bbo/trade/mark_price and TOP_K=15.\nDone: stage1 SLIM pack finalized with auditable metric contract.\nNOTE')
    runner.run_step(stage1, "precheck", "test -x tools/slim_finalize.sh && test -x /tmp/s3_compact_tool.py && test -f tools/hypotheses/latency_leadlag_v1.py")
    runner.run_step(stage1, "code_version", 'git rev-parse HEAD > "$PACK_DIR/analysis/code_version_ref.txt"')
    runner.run_step(stage1, "state_fetch", f"python3 /tmp/s3_compact_tool.py get quantlab-compact compacted/_state.json {q(state_json)}")
    state = read_state(state_json)
    stage1_prepare_selection(stage1.dir, state)

    schedule_rows = list(csv.DictReader((stage1.dir / "stage1_schedule.tsv").open("r", encoding="utf-8", newline=""), delimiter="\t"))
    for job in schedule_rows:
        if runner.timebox_hit:
            break
        cmd = (
            "python3 tools/hypotheses/latency_leadlag_v1.py "
            f"--object-keys-tsv {q(stage1.dir / 'attempts' / job['job_id'] / 'object_keys_window.tsv')} "
            f"--downloads-dir {q(stage1.dir / 'attempts' / job['job_id'] / 'downloads_primary')} "
            f"--exchange-order {q(','.join(EXCHANGES))} "
            f"--symbol {q(job['symbol'])} --stream {q(job['stream'])} "
            f"--start {q(job['start'])} --end {q(job['end'])} "
            f"--tolerance-ms {q(TOL_MS)} --pair-mode all6 "
            f"--delta-ms-list {q(DELTA_LIST)} --h-ms-list {q(job['h_list'])} "
            f"--results-out {q(stage1.dir / 'attempts' / job['job_id'] / 'run_primary' / 'results_rollup.tsv')} "
            f"--pair-support-out {q(stage1.dir / 'attempts' / job['job_id'] / 'run_primary' / 'pair_support.tsv')} "
            f"--summary-out {q(stage1.dir / 'attempts' / job['job_id'] / 'run_primary' / 'summary.json')}"
        )
        runner.run_step(stage1, f"stage1_primary_{job['job_id']}", cmd)

    stage1_result = stage1_aggregate(stage1.dir, TOP_K)
    write_stage1_label_manifest(stage1.dir)
    write_integrity(
        stage1.dir,
        [
            "selection_proof/stream_window_selection.tsv",
            "selection_proof/eligible_symbols_by_stream_window.tsv",
            "selection_proof/object_keys_selected.tsv",
            "stage1_schedule.tsv",
            "scan_results_stage1.tsv",
            "stage1_topk.tsv",
            "label_report.txt",
            "artifact_manifest.tsv",
            "result.txt",
            "command_index.tsv",
            "time_v_summary.tsv",
        ],
    )

    runner.run_step(stage1, "stage1_finalize", f"bash tools/slim_finalize.sh {q(stage1.name)} {q(stage1.dir)} {q(archive_root)}")
    runner.run_step(
        stage1,
        "stage1_sha_verify",
        f"sha256sum -c {q(REPO / 'evidence' / f'{stage1.name}.tar.gz.sha256')} > {q(REPO / 'evidence' / f'{stage1.name}.sha_verify_tmp.txt')}; "
        f"MOVED_TO=$(cat {q(REPO / 'evidence' / f'{stage1.name}.moved_to.txt')}); "
        f"cp {q(REPO / 'evidence' / f'{stage1.name}.sha_verify_tmp.txt')} \"$MOVED_TO/sha_verify.txt\"; "
        f"rm -f {q(REPO / 'evidence' / f'{stage1.name}.sha_verify_tmp.txt')}",
    )
    runner.run_step(
        stage1,
        "stage1_post_guard",
        f"test ! -d {q(REPO / 'evidence' / stage1.name)} && "
        f"test -f {q(REPO / 'evidence' / f'{stage1.name}.tar.gz')} && "
        f"test -f {q(REPO / 'evidence' / f'{stage1.name}.tar.gz.sha256')} && "
        f"test -f {q(REPO / 'evidence' / f'{stage1.name}.moved_to.txt')} && "
        f"MOVED_TO=$(cat {q(REPO / 'evidence' / f'{stage1.name}.moved_to.txt')}) && "
        f"test -d \"$MOVED_TO\" && grep -q OK \"$MOVED_TO/sha_verify.txt\"",
    )

    stage1_moved_to = Path((REPO / "evidence" / f"{stage1.name}.moved_to.txt").read_text(encoding="utf-8").strip())
    stage1_topk_rows = list(csv.DictReader((stage1_moved_to / "stage1_topk.tsv").open("r", encoding="utf-8", newline=""), delimiter="\t"))
    final_result = stage1_result
    stage2_moved_to = Path("")

    if stage1_topk_rows:
        # Stage-2
        stage2 = runner.init_pack(stage2_pack)
        runner.run_step(stage2, "pre_note", 'cat > "$PACK_DIR/analysis/pre_note.txt" <<NOTE\nPhase-5 bounded scan stage-2.\nGoal: targeted eval only for stage1 TOP_K, strict N=6, ONvsON determinism.\nDone: stage2 SLIM pack finalized with PASS/PROMISING or FAIL/THIN_EDGE.\nNOTE')
        runner.run_step(stage2, "precheck", f"test -f {q(stage1_moved_to / 'stage1_topk.tsv')} && test -x tools/slim_finalize.sh && test -f tools/hypotheses/latency_leadlag_v1.py")
        runner.run_step(stage2, "code_version", 'git rev-parse HEAD > "$PACK_DIR/analysis/code_version_ref.txt"')
        jobs = stage2_plan(stage2.dir, state, stage1_moved_to / "stage1_topk.tsv")

        for j in jobs:
            if runner.timebox_hit:
                break
            primary_cmd = (
                "python3 tools/hypotheses/latency_leadlag_v1.py "
                f"--object-keys-tsv {q(stage2.dir / 'attempts' / j['job_id'] / 'object_keys_window.tsv')} "
                f"--downloads-dir {q(stage2.dir / 'attempts' / j['job_id'] / 'downloads_primary')} "
                f"--exchange-order {q(','.join(EXCHANGES))} "
                f"--symbol {q(j['symbol'])} --stream {q(j['stream'])} "
                f"--start {q(j['start'])} --end {q(j['end'])} "
                f"--tolerance-ms {q(TOL_MS)} --pair-mode all6 "
                f"--cells_file {q(stage2.dir / 'attempts' / j['job_id'] / 'cells_file_window.tsv')} "
                f"--delta-ms-list 0 --h-ms-list 250 "
                f"--results-out {q(stage2.dir / 'attempts' / j['job_id'] / 'run_primary' / 'results_rollup.tsv')} "
                f"--pair-support-out {q(stage2.dir / 'attempts' / j['job_id'] / 'run_primary' / 'pair_support.tsv')} "
                f"--summary-out {q(stage2.dir / 'attempts' / j['job_id'] / 'run_primary' / 'summary.json')}"
            )
            runner.run_step(stage2, f"stage2_primary_{j['job_id']}", primary_cmd)
            if runner.timebox_hit:
                break
            replay_cmd = (
                "python3 tools/hypotheses/latency_leadlag_v1.py "
                f"--object-keys-tsv {q(stage2.dir / 'attempts' / j['job_id'] / 'object_keys_window.tsv')} "
                f"--downloads-dir {q(stage2.dir / 'attempts' / j['job_id'] / 'downloads_replay')} "
                f"--exchange-order {q(','.join(EXCHANGES))} "
                f"--symbol {q(j['symbol'])} --stream {q(j['stream'])} "
                f"--start {q(j['start'])} --end {q(j['end'])} "
                f"--tolerance-ms {q(TOL_MS)} --pair-mode all6 "
                f"--cells_file {q(stage2.dir / 'attempts' / j['job_id'] / 'cells_file_window.tsv')} "
                f"--delta-ms-list 0 --h-ms-list 250 "
                f"--results-out {q(stage2.dir / 'attempts' / j['job_id'] / 'run_replay_on' / 'results_rollup.tsv')} "
                f"--pair-support-out {q(stage2.dir / 'attempts' / j['job_id'] / 'run_replay_on' / 'pair_support.tsv')} "
                f"--summary-out {q(stage2.dir / 'attempts' / j['job_id'] / 'run_replay_on' / 'summary.json')}"
            )
            runner.run_step(stage2, f"stage2_replay_{j['job_id']}", replay_cmd)

        final_result = stage2_aggregate(stage2.dir, N_TARGET, runner.timebox_hit)
        write_stage2_manifest(stage2.dir)
        write_integrity(
            stage2.dir,
            [
                "selection_proof/object_keys_selected.tsv",
                "stage2_cell_windows.tsv",
                "stage2_jobs.tsv",
                "stage2_results_windows.tsv",
                "stage2_cell_summary.tsv",
                "determinism_compare.tsv",
                "final_top_cell.txt",
                "label_report.txt",
                "artifact_manifest.tsv",
                "result.txt",
                "command_index.tsv",
                "time_v_summary.tsv",
            ],
        )
        runner.run_step(stage2, "stage2_finalize", f"bash tools/slim_finalize.sh {q(stage2.name)} {q(stage2.dir)} {q(archive_root)}")
        runner.run_step(
            stage2,
            "stage2_sha_verify",
            f"sha256sum -c {q(REPO / 'evidence' / f'{stage2.name}.tar.gz.sha256')} > {q(REPO / 'evidence' / f'{stage2.name}.sha_verify_tmp.txt')}; "
            f"MOVED_TO=$(cat {q(REPO / 'evidence' / f'{stage2.name}.moved_to.txt')}); "
            f"cp {q(REPO / 'evidence' / f'{stage2.name}.sha_verify_tmp.txt')} \"$MOVED_TO/sha_verify.txt\"; "
            f"rm -f {q(REPO / 'evidence' / f'{stage2.name}.sha_verify_tmp.txt')}",
        )
        runner.run_step(
            stage2,
            "stage2_post_guard",
            f"test ! -d {q(REPO / 'evidence' / stage2.name)} && "
            f"test -f {q(REPO / 'evidence' / f'{stage2.name}.tar.gz')} && "
            f"test -f {q(REPO / 'evidence' / f'{stage2.name}.tar.gz.sha256')} && "
            f"test -f {q(REPO / 'evidence' / f'{stage2.name}.moved_to.txt')} && "
            f"MOVED_TO=$(cat {q(REPO / 'evidence' / f'{stage2.name}.moved_to.txt')}) && "
            f"test -d \"$MOVED_TO\" && grep -q OK \"$MOVED_TO/sha_verify.txt\"",
        )
        stage2_moved_to = Path((REPO / "evidence" / f"{stage2.name}.moved_to.txt").read_text(encoding="utf-8").strip())

    print(f"STAGE1_PACK={stage1_pack}")
    print(f"STAGE1_MOVED_TO={stage1_moved_to}")
    print(f"STAGE2_PACK={stage2_pack}")
    print(f"STAGE2_MOVED_TO={stage2_moved_to}")
    print(f"FINAL_RESULT={final_result}")
    print(f"GLOBAL_CUM_WALL={runner.cumulative_wall_s:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
