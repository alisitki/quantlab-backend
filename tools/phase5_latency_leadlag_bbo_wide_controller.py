#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import hashlib
import json
import math
import os
import re
import shlex
import statistics
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


REPO = Path("/home/deploy/quantlab-backend")
MAX_WALL = 5400.0
RESERVED_OVERHEAD = 900.0
EXCHANGES = ["binance", "okx", "bybit"]
STREAM = "bbo"
TOL_MS = 20
DELTA_LIST = "0,10,25,50,100"
H_LIST = "250,500,1000"
TOP_K = 15
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
DETERMINISM_BASIS = "pair,delta_t_ms,h_ms,event_count,mean_forward_return_bps,t_stat"
PROGRESS_HEADER = [
    "selection_rank",
    "symbol",
    "window_id",
    "status",
    "exit_code",
    "elapsed_s",
    "max_rss_kb",
    "notes",
]
TERMINAL_SKIP_STATUSES = {"DONE", "FAILED_OOM", "FAILED_OTHER", "SKIPPED"}


@dataclass
class PackCtx:
    name: str
    dir: Path
    cmd_index: Path
    time_summary: Path


@dataclass
class StepCapture:
    exit_code: int
    elapsed_s: float
    max_rss_kb: int
    stdout_path: Path
    stderr_path: Path
    time_path: Path
    exit_path: Path


def normalize_symbol(sym: str) -> str:
    return str(sym).replace("/", "").replace("-", "").strip().lower()


def parse_elapsed_seconds(time_v_text: str) -> float:
    m = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*([^\n]+)", time_v_text)
    if not m:
        return 0.0
    raw = m.group(1).strip()
    vals = [float(x) for x in raw.split(":")]
    if len(vals) == 3:
        return vals[0] * 3600 + vals[1] * 60 + vals[2]
    if len(vals) == 2:
        return vals[0] * 60 + vals[1]
    return vals[0]


def parse_max_rss_kb(time_v_text: str) -> int:
    m = re.search(r"Maximum resident set size \(kbytes\):\s*([0-9]+)", time_v_text)
    return int(m.group(1)) if m else 0


def write_tsv(path: Path, header: List[str], rows: Iterable[Iterable[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        for r in rows:
            w.writerow(list(r))


def q(v: object) -> str:
    return shlex.quote(str(v))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 latency_leadlag_v1 bbo-wide stage1 controller")
    p.add_argument("--resume-from-pack", default="", help="Old pack name or absolute pack path")
    p.add_argument("--max-windows", type=int, default=0, help="Optional cap for smoke tests")
    p.add_argument("--progress-interval-sec", type=int, default=120)
    return p.parse_args()


def write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def ensure_progress_file(progress_path: Path) -> None:
    if progress_path.exists():
        return
    write_tsv(progress_path, PROGRESS_HEADER, [])


def append_progress_row(
    progress_path: Path,
    selection_rank: int,
    symbol: str,
    window_id: str,
    status: str,
    exit_code: int,
    elapsed_s: float,
    max_rss_kb: int,
    notes: str,
) -> None:
    ensure_progress_file(progress_path)
    with progress_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(
            [
                selection_rank,
                symbol,
                window_id,
                status,
                exit_code,
                f"{elapsed_s:.6f}",
                max_rss_kb,
                notes,
            ]
        )


def read_progress_latest(progress_path: Path) -> Dict[Tuple[int, str, str], dict]:
    out: Dict[Tuple[int, str, str], dict] = {}
    if not progress_path.exists():
        return out
    with progress_path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (int(r["selection_rank"]), r["symbol"], r["window_id"])
            out[key] = r
    return out


def classify_failure(exit_code: int, stderr_text: str) -> str:
    if exit_code in {137, 9}:
        return "FAILED_OOM"
    if exit_code != 0 and "Killed" in stderr_text:
        return "FAILED_OOM"
    if exit_code != 0:
        return "FAILED_OTHER"
    return "DONE"


def resolve_pack_dir(resume_from_pack: str) -> Path:
    raw = str(resume_from_pack).strip()
    if not raw:
        raise RuntimeError("empty_resume_from_pack")
    p = Path(raw)
    if p.is_dir():
        return p
    moved = REPO / "evidence" / f"{raw}.moved_to.txt"
    if moved.exists():
        target = Path(moved.read_text(encoding="utf-8").strip())
        if target.is_dir():
            return target
    candidates = sorted(glob.glob(f"/home/deploy/quantlab-evidence-archive/*_slim/{raw}"))
    if candidates:
        c = Path(candidates[-1])
        if c.is_dir():
            return c
    raise RuntimeError(f"resume_pack_not_found:{raw}")


def next_resume_index(resume_pack_name: str) -> int:
    m = re.search(r"__resume([0-9]+)$", resume_pack_name)
    if not m:
        return 1
    return int(m.group(1)) + 1


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

    def run_step_capture(self, ctx: PackCtx, step: str, cmd: str) -> StepCapture:
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

        return StepCapture(
            exit_code=ec,
            elapsed_s=elapsed,
            max_rss_kb=rss,
            stdout_path=outf,
            stderr_path=errf,
            time_path=timef,
            exit_path=exitf,
        )

    def run_step(self, ctx: PackCtx, step: str, cmd: str) -> None:
        res = self.run_step_capture(ctx=ctx, step=step, cmd=cmd)
        if res.exit_code != 0:
            raise RuntimeError(f"step_failed:{step}:exit={res.exit_code}")


def read_state(state_path: Path) -> dict:
    state = json.loads(state_path.read_text(encoding="utf-8"))
    parts = state.get("partitions")
    if not isinstance(parts, dict):
        raise RuntimeError("state_missing_partitions")
    return state


def build_meta(state: dict, exchanges: List[str], symbols: List[str]) -> Dict[Tuple[str, str, str], dict]:
    out: Dict[Tuple[str, str, str], dict] = {}
    symbol_set = set(symbols)
    for key, val in state["partitions"].items():
        sp = key.split("/")
        if len(sp) != 4:
            continue
        ex, stream, sym, date = sp
        if ex not in exchanges:
            continue
        if stream != STREAM:
            continue
        if sym not in symbol_set:
            continue
        if str(val.get("status", "")).lower() != "success":
            continue
        dqp = val.get("day_quality_post")
        if dqp not in {"GOOD", "DEGRADED"}:
            continue
        out[(ex, sym, date)] = {
            "rows": int(val.get("rows") or 0),
            "quality": dqp,
        }
    return out


def quality_tag(day1_quality: str, day2_quality: str) -> str:
    if day1_quality == "GOOD" and day2_quality == "GOOD":
        return "GG"
    if day1_quality == "DEGRADED" and day2_quality == "DEGRADED":
        return "DD"
    return "GD"


def build_symbol_windows(meta: Dict[Tuple[str, str, str], dict], symbols: List[str], exchanges: List[str]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {sym: [] for sym in symbols}
    for sym in symbols:
        date_sets = []
        for ex in exchanges:
            date_sets.append({d for (e, s, d) in meta if e == ex and s == sym})
        common = sorted(set.intersection(*date_sets)) if date_sets else []
        for i in range(len(common) - 1):
            d1, d2 = common[i], common[i + 1]
            t1 = dt.datetime.strptime(d1, "%Y%m%d")
            t2 = dt.datetime.strptime(d2, "%Y%m%d")
            if (t2 - t1).days != 1:
                continue
            day1_q = "GOOD"
            day2_q = "GOOD"
            rows_day1 = 0
            rows_day2 = 0
            for ex in exchanges:
                m1 = meta[(ex, sym, d1)]
                m2 = meta[(ex, sym, d2)]
                rows_day1 += int(m1["rows"])
                rows_day2 += int(m2["rows"])
                if m1["quality"] != "GOOD":
                    day1_q = "DEGRADED"
                if m2["quality"] != "GOOD":
                    day2_q = "DEGRADED"
            out[sym].append(
                {
                    "symbol": sym,
                    "start": d1,
                    "end": d2,
                    "window_id": f"{d1}..{d2}",
                    "day1_quality": day1_q,
                    "day2_quality": day2_q,
                    "quality_tag": quality_tag(day1_q, day2_q),
                    "rows_total_day1": rows_day1,
                    "rows_total_day2": rows_day2,
                    "rows_total_2day": rows_day1 + rows_day2,
                }
            )
    return out


def estimate_per_window_s() -> Tuple[float, str]:
    candidates = sorted(
        glob.glob("/home/deploy/quantlab-evidence-archive/*_slim/phase5-latency-scan-stage1-*/stage1_schedule.tsv"),
        key=lambda p: os.path.getmtime(p),
    )
    if candidates:
        sched = Path(candidates[-1])
        pack_dir = sched.parent
        time_path = pack_dir / "time_v_summary.tsv"
        if time_path.exists():
            job_meta: Dict[str, Tuple[str, int]] = {}
            with sched.open("r", encoding="utf-8", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t"):
                    job_meta[r["job_id"]] = (r["stream"], int(r["cell_count"]))
            vals: List[float] = []
            with time_path.open("r", encoding="utf-8", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t"):
                    st = r["step"]
                    m = re.match(r"stage1_primary_(job_\d+)$", st)
                    if not m:
                        continue
                    jid = m.group(1)
                    if jid not in job_meta:
                        continue
                    stream, cc = job_meta[jid]
                    if stream != "bbo":
                        continue
                    elapsed = float(r["elapsed_s"])
                    if cc <= 0:
                        continue
                    norm = elapsed * (90.0 / float(cc))
                    vals.append(norm)
            if vals:
                vals_sorted = sorted(vals)
                idx = int((len(vals_sorted) - 1) * 0.9)
                est = vals_sorted[idx]
                est = max(60.0, min(est, 300.0))
                return est, f"stage1_history_p90_norm90:{sched}"

    return 180.0, "fallback_default_180s"


def select_windows_round_robin(symbol_windows: Dict[str, List[dict]], n_target: int) -> Tuple[List[dict], List[Tuple[str, int, int]]]:
    quality_order = {"GG": 0, "GD": 1, "DD": 2}
    rank_rows: List[Tuple[str, int, int]] = []
    for sym, wins in symbol_windows.items():
        if not wins:
            continue
        rank_rows.append((sym, len(wins), min(int(w["rows_total_2day"]) for w in wins)))
    rank_rows.sort(key=lambda x: (-x[1], x[2], x[0]))

    ordered: Dict[str, List[dict]] = {}
    for sym, _cnt, _minrows in rank_rows:
        ws = list(symbol_windows[sym])
        ws.sort(key=lambda w: (quality_order[w["quality_tag"]], w["window_id"]))
        ordered[sym] = ws

    idx_map = {sym: 0 for sym in ordered}
    selected: List[dict] = []
    symbol_order = [sym for sym, _cnt, _minrows in rank_rows]

    while len(selected) < n_target:
        made_progress = False
        for sym in symbol_order:
            i = idx_map[sym]
            if i >= len(ordered[sym]):
                continue
            selected.append(dict(ordered[sym][i]))
            idx_map[sym] = i + 1
            made_progress = True
            if len(selected) >= n_target:
                break
        if not made_progress:
            break

    for i, rec in enumerate(selected, start=1):
        rec["selection_rank"] = i
    return selected, rank_rows


def write_selection_proofs(pack_dir: Path, selected: List[dict], rank_rows: List[Tuple[str, int, int]], meta: Dict[Tuple[str, str, str], dict]) -> List[dict]:
    sel_dir = pack_dir / "selection_proof"
    sel_dir.mkdir(parents=True, exist_ok=True)

    write_tsv(
        sel_dir / "symbol_rankings.tsv",
        ["symbol", "coverage_windows_count", "rows_total_2day_min"],
        [[sym, cnt, minrows] for sym, cnt, minrows in rank_rows],
    )

    write_tsv(
        pack_dir / "selected_windows.tsv",
        [
            "selection_rank",
            "symbol",
            "window_id",
            "start",
            "end",
            "day1_quality",
            "day2_quality",
            "quality_tag",
            "rows_total_day1",
            "rows_total_day2",
            "rows_total_2day",
        ],
        [
            [
                r["selection_rank"],
                r["symbol"],
                r["window_id"],
                r["start"],
                r["end"],
                r["day1_quality"],
                r["day2_quality"],
                r["quality_tag"],
                r["rows_total_day1"],
                r["rows_total_day2"],
                r["rows_total_2day"],
            ]
            for r in selected
        ],
    )

    agg_rows: List[List[object]] = []
    jobs: List[dict] = []
    for rec in selected:
        rank = int(rec["selection_rank"])
        sym = rec["symbol"]
        start = rec["start"]
        end = rec["end"]
        win = rec["window_id"]
        job_id = f"job_{rank:03d}"
        attempt_dir = pack_dir / "attempts" / job_id
        attempt_dir.mkdir(parents=True, exist_ok=True)

        rows = []
        for day, label in [(start, "day1"), (end, "day2")]:
            for ex in EXCHANGES:
                key = (ex, sym, day)
                if key not in meta:
                    raise RuntimeError(f"missing_meta_for_selection:{job_id}:{ex}:{sym}:{day}")
                partition_key = f"{ex}/{STREAM}/{sym}/{day}"
                data_key = f"exchange={ex}/stream={STREAM}/symbol={sym}/date={day}/data.parquet"
                meta_key = f"exchange={ex}/stream={STREAM}/symbol={sym}/date={day}/meta.json"
                row = [rank, job_id, sym, win, label, ex, day, partition_key, data_key, meta_key, "quantlab-compact"]
                rows.append(row)
                agg_rows.append(row)

        write_tsv(
            attempt_dir / "object_keys_window.tsv",
            ["selection_rank", "job_id", "symbol", "window_id", "label", "exchange", "date", "partition_key", "data_key", "meta_key", "bucket"],
            rows,
        )
        jobs.append(
            {
                "selection_rank": rank,
                "job_id": job_id,
                "symbol": sym,
                "start": start,
                "end": end,
                "window_id": win,
                "object_key_count": len(rows),
                "day1_quality": rec["day1_quality"],
                "day2_quality": rec["day2_quality"],
                "quality_tag": rec["quality_tag"],
            }
        )

    write_tsv(
        pack_dir / "object_keys_selected.tsv",
        ["selection_rank", "job_id", "symbol", "window_id", "label", "exchange", "date", "partition_key", "data_key", "meta_key", "bucket"],
        agg_rows,
    )
    return jobs


def aggregate_stage1(pack_dir: Path, jobs: List[dict], top_k: int) -> Tuple[str, str]:
    job_rows_loaded: Dict[str, Dict[str, int]] = {}
    det_rows: List[List[object]] = []
    result_rows: List[List[object]] = []

    for j in jobs:
        job_id = j["job_id"]
        rank = int(j["selection_rank"])
        p_rollup = pack_dir / "attempts" / job_id / "run_primary" / "results_rollup.tsv"
        p_summary = pack_dir / "attempts" / job_id / "run_primary" / "summary.json"
        rows_loaded = {ex: 0 for ex in EXCHANGES}
        if p_summary.exists():
            sj = json.loads(p_summary.read_text(encoding="utf-8"))
            src = ((sj.get("inputs") or {}).get("rows_loaded_by_exchange") or {})
            for ex in EXCHANGES:
                rows_loaded[ex] = int(src.get(ex, 0))
        job_rows_loaded[job_id] = rows_loaded

        det_rows.append(
            [
                rank,
                job_id,
                j["symbol"],
                j["window_id"],
                "",
                "",
                "SKIP_STAGE1",
                DETERMINISM_BASIS,
            ]
        )

        if not p_rollup.exists():
            continue
        coverage_pass = int(j["object_key_count"]) == 6 and all(rows_loaded.get(ex, 0) >= 200 for ex in EXCHANGES)
        with p_rollup.open("r", encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                pair = r["pair"]
                dt_ms = int(r["delta_t_ms"])
                h_ms = int(r["h_ms"])
                event_count = int(r["event_count"])
                mean_bps = float(r["mean_forward_return_bps"])
                t_stat = float(r["t_stat"])
                support_pass = coverage_pass and event_count >= 200
                if not support_pass:
                    label = "INSUFFICIENT_SUPPORT"
                elif abs(t_stat) >= 3.0 and mean_bps > 0:
                    label = "DIRECTIONAL"
                elif abs(t_stat) >= 3.0 and mean_bps < 0:
                    label = "ANTI_EDGE"
                else:
                    label = "NO_EDGE"
                result_rows.append(
                    [
                        rank,
                        job_id,
                        j["symbol"],
                        j["window_id"],
                        pair,
                        dt_ms,
                        h_ms,
                        event_count,
                        f"{mean_bps:.15f}",
                        f"{t_stat:.15f}",
                        "true" if coverage_pass else "false",
                        "true" if support_pass else "false",
                        "SKIP_STAGE1",
                        label,
                        j["day1_quality"],
                        j["day2_quality"],
                    ]
                )

    write_tsv(
        pack_dir / "determinism_compare.tsv",
        ["selection_rank", "job_id", "symbol", "window_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
        det_rows,
    )

    write_tsv(
        pack_dir / "results_windows.tsv",
        [
            "selection_rank",
            "job_id",
            "symbol",
            "window_id",
            "pair",
            "dt_ms",
            "h_ms",
            "event_count",
            "mean_bps",
            "t_stat",
            "coverage_pass",
            "support_pass",
            "determinism_status",
            "window_label",
            "day1_quality",
            "day2_quality",
        ],
        result_rows,
    )

    by_cell: Dict[str, List[List[object]]] = {}
    for r in result_rows:
        pair = str(r[4])
        dt_ms = int(r[5])
        h_ms = int(r[6])
        cell_id = f"{STREAM}|{pair}|dt={dt_ms}|H={h_ms}|tol={TOL_MS}"
        by_cell.setdefault(cell_id, []).append(r)

    def p10(values: List[float]) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        idx = int((len(s) - 1) * 0.10)
        return s[idx]

    cell_rows: List[List[object]] = []
    for cell_id, rows in sorted(by_cell.items(), key=lambda x: x[0]):
        sample = rows[0]
        pair = str(sample[4])
        dt_ms = int(sample[5])
        h_ms = int(sample[6])
        total = len(rows)
        valid = [r for r in rows if str(r[11]) == "true"]
        means = [float(r[8]) for r in valid]
        n_actual = len(valid)
        support_rate = (n_actual / total) if total else 0.0
        dir_count = sum(1 for r in rows if str(r[13]) == "DIRECTIONAL")
        anti_count = sum(1 for r in rows if str(r[13]) == "ANTI_EDGE")
        noedge_count = sum(1 for r in rows if str(r[13]) == "NO_EDGE")
        insuff_count = sum(1 for r in rows if str(r[13]) == "INSUFFICIENT_SUPPORT")
        detfail_count = sum(1 for r in rows if str(r[13]) == "FAIL/DETERMINISM_FAIL")
        directional_rate = (dir_count / n_actual) if n_actual else 0.0
        med = statistics.median(means) if means else 0.0
        p10v = p10(means)
        minv = min(means) if means else 0.0
        score = max(p10v, 0.0) * directional_rate * math.log1p(max(n_actual, 0))
        prelim = "CANDIDATE" if (n_actual >= 12 and p10v >= 1.0 and med >= 1.2 and directional_rate >= 0.8 and insuff_count == 0) else "THIN_EDGE_PRELIM"
        cell_rows.append(
            [
                cell_id,
                pair,
                dt_ms,
                h_ms,
                total,
                n_actual,
                f"{support_rate:.6f}",
                f"{med:.15f}",
                f"{p10v:.15f}",
                f"{minv:.15f}",
                f"{directional_rate:.6f}",
                dir_count,
                anti_count,
                noedge_count,
                insuff_count,
                detfail_count,
                f"{score:.15f}",
                prelim,
            ]
        )

    write_tsv(
        pack_dir / "cell_summary.tsv",
        [
            "cell_id",
            "pair",
            "dt_ms",
            "h_ms",
            "N_total_rows",
            "N_actual_stage1",
            "support_pass_rate",
            "median_mean_bps",
            "p10_mean_bps",
            "min_mean_bps",
            "directional_rate",
            "count_DIRECTIONAL",
            "count_ANTI_EDGE",
            "count_NO_EDGE",
            "count_INSUFFICIENT_SUPPORT",
            "count_FAIL_DETERMINISM",
            "stage1_score",
            "stage1_cell_label",
        ],
        cell_rows,
    )

    ranked = sorted(
        cell_rows,
        key=lambda r: (
            -float(r[7]),
            -float(r[8]),
            -float(r[10]),
            -int(r[5]),
            str(r[0]),
        ),
    )
    top_rows = ranked[:top_k]
    write_tsv(
        pack_dir / "stage1_topk.tsv",
        [
            "rank",
            "cell_id",
            "pair",
            "dt_ms",
            "h_ms",
            "N_actual_stage1",
            "median_mean_bps",
            "p10_mean_bps",
            "min_mean_bps",
            "directional_rate",
            "support_pass_rate",
            "stage1_score",
            "stage1_cell_label",
        ],
        [
            [i, r[0], r[1], r[2], r[3], r[5], r[7], r[8], r[9], r[10], r[6], r[16], r[17]]
            for i, r in enumerate(top_rows, start=1)
        ],
    )

    best_cell_id = str(top_rows[0][0]) if top_rows else "NONE"
    result = f"PASS/STAGE1_TOPK_READY:{len(top_rows)}" if top_rows else "FAIL/STAGE1_NO_CANDIDATE"
    return result, best_cell_id


def write_label_and_manifest(
    pack_dir: Path,
    result: str,
    best_cell_id: str,
    n_selected: int,
    n_completed: int,
    n_failed_oom: int,
    n_failed_other: int,
    n_skipped_resume: int,
    est_per_window_s: float,
    est_source: str,
    timebox_hit: bool,
    resume_from_pack: str,
    resume_mode: bool,
    resume_no_new_jobs: bool,
    topk_inherited_from_pack: str,
) -> None:
    code_ref = (pack_dir / "analysis" / "code_version_ref.txt").read_text(encoding="utf-8").strip()
    hash_inputs = hashlib.sha256(
        (
            (pack_dir / "selected_windows.tsv").read_text(encoding="utf-8")
            + (pack_dir / "object_keys_selected.tsv").read_text(encoding="utf-8")
        ).encode()
    ).hexdigest()
    hash_outputs = hashlib.sha256(
        (
            (pack_dir / "results_windows.tsv").read_text(encoding="utf-8")
            + (pack_dir / "cell_summary.tsv").read_text(encoding="utf-8")
            + (pack_dir / "stage1_topk.tsv").read_text(encoding="utf-8")
            + (pack_dir / "determinism_compare.tsv").read_text(encoding="utf-8")
        ).encode()
    ).hexdigest()

    label = result.split(":", 1)[0]
    lines = [
        f"label={label}",
        "artifact_manifest=selected_windows.tsv,object_keys_selected.tsv,progress.tsv,results_windows.tsv,cell_summary.tsv,stage1_topk.tsv,determinism_compare.tsv,label_report",
        (
            "decision_inputs="
            f"N_selected={n_selected},N_completed={n_completed},N_failed_oom={n_failed_oom},"
            f"N_failed_other={n_failed_other},N_skipped_resume={n_skipped_resume},best_cell_id={best_cell_id}"
        ),
        f"estimator=per_window_s:{est_per_window_s:.6f},source:{est_source}",
        f"timebox_hit={str(timebox_hit).lower()}",
        f"resume_mode={str(resume_mode).lower()}",
        f"resume_from_pack={resume_from_pack}",
        f"hash_inputs={hash_inputs}",
        f"hash_outputs={hash_outputs}",
        f"compare_basis={DETERMINISM_BASIS}",
        f"run_id={pack_dir.name}",
        f"code_version_ref={code_ref}",
        "metric_contract_note=bbo-only stage1; robust decision deferred to stage2 targeted with ONvsON",
        "scope_guard=No new fields beyond listed; stream=bbo only",
        f"run_trace=no_reruns=true;primary_only=true;max_wall={int(MAX_WALL)}",
    ]
    if resume_no_new_jobs:
        lines.append("resume_no_new_jobs=true")
        lines.append(f"topk_inherited_from_pack={topk_inherited_from_pack}")
    (pack_dir / "label_report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    write_tsv(
        pack_dir / "artifact_manifest.tsv",
        ["expected_relpath", "resolved_relpath", "status"],
        [
            ["selected_windows.tsv", "selected_windows.tsv", "OK"],
            ["object_keys_selected.tsv", "object_keys_selected.tsv", "OK"],
            ["progress.tsv", "progress.tsv", "OK"],
            ["results_windows.tsv", "results_windows.tsv", "OK"],
            ["cell_summary.tsv", "cell_summary.tsv", "OK"],
            ["stage1_topk.tsv", "stage1_topk.tsv", "OK"],
            ["determinism_compare.tsv", "determinism_compare.tsv", "OK"],
            ["result.txt", "result.txt", "OK"],
            ["label_report.txt", "label_report.txt", "OK"],
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


def read_topk_stats(stage1_topk_path: Path) -> Tuple[int, str]:
    if not stage1_topk_path.exists():
        return 0, "NONE"
    with stage1_topk_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    if not rows:
        return 0, "NONE"
    return len(rows), str(rows[0].get("cell_id") or "NONE")


def load_jobs_from_pack_files(pack_dir: Path) -> List[dict]:
    selected_path = pack_dir / "selected_windows.tsv"
    obj_path = pack_dir / "object_keys_selected.tsv"
    if not selected_path.exists() or not obj_path.exists():
        raise RuntimeError("missing_selected_or_object_keys_for_resume")
    selected = list(csv.DictReader(selected_path.open("r", encoding="utf-8", newline=""), delimiter="\t"))
    obj_rows = list(csv.DictReader(obj_path.open("r", encoding="utf-8", newline=""), delimiter="\t"))
    obj_counts: Dict[str, int] = {}
    for r in obj_rows:
        obj_counts[r["job_id"]] = obj_counts.get(r["job_id"], 0) + 1
    jobs: List[dict] = []
    for r in selected:
        rank = int(r["selection_rank"])
        job_id = f"job_{rank:03d}"
        jobs.append(
            {
                "selection_rank": rank,
                "job_id": job_id,
                "symbol": r["symbol"],
                "start": r["start"],
                "end": r["end"],
                "window_id": r["window_id"],
                "object_key_count": int(obj_counts.get(job_id, 0)),
                "day1_quality": r["day1_quality"],
                "day2_quality": r["day2_quality"],
                "quality_tag": r["quality_tag"],
            }
        )
    jobs.sort(key=lambda x: int(x["selection_rank"]))
    return jobs


def materialize_attempt_object_keys(pack_dir: Path) -> None:
    obj_path = pack_dir / "object_keys_selected.tsv"
    if not obj_path.exists():
        raise RuntimeError("missing_object_keys_selected_for_materialize")
    grouped: Dict[str, List[List[object]]] = {}
    with obj_path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            jid = r["job_id"]
            grouped.setdefault(jid, []).append(
                [
                    r["selection_rank"],
                    r["job_id"],
                    r["symbol"],
                    r["window_id"],
                    r["label"],
                    r["exchange"],
                    r["date"],
                    r["partition_key"],
                    r["data_key"],
                    r["meta_key"],
                    r["bucket"],
                ]
            )
    for jid, rows in grouped.items():
        adir = pack_dir / "attempts" / jid
        adir.mkdir(parents=True, exist_ok=True)
        write_tsv(
            adir / "object_keys_window.tsv",
            ["selection_rank", "job_id", "symbol", "window_id", "label", "exchange", "date", "partition_key", "data_key", "meta_key", "bucket"],
            rows,
        )


def main() -> int:
    args = parse_args()
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archive_root = Path(f"/home/deploy/quantlab-evidence-archive/{dt.datetime.utcnow().strftime('%Y%m%d')}_slim")
    resume_mode = bool(str(args.resume_from_pack).strip())
    resume_from_pack = ""
    old_pack_dir: Optional[Path] = None
    if resume_mode:
        old_pack_dir = resolve_pack_dir(str(args.resume_from_pack))
        resume_from_pack = old_pack_dir.name
        rix = next_resume_index(resume_from_pack)
        pack_name = f"multi-hypothesis-phase5-latency-leadlag-v1-bbo-wide-{ts}__resume{rix}"
    else:
        pack_name = f"multi-hypothesis-phase5-latency-leadlag-v1-bbo-wide-{ts}"
    state_json = Path(f"/tmp/quantlab_compacted_state_bbo_wide_{ts}.json")
    runner = StepRunner(REPO, MAX_WALL)
    pack = runner.init_pack(pack_name)
    progress_path = pack.dir / "progress.tsv"
    ensure_progress_file(progress_path)
    resume_latest: Dict[Tuple[int, str, str], dict] = {}
    if resume_mode and old_pack_dir is not None:
        resume_latest = read_progress_latest(old_pack_dir / "progress.tsv")

    pre_note = (
        "Phase-5 latency_leadlag_v1 BBO-wide stage1 campaign.\n"
        "Goal: primary-only wide scan across symbols/windows to produce TOP_K candidates.\n"
        "ROBUST decision is deferred to stage2 targeted ONvsON.\n"
        f"resume_mode={str(resume_mode).lower()} resume_from_pack={resume_from_pack or 'NONE'}\n"
    )
    write_text_file(pack.dir / "analysis" / "pre_note.txt", pre_note)

    runner.run_step(pack, "precheck", "test -x tools/slim_finalize.sh && test -x /tmp/s3_compact_tool.py && test -f tools/hypotheses/latency_leadlag_v1.py")
    runner.run_step(pack, "code_version", f"git rev-parse HEAD > {q(pack.dir / 'analysis' / 'code_version_ref.txt')}")
    runner.run_step(pack, "state_fetch", f"python3 /tmp/s3_compact_tool.py get quantlab-compact compacted/_state.json {q(state_json)}")

    result = "FAIL/STAGE1_NO_CANDIDATE"
    best_cell = "NONE"
    est_per_window_s = 0.0
    est_source = "n/a"
    n_selected = 0
    n_completed = 0
    n_failed_oom = 0
    n_failed_other = 0
    n_skipped_resume = 0
    resume_no_new_jobs = False
    topk_inherited_from_pack = ""

    state = read_state(state_json)
    meta = build_meta(state=state, exchanges=EXCHANGES, symbols=CORE_SYMBOLS)
    jobs: List[dict] = []

    if resume_mode and old_pack_dir is not None:
        runner.run_step(
            pack,
            "resume_copy_selection_inputs",
            " && ".join(
                [
                    f"if test -f {q(old_pack_dir / 'selected_windows.tsv')}; then cp -f {q(old_pack_dir / 'selected_windows.tsv')} {q(pack.dir / 'selected_windows.tsv')}; fi",
                    f"if test -f {q(old_pack_dir / 'object_keys_selected.tsv')}; then cp -f {q(old_pack_dir / 'object_keys_selected.tsv')} {q(pack.dir / 'object_keys_selected.tsv')}; fi",
                    f"if test -f {q(old_pack_dir / 'selection_proof' / 'symbol_rankings.tsv')}; then mkdir -p {q(pack.dir / 'selection_proof')} && cp -f {q(old_pack_dir / 'selection_proof' / 'symbol_rankings.tsv')} {q(pack.dir / 'selection_proof' / 'symbol_rankings.tsv')}; fi",
                ]
            ),
        )
        materialize_attempt_object_keys(pack.dir)
        jobs = load_jobs_from_pack_files(pack.dir)
        if args.max_windows and args.max_windows > 0:
            jobs = jobs[: int(args.max_windows)]
        n_selected = len(jobs)
        write_text_file(
            pack.dir / "analysis" / "selection_summary.json",
            json.dumps(
                {
                    "stream": STREAM,
                    "resume_mode": True,
                    "resume_from_pack": resume_from_pack,
                    "n_windows_selected": n_selected,
                    "max_wall_s": MAX_WALL,
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
        )
    else:
        symbol_windows = build_symbol_windows(meta=meta, symbols=CORE_SYMBOLS, exchanges=EXCHANGES)
        total_candidates = sum(len(v) for v in symbol_windows.values())
        if total_candidates <= 0:
            write_tsv(pack.dir / "selected_windows.tsv", ["selection_rank", "symbol", "window_id", "start", "end", "day1_quality", "day2_quality", "quality_tag", "rows_total_day1", "rows_total_day2", "rows_total_2day"], [])
            write_tsv(pack.dir / "object_keys_selected.tsv", ["selection_rank", "job_id", "symbol", "window_id", "label", "exchange", "date", "partition_key", "data_key", "meta_key", "bucket"], [])
            jobs = []
            result = "FAIL/STAGE1_NO_CANDIDATE;detail=no_eligible_window"
        else:
            est_per_window_s, est_source = estimate_per_window_s()
            exec_budget = max(0.0, MAX_WALL - RESERVED_OVERHEAD)
            n_target = int(exec_budget // est_per_window_s) if est_per_window_s > 0 else 1
            n_target = max(1, min(total_candidates, n_target))
            if args.max_windows and args.max_windows > 0:
                n_target = min(n_target, int(args.max_windows))
            selected, rank_rows = select_windows_round_robin(symbol_windows=symbol_windows, n_target=n_target)
            jobs = write_selection_proofs(pack_dir=pack.dir, selected=selected, rank_rows=rank_rows, meta=meta)
            n_selected = len(jobs)
            write_text_file(
                pack.dir / "analysis" / "selection_summary.json",
                json.dumps(
                    {
                        "stream": STREAM,
                        "total_candidates": total_candidates,
                        "n_windows_target": n_target,
                        "n_windows_selected": len(selected),
                        "est_per_window_s": est_per_window_s,
                        "est_source": est_source,
                        "max_wall_s": MAX_WALL,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
                + "\n",
            )

    print(
        f"PROGRESS stage=selection done selected={len(jobs)} resume_mode={str(resume_mode).lower()} "
        f"resume_from={resume_from_pack or 'NONE'}",
        flush=True,
    )

    consecutive_oom = 0
    stop_due_oom = False
    last_progress_emit = dt.datetime.utcnow()
    forced_oom_job = os.getenv("QLAB_TEST_FORCE_EXIT137_JOB", "").strip()

    for j in jobs:
        if runner.timebox_hit:
            print(
                f"PROGRESS stage=run timebox_hit=true completed_jobs={n_completed} total_jobs={len(jobs)} "
                f"failed_oom={n_failed_oom} failed_other={n_failed_other} skipped_resume={n_skipped_resume} "
                f"cum_wall_s={runner.cumulative_wall_s:.3f}",
                flush=True,
            )
            break
        if stop_due_oom:
            break

        rank = int(j["selection_rank"])
        key = (rank, j["symbol"], j["window_id"])
        if key in resume_latest and resume_latest[key].get("status", "") in TERMINAL_SKIP_STATUSES:
            n_skipped_resume += 1
            append_progress_row(
                progress_path=progress_path,
                selection_rank=rank,
                symbol=j["symbol"],
                window_id=j["window_id"],
                status="SKIPPED",
                exit_code=0,
                elapsed_s=0.0,
                max_rss_kb=0,
                notes=f"resume_skip_from={resume_from_pack};old_status={resume_latest[key].get('status','')}",
            )
            continue

        if forced_oom_job and forced_oom_job == j["job_id"]:
            cmd = "bash -lc 'exit 137'"
        else:
            cmd = (
                "python3 tools/hypotheses/latency_leadlag_v1.py "
                f"--object-keys-tsv {q(pack.dir / 'attempts' / j['job_id'] / 'object_keys_window.tsv')} "
                f"--downloads-dir {q(pack.dir / 'attempts' / j['job_id'] / 'downloads_primary')} "
                f"--exchange-order {q(','.join(EXCHANGES))} "
                f"--symbol {q(j['symbol'])} --stream {q(STREAM)} "
                f"--start {q(j['start'])} --end {q(j['end'])} "
                f"--tolerance-ms {q(TOL_MS)} --pair-mode all6 "
                f"--delta-ms-list {q(DELTA_LIST)} --h-ms-list {q(H_LIST)} "
                f"--results-out {q(pack.dir / 'attempts' / j['job_id'] / 'run_primary' / 'results_rollup.tsv')} "
                f"--pair-support-out {q(pack.dir / 'attempts' / j['job_id'] / 'run_primary' / 'pair_support.tsv')} "
                f"--summary-out {q(pack.dir / 'attempts' / j['job_id'] / 'run_primary' / 'summary.json')}"
            )

        cap = runner.run_step_capture(pack, f"primary_{j['job_id']}", cmd)
        stderr_text = cap.stderr_path.read_text(encoding="utf-8", errors="replace") if cap.stderr_path.exists() else ""
        status = classify_failure(exit_code=cap.exit_code, stderr_text=stderr_text)

        if status == "DONE":
            n_completed += 1
            consecutive_oom = 0
        elif status == "FAILED_OOM":
            n_failed_oom += 1
            consecutive_oom += 1
        else:
            n_failed_other += 1
            consecutive_oom = 0

        append_progress_row(
            progress_path=progress_path,
            selection_rank=rank,
            symbol=j["symbol"],
            window_id=j["window_id"],
            status=status,
            exit_code=cap.exit_code,
            elapsed_s=cap.elapsed_s,
            max_rss_kb=cap.max_rss_kb,
            notes=f"job_id={j['job_id']}",
        )

        now = dt.datetime.utcnow()
        if (now - last_progress_emit).total_seconds() >= max(10, int(args.progress_interval_sec)):
            print(
                f"PROGRESS stage=run completed_jobs={n_completed} total_jobs={len(jobs)} "
                f"failed_oom={n_failed_oom} failed_other={n_failed_other} skipped_resume={n_skipped_resume} "
                f"last_job={j['job_id']} status={status} cum_wall_s={runner.cumulative_wall_s:.3f}",
                flush=True,
            )
            last_progress_emit = now

        if consecutive_oom >= 2:
            stop_due_oom = True
            print(
                f"PROGRESS stage=run stop_reason=consecutive_oom_2 last_job={j['job_id']} "
                f"cum_wall_s={runner.cumulative_wall_s:.3f}",
                flush=True,
            )
            break

    if not jobs:
        write_tsv(pack.dir / "results_windows.tsv", ["selection_rank", "job_id", "symbol", "window_id", "pair", "dt_ms", "h_ms", "event_count", "mean_bps", "t_stat", "coverage_pass", "support_pass", "determinism_status", "window_label", "day1_quality", "day2_quality"], [])
        write_tsv(pack.dir / "cell_summary.tsv", ["cell_id", "pair", "dt_ms", "h_ms", "N_total_rows", "N_actual_stage1", "support_pass_rate", "median_mean_bps", "p10_mean_bps", "min_mean_bps", "directional_rate", "count_DIRECTIONAL", "count_ANTI_EDGE", "count_NO_EDGE", "count_INSUFFICIENT_SUPPORT", "count_FAIL_DETERMINISM", "stage1_score", "stage1_cell_label"], [])
        write_tsv(pack.dir / "stage1_topk.tsv", ["rank", "cell_id", "pair", "dt_ms", "h_ms", "N_actual_stage1", "median_mean_bps", "p10_mean_bps", "min_mean_bps", "directional_rate", "support_pass_rate", "stage1_score", "stage1_cell_label"], [])
        write_tsv(pack.dir / "determinism_compare.tsv", ["selection_rank", "job_id", "symbol", "window_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"], [])
        if "detail=" not in result:
            result = "FAIL/STAGE1_NO_CANDIDATE;detail=no_jobs_selected"
    else:
        should_inherit = bool(resume_mode and old_pack_dir is not None and n_completed == 0 and n_skipped_resume > 0)
        if resume_mode:
            runner.run_step(
                pack,
                "resume_no_new_jobs_guard",
                f"printf '%s\\n' {q('resume_no_new_jobs=' + str(should_inherit).lower() + ',n_completed=' + str(n_completed) + ',n_skipped_resume=' + str(n_skipped_resume) + ',resume_from_pack=' + (resume_from_pack or 'NONE'))}",
            )
        if should_inherit and old_pack_dir is not None:
            runner.run_step(
                pack,
                "resume_inherit_stage1_outputs",
                " && ".join(
                    [
                        f"test -f {q(old_pack_dir / 'stage1_topk.tsv')}",
                        f"cp -f {q(old_pack_dir / 'stage1_topk.tsv')} {q(pack.dir / 'stage1_topk.tsv')}",
                        f"test -f {q(old_pack_dir / 'cell_summary.tsv')}",
                        f"cp -f {q(old_pack_dir / 'cell_summary.tsv')} {q(pack.dir / 'cell_summary.tsv')}",
                        f"if test -f {q(old_pack_dir / 'results_windows.tsv')}; then cp -f {q(old_pack_dir / 'results_windows.tsv')} {q(pack.dir / 'results_windows.tsv')}; fi",
                        f"if test -f {q(old_pack_dir / 'determinism_compare.tsv')}; then cp -f {q(old_pack_dir / 'determinism_compare.tsv')} {q(pack.dir / 'determinism_compare.tsv')}; fi",
                    ]
                ),
            )
            if not (pack.dir / "results_windows.tsv").exists():
                write_tsv(
                    pack.dir / "results_windows.tsv",
                    ["selection_rank", "job_id", "symbol", "window_id", "pair", "dt_ms", "h_ms", "event_count", "mean_bps", "t_stat", "coverage_pass", "support_pass", "determinism_status", "window_label", "day1_quality", "day2_quality"],
                    [],
                )
            if not (pack.dir / "determinism_compare.tsv").exists():
                write_tsv(
                    pack.dir / "determinism_compare.tsv",
                    ["selection_rank", "job_id", "symbol", "window_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
                    [],
                )
            topk_count, best_cell = read_topk_stats(pack.dir / "stage1_topk.tsv")
            if topk_count > 0:
                result = f"PASS/STAGE1_TOPK_READY:{topk_count};detail=resume_no_new_jobs"
            else:
                result = "FAIL/STAGE1_NO_CANDIDATE;detail=resume_no_new_jobs_missing_topk"
                best_cell = "NONE"
            resume_no_new_jobs = True
            topk_inherited_from_pack = resume_from_pack
        else:
            result, best_cell = aggregate_stage1(pack.dir, jobs, TOP_K)
            if stop_due_oom:
                result = result + ";detail=consecutive_oom_stop"
            elif n_failed_oom > 0 or n_failed_other > 0:
                result = result + ";detail=partial_job_failures"
            elif runner.timebox_hit:
                result = result + ";detail=timebox_partial"

    write_text_file(pack.dir / "result.txt", result + "\n")
    write_label_and_manifest(
        pack_dir=pack.dir,
        result=result,
        best_cell_id=best_cell,
        n_selected=len(jobs),
        n_completed=n_completed,
        n_failed_oom=n_failed_oom,
        n_failed_other=n_failed_other,
        n_skipped_resume=n_skipped_resume,
        est_per_window_s=est_per_window_s,
        est_source=est_source,
        timebox_hit=runner.timebox_hit,
        resume_from_pack=resume_from_pack,
        resume_mode=resume_mode,
        resume_no_new_jobs=resume_no_new_jobs,
        topk_inherited_from_pack=topk_inherited_from_pack,
    )

    write_integrity(
        pack.dir,
        [
            "selected_windows.tsv",
            "object_keys_selected.tsv",
            "progress.tsv",
            "results_windows.tsv",
            "cell_summary.tsv",
            "stage1_topk.tsv",
            "determinism_compare.tsv",
            "result.txt",
            "label_report.txt",
            "artifact_manifest.tsv",
            "command_index.tsv",
            "time_v_summary.tsv",
        ],
    )

    runner.run_step(pack, "finalize", f"bash tools/slim_finalize.sh {q(pack.name)} {q(pack.dir)} {q(archive_root)}")
    runner.run_step(
        pack,
        "sha_verify",
        f"sha256sum -c {q(REPO / 'evidence' / f'{pack.name}.tar.gz.sha256')} > {q(REPO / 'evidence' / f'{pack.name}.sha_verify_tmp.txt')}; "
        f"MOVED_TO=$(cat {q(REPO / 'evidence' / f'{pack.name}.moved_to.txt')}); "
        f"cp {q(REPO / 'evidence' / f'{pack.name}.sha_verify_tmp.txt')} \"$MOVED_TO/sha_verify.txt\"; "
        f"rm -f {q(REPO / 'evidence' / f'{pack.name}.sha_verify_tmp.txt')}",
    )
    runner.run_step(
        pack,
        "post_guard",
        f"test ! -d {q(REPO / 'evidence' / pack.name)} && "
        f"test -f {q(REPO / 'evidence' / f'{pack.name}.tar.gz')} && "
        f"test -f {q(REPO / 'evidence' / f'{pack.name}.tar.gz.sha256')} && "
        f"test -f {q(REPO / 'evidence' / f'{pack.name}.moved_to.txt')} && "
        f"MOVED_TO=$(cat {q(REPO / 'evidence' / f'{pack.name}.moved_to.txt')}) && "
        f"test -d \"$MOVED_TO\" && grep -q OK \"$MOVED_TO/sha_verify.txt\"",
    )

    moved_to = Path((REPO / "evidence" / f"{pack.name}.moved_to.txt").read_text(encoding="utf-8").strip())
    final_result = (moved_to / "result.txt").read_text(encoding="utf-8").strip()
    print(f"PACK={pack.name}")
    print(f"MOVED_TO={moved_to}")
    print(f"FINAL_RESULT={final_result}")
    print(f"GLOBAL_CUM_WALL={runner.cumulative_wall_s:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
