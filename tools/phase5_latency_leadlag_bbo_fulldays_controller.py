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
from typing import Dict, Iterable, List, Optional, Set, Tuple


REPO = Path("/home/deploy/quantlab-backend")
NO_MAX_WALL_SECONDS = 10**12
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
CAMPAIGN_CATEGORY = "FULLSCAN"
CAMPAIGN_IMPORTANCE = "MAJOR"
CAMPAIGN_KIND = "phase5_latency_leadlag_v1_bbo_fulldays"
CAMPAIGN_NOTES = "This is a major full-days sweep; keep distinct."
PROGRESS_HEADER = [
    "selection_rank",
    "symbol",
    "date",
    "status",
    "exit_code",
    "elapsed_s",
    "max_rss_kb",
    "notes",
]
TERMINAL_SKIP_STATUSES = {"DONE", "FAILED_OOM", "FAILED_OTHER", "SKIPPED_RESUME", "SKIPPED"}


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


@dataclass
class JobRun:
    status: str
    exit_code: int
    elapsed_s: float
    max_rss_kb: int
    primary_hash: str
    replay_hash: str
    determinism_status: str
    primary_rows: List[dict]
    rows_loaded_by_exchange: Dict[str, int]
    smoke_event_positive: bool


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


def write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def q(v: object) -> str:
    return shlex.quote(str(v))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 latency_leadlag_v1 bbo full-days controller")
    p.add_argument("--resume-from-pack", default="", help="Old pack name or absolute moved pack path")
    p.add_argument("--sanity-dates-file", default="", help="Optional YYYYMMDD list; reporting only")
    p.add_argument("--job-timeout-sec", type=int, default=1200, help="Timeout per primary/replay run")
    p.add_argument("--progress-interval-sec", type=int, default=60)
    p.add_argument("--max-jobs", type=int, default=0, help="Optional cap; 0 means all eligible")
    return p.parse_args()


def ensure_progress_file(progress_path: Path) -> None:
    if progress_path.exists():
        return
    write_tsv(progress_path, PROGRESS_HEADER, [])


def append_progress_row(
    progress_path: Path,
    selection_rank: int,
    symbol: str,
    date: str,
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
                date,
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
            key = (int(r["selection_rank"]), r["symbol"], r["date"])
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
        if ex not in exchanges or stream != STREAM or sym not in symbol_set:
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


def build_symbol_days(meta: Dict[Tuple[str, str, str], dict], symbols: List[str], exchanges: List[str]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {sym: [] for sym in symbols}
    for sym in symbols:
        date_sets: List[Set[str]] = []
        for ex in exchanges:
            date_sets.append({d for (e, s, d) in meta if e == ex and s == sym})
        common = sorted(set.intersection(*date_sets)) if date_sets else []
        for date in common:
            rows_total = 0
            day_quality = "GOOD"
            for ex in exchanges:
                m = meta[(ex, sym, date)]
                rows_total += int(m["rows"])
                if m["quality"] != "GOOD":
                    day_quality = "DEGRADED"
            out[sym].append(
                {
                    "symbol": sym,
                    "date": date,
                    "day_quality": day_quality,
                    "rows_total_day": rows_total,
                }
            )
    return out


def rank_rows_for_proof(symbol_days: Dict[str, List[dict]]) -> List[Tuple[str, int, int]]:
    rows: List[Tuple[str, int, int]] = []
    for sym, days in symbol_days.items():
        if not days:
            continue
        rows.append((sym, len(days), min(int(d["rows_total_day"]) for d in days)))
    rows.sort(key=lambda x: (-x[1], x[2], x[0]))
    return rows


def select_days(symbol_days: Dict[str, List[dict]], symbols_order: List[str], max_jobs: int) -> List[dict]:
    selected: List[dict] = []
    for sym in symbols_order:
        for rec in sorted(symbol_days.get(sym, []), key=lambda x: x["date"]):
            selected.append(dict(rec))
    if max_jobs > 0:
        selected = selected[:max_jobs]
    for i, rec in enumerate(selected, start=1):
        rec["selection_rank"] = i
    return selected


def write_selection_proofs(
    pack_dir: Path,
    selected: List[dict],
    rank_rows: List[Tuple[str, int, int]],
) -> List[dict]:
    sel_dir = pack_dir / "selection_proof"
    sel_dir.mkdir(parents=True, exist_ok=True)

    write_tsv(
        sel_dir / "symbol_rankings.tsv",
        ["symbol", "coverage_days_count", "rows_total_day_min"],
        [[sym, cnt, minrows] for sym, cnt, minrows in rank_rows],
    )

    write_tsv(
        pack_dir / "selected_days.tsv",
        ["selection_rank", "symbol", "date", "day_quality", "rows_total_day"],
        [[r["selection_rank"], r["symbol"], r["date"], r["day_quality"], r["rows_total_day"]] for r in selected],
    )

    agg_rows: List[List[object]] = []
    jobs: List[dict] = []
    for rec in selected:
        rank = int(rec["selection_rank"])
        sym = rec["symbol"]
        date = rec["date"]
        day_quality = rec["day_quality"]
        job_id = f"job_{rank:03d}"
        attempt_dir = pack_dir / "attempts" / job_id
        attempt_dir.mkdir(parents=True, exist_ok=True)

        rows = []
        for ex in EXCHANGES:
            partition_key = f"{ex}/{STREAM}/{sym}/{date}"
            data_key = f"exchange={ex}/stream={STREAM}/symbol={sym}/date={date}/data.parquet"
            meta_key = f"exchange={ex}/stream={STREAM}/symbol={sym}/date={date}/meta.json"
            row = [rank, job_id, sym, date, "day", ex, partition_key, data_key, meta_key, "quantlab-compact"]
            rows.append(row)
            agg_rows.append(row)
        write_tsv(
            attempt_dir / "object_keys_day.tsv",
            ["selection_rank", "job_id", "symbol", "date", "label", "exchange", "partition_key", "data_key", "meta_key", "bucket"],
            rows,
        )
        jobs.append(
            {
                "selection_rank": rank,
                "job_id": job_id,
                "symbol": sym,
                "date": date,
                "day_quality": day_quality,
                "rows_total_day": int(rec["rows_total_day"]),
                "object_key_count": len(rows),
            }
        )

    write_tsv(
        pack_dir / "object_keys_selected.tsv",
        ["selection_rank", "job_id", "symbol", "date", "label", "exchange", "partition_key", "data_key", "meta_key", "bucket"],
        agg_rows,
    )
    return jobs


def load_jobs_from_pack_files(pack_dir: Path) -> List[dict]:
    selected_path = pack_dir / "selected_days.tsv"
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
                "date": r["date"],
                "day_quality": r["day_quality"],
                "rows_total_day": int(r.get("rows_total_day") or 0),
                "object_key_count": int(obj_counts.get(job_id, 0)),
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
                    r["date"],
                    r["label"],
                    r["exchange"],
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
            adir / "object_keys_day.tsv",
            ["selection_rank", "job_id", "symbol", "date", "label", "exchange", "partition_key", "data_key", "meta_key", "bucket"],
            rows,
        )


def read_sanity_dates(path: Path) -> List[str]:
    vals: Set[str] = set()
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        v = raw.strip()
        if not v:
            continue
        if not re.fullmatch(r"[0-9]{8}", v):
            raise RuntimeError(f"invalid_sanity_date:{v}")
        vals.add(v)
    return sorted(vals)


def write_sanity_check(pack_dir: Path, state_days: Set[str], user_dates: List[str]) -> bool:
    state_sorted = sorted(state_days)
    user_sorted = sorted(set(user_dates))
    inter = sorted(set(state_sorted).intersection(user_sorted))
    user_minus = sorted(set(user_sorted).difference(state_sorted))
    state_minus = sorted(set(state_sorted).difference(user_sorted))
    write_tsv(
        pack_dir / "sanity_dates_check.tsv",
        ["state_days_count", "user_list_count", "intersection_count", "user_list_minus_state", "state_minus_user_list"],
        [[len(state_sorted), len(user_sorted), len(inter), ",".join(user_minus), ",".join(state_minus)]],
    )
    return bool(user_sorted) and len(inter) == 0


def canonical_rows(rows: List[dict]) -> List[dict]:
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
            }
        )
    return out


def hash_canon(canon: List[dict]) -> str:
    return hashlib.sha256(json.dumps(canon, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()


def load_rollup_rows(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_rows_loaded_by_exchange(summary_path: Path) -> Dict[str, int]:
    out = {ex: 0 for ex in EXCHANGES}
    if not summary_path.exists():
        return out
    try:
        sj = json.loads(summary_path.read_text(encoding="utf-8"))
        src = ((sj.get("inputs") or {}).get("rows_loaded_by_exchange") or {})
        for ex in EXCHANGES:
            out[ex] = int(src.get(ex, 0))
    except Exception:
        return out
    return out


def run_one_job(
    runner: StepRunner,
    pack: PackCtx,
    job: dict,
    timeout_s: int,
    forced_oom_job: str,
) -> JobRun:
    job_id = str(job["job_id"])
    step_primary = f"primary_{job_id}"
    step_replay = f"replay_{job_id}"
    step_cleanup = f"cleanup_{job_id}"

    if forced_oom_job and forced_oom_job == job_id:
        primary_cmd = "bash -lc 'exit 137'"
    else:
        primary_cmd = (
            f"timeout {int(timeout_s)}s "
            "python3 tools/hypotheses/latency_leadlag_v1.py "
            f"--object-keys-tsv {q(pack.dir / 'attempts' / job_id / 'object_keys_day.tsv')} "
            f"--downloads-dir {q(pack.dir / 'attempts' / job_id / 'downloads_primary')} "
            f"--exchange-order {q(','.join(EXCHANGES))} "
            f"--symbol {q(job['symbol'])} --stream {q(STREAM)} "
            f"--start {q(job['date'])} --end {q(job['date'])} "
            f"--tolerance-ms {q(TOL_MS)} --pair-mode all6 "
            f"--delta-ms-list {q(DELTA_LIST)} --h-ms-list {q(H_LIST)} "
            f"--results-out {q(pack.dir / 'attempts' / job_id / 'run_primary' / 'results_rollup.tsv')} "
            f"--pair-support-out {q(pack.dir / 'attempts' / job_id / 'run_primary' / 'pair_support.tsv')} "
            f"--summary-out {q(pack.dir / 'attempts' / job_id / 'run_primary' / 'summary.json')}"
        )
    cap1 = runner.run_step_capture(pack, step_primary, primary_cmd)
    stderr1 = cap1.stderr_path.read_text(encoding="utf-8", errors="replace") if cap1.stderr_path.exists() else ""
    status1 = classify_failure(cap1.exit_code, stderr1)

    primary_rollup = pack.dir / "attempts" / job_id / "run_primary" / "results_rollup.tsv"
    primary_summary = pack.dir / "attempts" / job_id / "run_primary" / "summary.json"
    replay_rollup = pack.dir / "attempts" / job_id / "run_replay_on" / "results_rollup.tsv"

    primary_rows = load_rollup_rows(primary_rollup)
    rows_loaded = load_rows_loaded_by_exchange(primary_summary)
    primary_hash = ""
    replay_hash = ""
    det_status = "FAIL_EXEC"
    smoke_positive = any(int(r.get("event_count") or 0) > 0 for r in primary_rows)

    total_elapsed = cap1.elapsed_s
    max_rss = cap1.max_rss_kb
    out_status = status1
    out_exit = cap1.exit_code

    if status1 == "DONE":
        replay_cmd = (
            f"timeout {int(timeout_s)}s "
            "python3 tools/hypotheses/latency_leadlag_v1.py "
            f"--object-keys-tsv {q(pack.dir / 'attempts' / job_id / 'object_keys_day.tsv')} "
            f"--downloads-dir {q(pack.dir / 'attempts' / job_id / 'downloads_replay')} "
            f"--exchange-order {q(','.join(EXCHANGES))} "
            f"--symbol {q(job['symbol'])} --stream {q(STREAM)} "
            f"--start {q(job['date'])} --end {q(job['date'])} "
            f"--tolerance-ms {q(TOL_MS)} --pair-mode all6 "
            f"--delta-ms-list {q(DELTA_LIST)} --h-ms-list {q(H_LIST)} "
            f"--results-out {q(pack.dir / 'attempts' / job_id / 'run_replay_on' / 'results_rollup.tsv')} "
            f"--pair-support-out {q(pack.dir / 'attempts' / job_id / 'run_replay_on' / 'pair_support.tsv')} "
            f"--summary-out {q(pack.dir / 'attempts' / job_id / 'run_replay_on' / 'summary.json')}"
        )
        cap2 = runner.run_step_capture(pack, step_replay, replay_cmd)
        stderr2 = cap2.stderr_path.read_text(encoding="utf-8", errors="replace") if cap2.stderr_path.exists() else ""
        status2 = classify_failure(cap2.exit_code, stderr2)
        total_elapsed += cap2.elapsed_s
        max_rss = max(max_rss, cap2.max_rss_kb)
        if status2 == "DONE":
            replay_rows = load_rollup_rows(replay_rollup)
            c1 = canonical_rows(primary_rows)
            c2 = canonical_rows(replay_rows)
            primary_hash = hash_canon(c1)
            replay_hash = hash_canon(c2)
            det_status = "PASS" if c1 == c2 else "FAIL"
            out_status = "DONE"
            out_exit = 0
        else:
            out_status = status2
            out_exit = cap2.exit_code
            det_status = "FAIL"

    cleanup_cmd = (
        f"rm -rf {q(pack.dir / 'attempts' / job_id / 'downloads_primary')} "
        f"{q(pack.dir / 'attempts' / job_id / 'downloads_replay')} "
        f"{q(pack.dir / 'attempts' / job_id / 'run_primary' / 'pair_support.tsv')} "
        f"{q(pack.dir / 'attempts' / job_id / 'run_replay_on' / 'pair_support.tsv')} || true"
    )
    capc = runner.run_step_capture(pack, step_cleanup, cleanup_cmd)
    total_elapsed += capc.elapsed_s
    max_rss = max(max_rss, capc.max_rss_kb)

    return JobRun(
        status=out_status,
        exit_code=out_exit,
        elapsed_s=total_elapsed,
        max_rss_kb=max_rss,
        primary_hash=primary_hash,
        replay_hash=replay_hash,
        determinism_status=det_status,
        primary_rows=primary_rows,
        rows_loaded_by_exchange=rows_loaded,
        smoke_event_positive=smoke_positive,
    )


def rows_for_job(job: dict, run: JobRun) -> List[List[object]]:
    out: List[List[object]] = []
    coverage_pass = int(job["object_key_count"]) == 3 and all(int(run.rows_loaded_by_exchange.get(ex, 0)) >= 200 for ex in EXCHANGES)
    for r in run.primary_rows:
        event_count = int(r["event_count"])
        mean_bps = float(r["mean_forward_return_bps"])
        t_stat = float(r["t_stat"])
        support_pass = coverage_pass and event_count >= 200
        if run.determinism_status != "PASS":
            label = "FAIL/DETERMINISM_FAIL"
        elif not support_pass:
            label = "INSUFFICIENT_SUPPORT"
        elif abs(t_stat) >= 3.0 and mean_bps > 0:
            label = "DIRECTIONAL"
        elif abs(t_stat) >= 3.0 and mean_bps < 0:
            label = "ANTI_EDGE"
        else:
            label = "NO_EDGE"
        out.append(
            [
                job["selection_rank"],
                job["job_id"],
                job["symbol"],
                job["date"],
                r["pair"],
                int(r["delta_t_ms"]),
                int(r["h_ms"]),
                event_count,
                f"{mean_bps:.15f}",
                f"{t_stat:.15f}",
                "true" if coverage_pass else "false",
                "true" if support_pass else "false",
                run.determinism_status,
                label,
                job["day_quality"],
            ]
        )
    return out


def aggregate_stage1(pack_dir: Path, rows: List[List[object]], top_k: int) -> Tuple[str, str]:
    write_tsv(
        pack_dir / "results_days.tsv",
        [
            "selection_rank",
            "job_id",
            "symbol",
            "date",
            "pair",
            "dt_ms",
            "h_ms",
            "event_count",
            "mean_bps",
            "t_stat",
            "coverage_pass",
            "support_pass",
            "determinism_status",
            "day_label",
            "day_quality",
        ],
        rows,
    )

    by_cell: Dict[str, List[List[object]]] = {}
    for r in rows:
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
    for cell_id, cell in sorted(by_cell.items(), key=lambda x: x[0]):
        sample = cell[0]
        pair = str(sample[4])
        dt_ms = int(sample[5])
        h_ms = int(sample[6])
        total = len(cell)
        valid = [r for r in cell if str(r[11]) == "true" and str(r[12]) == "PASS"]
        means = [float(r[8]) for r in valid]
        n_actual = len(valid)
        support_rate = (n_actual / total) if total else 0.0
        dir_count = sum(1 for r in cell if str(r[13]) == "DIRECTIONAL")
        anti_count = sum(1 for r in cell if str(r[13]) == "ANTI_EDGE")
        noedge_count = sum(1 for r in cell if str(r[13]) == "NO_EDGE")
        insuff_count = sum(1 for r in cell if str(r[13]) == "INSUFFICIENT_SUPPORT")
        detfail_count = sum(1 for r in cell if str(r[13]) == "FAIL/DETERMINISM_FAIL")
        directional_rate = (dir_count / n_actual) if n_actual else 0.0
        med = statistics.median(means) if means else 0.0
        p10v = p10(means)
        minv = min(means) if means else 0.0
        score = max(p10v, 0.0) * directional_rate * math.log1p(max(n_actual, 0))
        prelim = "CANDIDATE" if (n_actual >= 12 and p10v >= 1.0 and med >= 1.2 and directional_rate >= 0.8 and insuff_count == 0 and detfail_count == 0) else "THIN_EDGE_PRELIM"
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
        key=lambda r: (-float(r[7]), -float(r[8]), -float(r[10]), -int(r[5]), str(r[0])),
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
        [[i, r[0], r[1], r[2], r[3], r[5], r[7], r[8], r[9], r[10], r[6], r[16], r[17]] for i, r in enumerate(top_rows, start=1)],
    )

    best_cell_id = str(top_rows[0][0]) if top_rows else "NONE"
    result = f"PASS/STAGE1_TOPK_READY:{len(top_rows)}" if top_rows else "FAIL/STAGE1_NO_CANDIDATE"
    return result, best_cell_id


def write_empty_outputs(pack_dir: Path) -> None:
    write_tsv(
        pack_dir / "results_days.tsv",
        [
            "selection_rank",
            "job_id",
            "symbol",
            "date",
            "pair",
            "dt_ms",
            "h_ms",
            "event_count",
            "mean_bps",
            "t_stat",
            "coverage_pass",
            "support_pass",
            "determinism_status",
            "day_label",
            "day_quality",
        ],
        [],
    )
    write_tsv(
        pack_dir / "determinism_compare.tsv",
        ["selection_rank", "job_id", "symbol", "date", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
        [],
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
        [],
    )
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
        [],
    )


def read_topk_stats(stage1_topk_path: Path) -> Tuple[int, str]:
    if not stage1_topk_path.exists():
        return 0, "NONE"
    with stage1_topk_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    if not rows:
        return 0, "NONE"
    return len(rows), str(rows[0].get("cell_id") or "NONE")


def validate_selection_integrity(pack_dir: Path, jobs: List[dict]) -> str:
    obj_path = pack_dir / "object_keys_selected.tsv"
    if not obj_path.exists():
        return "missing_object_keys_selected"
    by_job: Dict[str, Dict[str, object]] = {}
    with obj_path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            jid = str(r.get("job_id") or "")
            if not jid:
                continue
            rec = by_job.setdefault(jid, {"count": 0, "ex": set()})
            rec["count"] = int(rec["count"]) + 1
            ex = str(r.get("exchange") or "").strip().lower()
            if ex:
                ex_set = rec["ex"]
                if isinstance(ex_set, set):
                    ex_set.add(ex)
    expected_ex = set(EXCHANGES)
    for j in jobs:
        jid = str(j["job_id"])
        if int(j.get("object_key_count") or 0) != 3:
            return f"object_key_count_not_3:{jid}:{j.get('object_key_count')}"
        if jid not in by_job:
            return f"missing_object_keys_rows_for_job:{jid}"
        c = int(by_job[jid]["count"])
        ex_set = by_job[jid]["ex"] if isinstance(by_job[jid]["ex"], set) else set()
        if c != 3:
            return f"object_keys_rows_not_3:{jid}:{c}"
        if ex_set != expected_ex:
            return f"exchange_set_mismatch:{jid}:{'|'.join(sorted(ex_set))}"
    return ""


def write_campaign_meta(pack_dir: Path) -> None:
    created_at = dt.datetime.utcnow().isoformat() + "Z"
    write_tsv(
        pack_dir / "campaign_meta.tsv",
        ["key", "value"],
        [
            ["campaign_category", CAMPAIGN_CATEGORY],
            ["campaign_importance", CAMPAIGN_IMPORTANCE],
            ["campaign_kind", CAMPAIGN_KIND],
            ["created_at_utc", created_at],
            ["notes", CAMPAIGN_NOTES],
        ],
    )


def write_label_and_manifest(
    pack_dir: Path,
    result: str,
    best_cell_id: str,
    n_selected: int,
    n_completed: int,
    n_failed_oom: int,
    n_failed_other: int,
    n_skipped_resume: int,
    resume_from_pack: str,
    resume_mode: bool,
    resume_no_new_jobs: bool,
    topk_inherited_from_pack: str,
    smoke_gate_result: str,
    smoke_gate_candidate: str,
    sanity_warn: bool,
    sanity_file_used: bool,
    job_timeout_sec: int,
) -> None:
    code_ref = (pack_dir / "analysis" / "code_version_ref.txt").read_text(encoding="utf-8").strip()
    hash_inputs = hashlib.sha256(
        (
            (pack_dir / "selected_days.tsv").read_text(encoding="utf-8")
            + (pack_dir / "object_keys_selected.tsv").read_text(encoding="utf-8")
        ).encode()
    ).hexdigest()
    hash_outputs = hashlib.sha256(
        (
            (pack_dir / "results_days.tsv").read_text(encoding="utf-8")
            + (pack_dir / "cell_summary.tsv").read_text(encoding="utf-8")
            + (pack_dir / "stage1_topk.tsv").read_text(encoding="utf-8")
            + (pack_dir / "determinism_compare.tsv").read_text(encoding="utf-8")
        ).encode()
    ).hexdigest()
    label = result.split(":", 1)[0]
    lines = [
        f"label={label}",
        "artifact_manifest=selected_days.tsv,object_keys_selected.tsv,progress.tsv,results_days.tsv,cell_summary.tsv,stage1_topk.tsv,determinism_compare.tsv,label_report",
        (
            "decision_inputs="
            f"N_selected={n_selected},N_completed={n_completed},N_failed_oom={n_failed_oom},"
            f"N_failed_other={n_failed_other},N_skipped_resume={n_skipped_resume},best_cell_id={best_cell_id}"
        ),
        f"smoke_gate_result={smoke_gate_result}",
        f"smoke_gate_candidate={smoke_gate_candidate}",
        f"resume_mode={str(resume_mode).lower()}",
        f"resume_from_pack={resume_from_pack}",
        f"hash_inputs={hash_inputs}",
        f"hash_outputs={hash_outputs}",
        f"compare_basis={DETERMINISM_BASIS}",
        f"run_id={pack_dir.name}",
        f"code_version_ref={code_ref}",
        "scope_guard=No new fields beyond listed; stream=bbo only; selection from compacted state",
        f"run_trace=no_reruns=true;single_day=true;max_wall=none;job_timeout_sec={job_timeout_sec}",
        f"campaign_category={CAMPAIGN_CATEGORY}",
        f"campaign_importance={CAMPAIGN_IMPORTANCE}",
    ]
    if resume_no_new_jobs:
        lines.append("resume_no_new_jobs=true")
        lines.append(f"topk_inherited_from_pack={topk_inherited_from_pack}")
    if sanity_file_used:
        lines.append("sanity_dates_check_used=true")
    if sanity_warn:
        lines.append("WARN=user_list_outside_state")
    (pack_dir / "label_report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    sanity_status = "OPTIONAL_NOT_PROVIDED"
    sanity_resolved = "N/A"
    if sanity_file_used and (pack_dir / "sanity_dates_check.tsv").exists():
        sanity_status = "OK"
        sanity_resolved = "sanity_dates_check.tsv"
    elif sanity_file_used:
        sanity_status = "MISSING"
        sanity_resolved = "N/A"

    write_tsv(
        pack_dir / "artifact_manifest.tsv",
        ["expected_relpath", "resolved_relpath", "status"],
        [
            ["selected_days.tsv", "selected_days.tsv", "OK"],
            ["object_keys_selected.tsv", "object_keys_selected.tsv", "OK"],
            ["progress.tsv", "progress.tsv", "OK"],
            ["results_days.tsv", "results_days.tsv", "OK"],
            ["cell_summary.tsv", "cell_summary.tsv", "OK"],
            ["stage1_topk.tsv", "stage1_topk.tsv", "OK"],
            ["determinism_compare.tsv", "determinism_compare.tsv", "OK"],
            ["campaign_meta.tsv", "campaign_meta.tsv", "OK"],
            ["sanity_dates_check.tsv", sanity_resolved, sanity_status],
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
        pack_name = f"multi-hypothesis-phase5-latency-leadlag-v1-bbo-fulldays-{ts}__resume{rix}"
    else:
        pack_name = f"multi-hypothesis-phase5-latency-leadlag-v1-bbo-fulldays-{ts}"

    state_json = Path(f"/tmp/quantlab_compacted_state_bbo_fulldays_{ts}.json")
    runner = StepRunner(REPO, NO_MAX_WALL_SECONDS)
    pack = runner.init_pack(pack_name)
    progress_path = pack.dir / "progress.tsv"
    ensure_progress_file(progress_path)

    resume_latest: Dict[Tuple[int, str, str], dict] = {}
    if resume_mode and old_pack_dir is not None:
        resume_latest = read_progress_latest(old_pack_dir / "progress.tsv")

    pre_note = (
        "Phase-5 latency_leadlag_v1 BBO full-days campaign.\n"
        "Goal: state-driven single-day jobs with ONvsON determinism and resilient resume.\n"
        "Smoke gate: first executed day must show event_count>0 and non-empty determinism hashes.\n"
        f"resume_mode={str(resume_mode).lower()} resume_from_pack={resume_from_pack or 'NONE'}\n"
    )
    write_text_file(pack.dir / "analysis" / "pre_note.txt", pre_note)

    runner.run_step(pack, "precheck", "test -x tools/slim_finalize.sh && test -x /tmp/s3_compact_tool.py && test -f tools/hypotheses/latency_leadlag_v1.py")
    runner.run_step(pack, "code_version", f"git rev-parse HEAD > {q(pack.dir / 'analysis' / 'code_version_ref.txt')}")
    runner.run_step(pack, "state_fetch", f"python3 /tmp/s3_compact_tool.py get quantlab-compact compacted/_state.json {q(state_json)}")

    result = "FAIL/STAGE1_NO_CANDIDATE"
    best_cell = "NONE"
    n_selected = 0
    n_completed = 0
    n_failed_oom = 0
    n_failed_other = 0
    n_skipped_resume = 0
    resume_no_new_jobs = False
    topk_inherited_from_pack = ""
    sanity_warn = False
    sanity_file_used = bool(str(args.sanity_dates_file).strip())
    smoke_gate_result = "SKIPPED_NO_EXEC"
    smoke_gate_candidate = "NONE"
    fatal_error = ""
    job_timeout_sec_value = max(1, int(args.job_timeout_sec))

    results_rows: List[List[object]] = []
    det_rows: List[List[object]] = []
    jobs: List[dict] = []

    try:
        state = read_state(state_json)
        meta = build_meta(state=state, exchanges=EXCHANGES, symbols=CORE_SYMBOLS)
        symbol_days = build_symbol_days(meta=meta, symbols=CORE_SYMBOLS, exchanges=EXCHANGES)
        rank_rows = rank_rows_for_proof(symbol_days)
        state_days = {d["date"] for sym in CORE_SYMBOLS for d in symbol_days.get(sym, [])}

        if sanity_file_used:
            sanity_dates = read_sanity_dates(Path(args.sanity_dates_file))
            sanity_warn = write_sanity_check(pack.dir, state_days=state_days, user_dates=sanity_dates)

        if resume_mode and old_pack_dir is not None:
            copy_cmd_parts = [
                f"if test -f {q(old_pack_dir / 'selected_days.tsv')}; then cp -f {q(old_pack_dir / 'selected_days.tsv')} {q(pack.dir / 'selected_days.tsv')}; fi",
                f"if test -f {q(old_pack_dir / 'object_keys_selected.tsv')}; then cp -f {q(old_pack_dir / 'object_keys_selected.tsv')} {q(pack.dir / 'object_keys_selected.tsv')}; fi",
                f"if test -f {q(old_pack_dir / 'selection_proof' / 'symbol_rankings.tsv')}; then mkdir -p {q(pack.dir / 'selection_proof')} && cp -f {q(old_pack_dir / 'selection_proof' / 'symbol_rankings.tsv')} {q(pack.dir / 'selection_proof' / 'symbol_rankings.tsv')}; fi",
            ]
            if sanity_file_used:
                copy_cmd_parts.append(
                    f"if test -f {q(old_pack_dir / 'sanity_dates_check.tsv')}; then cp -f {q(old_pack_dir / 'sanity_dates_check.tsv')} {q(pack.dir / 'sanity_dates_check.tsv')}; fi"
                )
            runner.run_step(pack, "resume_copy_selection_inputs", " && ".join(copy_cmd_parts))
            materialize_attempt_object_keys(pack.dir)
            jobs = load_jobs_from_pack_files(pack.dir)
            if args.max_jobs and args.max_jobs > 0:
                jobs = jobs[: int(args.max_jobs)]
            n_selected = len(jobs)
        else:
            selected = select_days(symbol_days=symbol_days, symbols_order=CORE_SYMBOLS, max_jobs=max(0, int(args.max_jobs)))
            if not selected:
                write_tsv(pack.dir / "selected_days.tsv", ["selection_rank", "symbol", "date", "day_quality", "rows_total_day"], [])
                write_tsv(
                    pack.dir / "object_keys_selected.tsv",
                    ["selection_rank", "job_id", "symbol", "date", "label", "exchange", "partition_key", "data_key", "meta_key", "bucket"],
                    [],
                )
                write_tsv(pack.dir / "selection_proof" / "symbol_rankings.tsv", ["symbol", "coverage_days_count", "rows_total_day_min"], [[r[0], r[1], r[2]] for r in rank_rows])
                write_empty_outputs(pack.dir)
                result = "FAIL/STAGE1_NO_CANDIDATE;detail=no_eligible_day"
            else:
                jobs = write_selection_proofs(pack_dir=pack.dir, selected=selected, rank_rows=rank_rows)
                n_selected = len(jobs)

        write_text_file(
            pack.dir / "analysis" / "selection_summary.json",
            json.dumps(
                {
                    "stream": STREAM,
                    "resume_mode": resume_mode,
                    "resume_from_pack": resume_from_pack,
                    "n_selected": n_selected,
                    "max_jobs": int(args.max_jobs),
                    "job_timeout_sec": job_timeout_sec_value,
                    "global_wall": "none",
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
        )

        selection_integrity_error = ""
        if jobs:
            selection_integrity_error = validate_selection_integrity(pack.dir, jobs)
        if selection_integrity_error:
            write_text_file(pack.dir / "analysis" / "selection_integrity_error.txt", selection_integrity_error + "\n")
            write_empty_outputs(pack.dir)
            result = "FAIL/STAGE1_NO_CANDIDATE;detail=selection_integrity_error"
            jobs = []

        print(
            f"PROGRESS stage=selection done selected={len(jobs)} resume_mode={str(resume_mode).lower()} "
            f"resume_from={resume_from_pack or 'NONE'}",
            flush=True,
        )

        if jobs:
            forced_oom_job = os.getenv("QLAB_TEST_FORCE_EXIT137_JOB", "").strip()
            last_progress_emit = dt.datetime.utcnow()
            consecutive_oom = 0
            smoke_checked = False
            peak_rss_so_far = 0

            for j in jobs:
                rank = int(j["selection_rank"])
                key = (rank, str(j["symbol"]), str(j["date"]))
                if key in resume_latest and resume_latest[key].get("status", "") in TERMINAL_SKIP_STATUSES:
                    n_skipped_resume += 1
                    append_progress_row(
                        progress_path=progress_path,
                        selection_rank=rank,
                        symbol=str(j["symbol"]),
                        date=str(j["date"]),
                        status="SKIPPED_RESUME",
                        exit_code=0,
                        elapsed_s=0.0,
                        max_rss_kb=0,
                        notes=f"resume_skip_from={resume_from_pack};old_status={resume_latest[key].get('status','')}",
                    )
                    continue

                run = run_one_job(
                    runner=runner,
                    pack=pack,
                    job=j,
                    timeout_s=job_timeout_sec_value,
                    forced_oom_job=forced_oom_job,
                )
                peak_rss_so_far = max(peak_rss_so_far, int(run.max_rss_kb))

                append_progress_row(
                    progress_path=progress_path,
                    selection_rank=rank,
                    symbol=str(j["symbol"]),
                    date=str(j["date"]),
                    status=run.status,
                    exit_code=run.exit_code,
                    elapsed_s=run.elapsed_s,
                    max_rss_kb=run.max_rss_kb,
                    notes=f"job_id={j['job_id']}",
                )

                if run.status == "DONE":
                    n_completed += 1
                    consecutive_oom = 0
                elif run.status == "FAILED_OOM":
                    n_failed_oom += 1
                    consecutive_oom += 1
                else:
                    n_failed_other += 1
                    consecutive_oom = 0

                det_rows.append(
                    [
                        rank,
                        j["job_id"],
                        j["symbol"],
                        j["date"],
                        run.primary_hash,
                        run.replay_hash,
                        run.determinism_status,
                        DETERMINISM_BASIS,
                    ]
                )
                results_rows.extend(rows_for_job(j, run))

                if not smoke_checked:
                    smoke_checked = True
                    smoke_gate_candidate = f"{j['symbol']}:{j['date']}"
                    if run.status != "DONE":
                        smoke_gate_result = f"FAIL_exec_status={run.status}"
                        result = "FAIL/SMOKE_SINGLE_DAY_INCOMPATIBLE;detail=execution_failed"
                        print(f"PROGRESS stage=smoke result={smoke_gate_result}", flush=True)
                        break
                    if not run.smoke_event_positive:
                        smoke_gate_result = "FAIL_event_count_zero"
                        result = "FAIL/SMOKE_SINGLE_DAY_INCOMPATIBLE;detail=event_count_zero"
                        print(f"PROGRESS stage=smoke result={smoke_gate_result}", flush=True)
                        break
                    if not run.primary_hash or not run.replay_hash:
                        smoke_gate_result = "FAIL_hash_missing"
                        result = "FAIL/SMOKE_SINGLE_DAY_INCOMPATIBLE;detail=hash_missing"
                        print(f"PROGRESS stage=smoke result={smoke_gate_result}", flush=True)
                        break
                    smoke_gate_result = "PASS"
                    print(f"PROGRESS stage=smoke result={smoke_gate_result} candidate={smoke_gate_candidate}", flush=True)

                now = dt.datetime.utcnow()
                if (now - last_progress_emit).total_seconds() >= max(10, int(args.progress_interval_sec)):
                    print(
                        f"PROGRESS stage=run done={n_completed} total={len(jobs)} failed_oom={n_failed_oom} failed_other={n_failed_other} "
                        f"skipped_resume={n_skipped_resume} last_job={j['job_id']} cumulative_wall_s={runner.cumulative_wall_s:.3f} "
                        f"peak_rss_so_far={peak_rss_so_far}",
                        flush=True,
                    )
                    last_progress_emit = now

                print(
                    f"PROGRESS stage=job_done job_id={j['job_id']} symbol={j['symbol']} date={j['date']} "
                    f"status={run.status} det={run.determinism_status}",
                    flush=True,
                )

                if consecutive_oom >= 2:
                    result = "FAIL/STAGE1_NO_CANDIDATE;detail=consecutive_oom_stop"
                    print(
                        f"PROGRESS stage=run stop_reason=consecutive_oom_2 last_job={j['job_id']} cum_wall_s={runner.cumulative_wall_s:.3f}",
                        flush=True,
                    )
                    break

        if not jobs:
            if not (pack.dir / "results_days.tsv").exists():
                write_empty_outputs(pack.dir)
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
                            f"if test -f {q(old_pack_dir / 'results_days.tsv')}; then cp -f {q(old_pack_dir / 'results_days.tsv')} {q(pack.dir / 'results_days.tsv')}; fi",
                            f"if test -f {q(old_pack_dir / 'determinism_compare.tsv')}; then cp -f {q(old_pack_dir / 'determinism_compare.tsv')} {q(pack.dir / 'determinism_compare.tsv')}; fi",
                        ]
                    ),
                )
                if not (pack.dir / "results_days.tsv").exists() or not (pack.dir / "determinism_compare.tsv").exists():
                    write_empty_outputs(pack.dir)
                topk_count, best_cell = read_topk_stats(pack.dir / "stage1_topk.tsv")
                if topk_count > 0:
                    result = f"PASS/STAGE1_TOPK_READY:{topk_count};detail=resume_no_new_jobs"
                else:
                    result = "FAIL/STAGE1_NO_CANDIDATE;detail=resume_no_new_jobs_missing_topk"
                    best_cell = "NONE"
                resume_no_new_jobs = True
                topk_inherited_from_pack = resume_from_pack
            else:
                write_tsv(
                    pack.dir / "determinism_compare.tsv",
                    ["selection_rank", "job_id", "symbol", "date", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
                    det_rows,
                )
                agg_result, agg_best = aggregate_stage1(pack.dir, results_rows, TOP_K)
                if not result.startswith("FAIL/SMOKE_SINGLE_DAY_INCOMPATIBLE"):
                    result = agg_result
                best_cell = agg_best
                if n_failed_oom > 0 or n_failed_other > 0:
                    if "detail=" not in result:
                        result = result + ";detail=partial_job_failures"

    except Exception as exc:
        fatal_error = f"{type(exc).__name__}:{exc}"
        if not (pack.dir / "selected_days.tsv").exists():
            write_tsv(pack.dir / "selected_days.tsv", ["selection_rank", "symbol", "date", "day_quality", "rows_total_day"], [])
        if not (pack.dir / "object_keys_selected.tsv").exists():
            write_tsv(
                pack.dir / "object_keys_selected.tsv",
                ["selection_rank", "job_id", "symbol", "date", "label", "exchange", "partition_key", "data_key", "meta_key", "bucket"],
                [],
            )
        write_empty_outputs(pack.dir)
        result = "FAIL/STAGE1_NO_CANDIDATE;detail=controller_exception"
        best_cell = "NONE"

    if fatal_error:
        write_text_file(pack.dir / "analysis" / "fatal_error.txt", fatal_error + "\n")

    write_text_file(pack.dir / "result.txt", result + "\n")
    write_campaign_meta(pack.dir)
    write_label_and_manifest(
        pack_dir=pack.dir,
        result=result,
        best_cell_id=best_cell,
        n_selected=len(jobs),
        n_completed=n_completed,
        n_failed_oom=n_failed_oom,
        n_failed_other=n_failed_other,
        n_skipped_resume=n_skipped_resume,
        resume_from_pack=resume_from_pack,
        resume_mode=resume_mode,
        resume_no_new_jobs=resume_no_new_jobs,
        topk_inherited_from_pack=topk_inherited_from_pack,
        smoke_gate_result=smoke_gate_result,
        smoke_gate_candidate=smoke_gate_candidate,
        sanity_warn=sanity_warn,
        sanity_file_used=sanity_file_used,
        job_timeout_sec=job_timeout_sec_value,
    )

    required = [
        "selected_days.tsv",
        "object_keys_selected.tsv",
        "progress.tsv",
        "results_days.tsv",
        "cell_summary.tsv",
        "stage1_topk.tsv",
        "determinism_compare.tsv",
        "campaign_meta.tsv",
        "result.txt",
        "label_report.txt",
        "artifact_manifest.tsv",
        "command_index.tsv",
        "time_v_summary.tsv",
    ]
    if sanity_file_used:
        required.append("sanity_dates_check.tsv")
    try:
        write_integrity(pack.dir, required)
    except Exception as exc:
        write_text_file(pack.dir / "analysis" / "integrity_error.txt", f"{type(exc).__name__}:{exc}\n")

    finalize_ok = True
    try:
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
    except Exception as exc:
        finalize_ok = False
        write_text_file(pack.dir / "analysis" / "finalize_error.txt", f"{type(exc).__name__}:{exc}\n")

    moved_to_file = REPO / "evidence" / f"{pack.name}.moved_to.txt"
    if moved_to_file.exists():
        moved_to = Path(moved_to_file.read_text(encoding="utf-8").strip())
        if moved_to.exists():
            final_result = (moved_to / "result.txt").read_text(encoding="utf-8").strip() if (moved_to / "result.txt").exists() else result
            print(f"PACK={pack.name}")
            print(f"MOVED_TO={moved_to}")
            print(f"FINAL_RESULT={final_result}")
            print(f"GLOBAL_CUM_WALL={runner.cumulative_wall_s:.6f}")

    return 0 if finalize_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
