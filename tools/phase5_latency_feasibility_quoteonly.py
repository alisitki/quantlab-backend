#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
import pyarrow.parquet as pq


REPO = Path("/home/deploy/quantlab-backend")
EXCHANGES = ["binance", "okx", "bybit"]
STREAM = "bbo"
CAMPAIGN_CATEGORY = "FULLSCAN"
CAMPAIGN_IMPORTANCE = "MAJOR"
ANALYSIS_KIND_V1 = "FEASIBILITY_QUOTEONLY"
ANALYSIS_KIND_V2 = "FEASIBILITY_QUOTEONLY_V2"
CAMPAIGN_KIND = "phase5_latency_leadlag_v1_bbo_feasibility_quoteonly"
ADVERSE_WINDOW_MS = 200
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 quote-only feasibility controller")
    p.add_argument("--source-fullscan-pack", default="")
    p.add_argument("--source-robust-pack", default="")
    p.add_argument("--execution-mode", choices=["taker_taker", "maker_entry_taker_exit"], default="taker_taker")
    p.add_argument("--taker-fee-bps", type=float, default=4.0)
    p.add_argument("--maker-fee-bps", type=float, default=0.0)
    p.add_argument("--progress-interval-sec", type=int, default=60)
    p.add_argument("--job-timeout-sec", type=int, default=1800)
    p.add_argument("--max-signals-per-day", type=int, default=20000)

    p.add_argument("--worker", action="store_true")
    p.add_argument("--worker-job-id", default="")
    p.add_argument("--worker-selection-rank", type=int, default=0)
    p.add_argument("--worker-symbol", default="")
    p.add_argument("--worker-date", default="")
    p.add_argument("--worker-object-keys-tsv", default="")
    p.add_argument("--worker-selected-cells-tsv", default="")
    p.add_argument("--worker-out-tsv", default="")
    p.add_argument("--worker-summary-json", default="")
    p.add_argument("--worker-downloads-dir", default="")
    p.add_argument("--worker-seed-rows-binance", type=int, default=-1)
    p.add_argument("--worker-seed-rows-okx", type=int, default=-1)
    p.add_argument("--worker-seed-rows-bybit", type=int, default=-1)
    return p.parse_args()


def write_tsv(path: Path, header: List[str], rows: Iterable[Iterable[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        for r in rows:
            w.writerow(list(r))


def append_tsv_row(path: Path, row: Iterable[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(list(row))


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def q(v: object) -> str:
    return shlex.quote(str(v))


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


def percentile_p10(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int((len(s) - 1) * 0.10)
    return s[idx]


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def normalize_pair(pair: str) -> str:
    return pair.strip().lower()


def parse_cell_id(cell_id: str) -> dict:
    m = re.match(r"^([^|]+)\|([^|]+)\|dt=([0-9]+)\|H=([0-9]+)\|tol=([0-9]+)$", cell_id.strip())
    if not m:
        raise ValueError(f"invalid_cell_id:{cell_id}")
    return {
        "stream": m.group(1),
        "pair": m.group(2),
        "dt_ms": int(m.group(3)),
        "h_ms": int(m.group(4)),
        "tol_ms": int(m.group(5)),
    }


def parse_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def resolve_pack_dir(raw: str) -> Path:
    p = Path(raw.strip())
    if p.is_dir():
        return p
    moved = REPO / "evidence" / f"{raw}.moved_to.txt"
    if moved.exists():
        target = Path(moved.read_text(encoding="utf-8").strip())
        if target.is_dir():
            return target
    raise RuntimeError(f"pack_not_found:{raw}")


def parse_object_key_rows(rows: List[Dict[str, str]], symbol: str, date: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for r in rows:
        if str(r.get("symbol", "")) != symbol:
            continue
        if str(r.get("date", "")) != date:
            continue
        if str(r.get("exchange", "")).strip().lower() not in EXCHANGES:
            continue
        out.append(r)
    out.sort(key=lambda x: (x["exchange"], x.get("data_key", "")))
    return out


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k and k not in os.environ:
            os.environ[k] = v


def make_s3_client() -> boto3.client:
    load_dotenv(REPO / ".env")
    kwargs = {
        "endpoint_url": os.getenv("S3_COMPACT_ENDPOINT") or None,
        "region_name": os.getenv("S3_COMPACT_REGION") or "us-east-1",
    }
    ak = os.getenv("S3_COMPACT_ACCESS_KEY")
    sk = os.getenv("S3_COMPACT_SECRET_KEY")
    if ak and sk:
        kwargs["aws_access_key_id"] = ak
        kwargs["aws_secret_access_key"] = sk
    return boto3.client("s3", **kwargs)


def download_day_parquets(rows: List[Dict[str, str]], downloads_dir: Path) -> Dict[str, Path]:
    s3 = make_s3_client()
    out: Dict[str, Path] = {}
    for r in rows:
        ex = str(r["exchange"]).strip().lower()
        bucket = str(r.get("bucket") or "quantlab-compact")
        data_key = str(r["data_key"])
        local = downloads_dir / f"exchange={ex}" / "data.parquet"
        local.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, data_key, str(local))
        if not local.exists() or local.stat().st_size <= 0:
            raise RuntimeError(f"download_failed_or_empty:{ex}:{data_key}")
        out[ex] = local
    return out


def load_exchange_bbo(path: Path) -> dict:
    ts_out: List[int] = []
    seq_out: List[int] = []
    bid_out: List[float] = []
    ask_out: List[float] = []
    mid_out: List[float] = []

    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(columns=["ts_event", "seq", "bid_price", "ask_price"], batch_size=131072):
        cols = batch.to_pydict()
        n = len(cols["ts_event"])
        for i in range(n):
            ts = cols["ts_event"][i]
            seq = cols["seq"][i]
            bid = cols["bid_price"][i]
            ask = cols["ask_price"][i]
            if ts is None or seq is None or bid is None or ask is None:
                continue
            bid_f = float(bid)
            ask_f = float(ask)
            if bid_f <= 0.0 or ask_f <= 0.0:
                continue
            ts_out.append(int(ts))
            seq_out.append(int(seq))
            bid_out.append(bid_f)
            ask_out.append(ask_f)
            mid_out.append((bid_f + ask_f) / 2.0)

    idx = list(range(len(ts_out)))
    idx.sort(key=lambda i: (ts_out[i], seq_out[i], i))
    ts_sorted = [ts_out[i] for i in idx]
    seq_sorted = [seq_out[i] for i in idx]
    bid_sorted = [bid_out[i] for i in idx]
    ask_sorted = [ask_out[i] for i in idx]
    mid_sorted = [mid_out[i] for i in idx]
    return {
        "ts": ts_sorted,
        "seq": seq_sorted,
        "bid": bid_sorted,
        "ask": ask_sorted,
        "mid": mid_sorted,
        "rows": len(ts_sorted),
    }


def nearest_index_within_tol(ts: List[int], seq: List[int], query_ts: int, tol_ms: int) -> int:
    n = len(ts)
    if n <= 0:
        return -1
    pos = bisect.bisect_left(ts, query_ts)
    cand: List[Tuple[int, int, int, int]] = []
    left = pos - 1
    right = pos
    if left >= 0:
        diff = abs(query_ts - ts[left])
        if diff <= tol_ms:
            cand.append((diff, ts[left], seq[left], left))
    if right < n:
        diff = abs(ts[right] - query_ts)
        if diff <= tol_ms:
            cand.append((diff, ts[right], seq[right], right))
    if not cand:
        return -1
    cand.sort()
    return cand[0][3]


def compute_one_cell(
    leader: dict,
    follower: dict,
    dt_ms: int,
    h_ms: int,
    tol_ms: int,
    execution_mode: str,
    taker_fee_bps: float,
    maker_fee_bps: float,
    max_signals_per_day: int,
) -> dict:
    l_ts = leader["ts"]
    l_seq = leader["seq"]
    l_mid = leader["mid"]
    f_ts = follower["ts"]
    f_seq = follower["seq"]
    f_bid = follower["bid"]
    f_ask = follower["ask"]
    f_mid = follower["mid"]

    signals_total = 0
    signals_matched = 0
    unmatched = 0
    cap_hit = False

    gross_vals: List[float] = []
    penalty_vals: List[float] = []
    net_vals: List[float] = []

    if execution_mode == "maker_entry_taker_exit":
        fee_bps_total = float(maker_fee_bps) + float(taker_fee_bps)
    else:
        fee_bps_total = float(taker_fee_bps) * 2.0
    n_leader = len(l_ts)
    if n_leader < 2 or len(f_ts) < 2:
        return {
            "signals_total": 0,
            "signals_matched": 0,
            "event_count": 0,
            "gross_mean_bps": 0.0,
            "gross_median_bps": 0.0,
            "gross_p10_bps": 0.0,
            "gross_min_bps": 0.0,
            "penalty_mean_bps": 0.0,
            "penalty_median_bps": 0.0,
            "penalty_p10_bps": 0.0,
            "penalty_max_bps": 0.0,
            "fee_bps_total": fee_bps_total,
            "net_mean_bps": 0.0,
            "net_median_bps": 0.0,
            "net_p10_bps": 0.0,
            "net_min_bps": 0.0,
            "pos_rate": 0.0,
            "unmatched": 0,
            "cap_hit": False,
        }

    for i in range(1, n_leader):
        prev_mid = l_mid[i - 1]
        cur_mid = l_mid[i]
        if prev_mid <= 0.0 or cur_mid <= 0.0:
            continue
        lr = math.log(cur_mid / prev_mid)
        if lr == 0.0:
            continue

        sign = 1 if lr > 0.0 else -1
        signals_total += 1
        t0 = l_ts[i]

        entry_idx = nearest_index_within_tol(f_ts, f_seq, t0 + dt_ms, tol_ms)
        if entry_idx < 0:
            unmatched += 1
            continue
        t_entry = f_ts[entry_idx]

        exit_idx = nearest_index_within_tol(f_ts, f_seq, t_entry + h_ms, tol_ms)
        if exit_idx < 0:
            unmatched += 1
            continue

        entry_bid = f_bid[entry_idx]
        entry_ask = f_ask[entry_idx]
        entry_mid = f_mid[entry_idx]
        exit_bid = f_bid[exit_idx]
        exit_ask = f_ask[exit_idx]
        if entry_bid <= 0.0 or entry_ask <= 0.0 or entry_mid <= 0.0 or exit_bid <= 0.0 or exit_ask <= 0.0:
            unmatched += 1
            continue

        if sign > 0:
            if execution_mode == "maker_entry_taker_exit":
                gross_bps = 10000.0 * math.log(exit_bid / entry_bid)
            else:
                gross_bps = 10000.0 * math.log(exit_bid / entry_ask)
        else:
            if execution_mode == "maker_entry_taker_exit":
                gross_bps = 10000.0 * math.log(entry_ask / exit_ask)
            else:
                gross_bps = 10000.0 * math.log(entry_bid / exit_ask)

        w_start = bisect.bisect_left(f_ts, t_entry)
        w_end = bisect.bisect_right(f_ts, t_entry + ADVERSE_WINDOW_MS)
        if w_start < w_end:
            mids = f_mid[w_start:w_end]
            min_mid = min(mids)
            max_mid = max(mids)
        else:
            min_mid = entry_mid
            max_mid = entry_mid

        if sign > 0:
            adverse = 10000.0 * math.log(entry_mid / min_mid) if min_mid > 0.0 else 0.0
        else:
            adverse = 10000.0 * math.log(max_mid / entry_mid) if entry_mid > 0.0 else 0.0
        penalty_bps = max(0.0, adverse)
        net_bps = gross_bps - penalty_bps - fee_bps_total

        gross_vals.append(gross_bps)
        penalty_vals.append(penalty_bps)
        net_vals.append(net_bps)
        signals_matched += 1

        if len(net_vals) >= max_signals_per_day:
            cap_hit = True
            break

    event_count = len(net_vals)
    pos_rate = (sum(1 for x in net_vals if x > 0.0) / event_count) if event_count else 0.0
    return {
        "signals_total": signals_total,
        "signals_matched": signals_matched,
        "event_count": event_count,
        "gross_mean_bps": (sum(gross_vals) / event_count) if event_count else 0.0,
        "gross_median_bps": median(gross_vals),
        "gross_p10_bps": percentile_p10(gross_vals),
        "gross_min_bps": min(gross_vals) if event_count else 0.0,
        "penalty_mean_bps": (sum(penalty_vals) / event_count) if event_count else 0.0,
        "penalty_median_bps": median(penalty_vals),
        "penalty_p10_bps": percentile_p10(penalty_vals),
        "penalty_max_bps": max(penalty_vals) if event_count else 0.0,
        "fee_bps_total": fee_bps_total,
        "net_mean_bps": (sum(net_vals) / event_count) if event_count else 0.0,
        "net_median_bps": median(net_vals),
        "net_p10_bps": percentile_p10(net_vals),
        "net_min_bps": min(net_vals) if event_count else 0.0,
        "pos_rate": pos_rate,
        "unmatched": unmatched,
        "cap_hit": cap_hit,
    }


def classify_failure(exit_code: int, stderr_text: str) -> str:
    if exit_code in {137, 9}:
        return "FAILED_OOM"
    if exit_code != 0 and "Killed" in stderr_text:
        return "FAILED_OOM"
    if exit_code != 0:
        return "FAILED_OTHER"
    return "DONE"


class StepRunner:
    def __init__(self, repo: Path) -> None:
        self.repo = repo
        self.cumulative_wall_s = 0.0
        self.peak_rss_kb = 0

    def init_pack(self, name: str) -> PackCtx:
        pack_dir = self.repo / "evidence" / name
        (pack_dir / "analysis").mkdir(parents=True, exist_ok=True)
        (pack_dir / "attempts").mkdir(parents=True, exist_ok=True)
        (pack_dir / "controller_commands").mkdir(parents=True, exist_ok=True)
        (pack_dir / "finalize").mkdir(parents=True, exist_ok=True)

        cmd_index = pack_dir / "command_index.tsv"
        time_summary = pack_dir / "time_v_summary.tsv"
        cmd_index.write_text(
            "step\texit_code\tmax_rss_kb\telapsed_s\tcumulative_wall_s\tcmd_relpath\tstdout_relpath\tstderr_relpath\ttime_v_relpath\texit_relpath\n",
            encoding="utf-8",
        )
        time_summary.write_text(
            "step\texit_code\telapsed_s\tmax_rss_kb\tcumulative_wall_s\n",
            encoding="utf-8",
        )
        return PackCtx(name=name, dir=pack_dir, cmd_index=cmd_index, time_summary=time_summary)

    def _refresh_ctx_if_moved(self, ctx: PackCtx) -> None:
        moved = self.repo / "evidence" / f"{ctx.name}.moved_to.txt"
        if not moved.exists():
            return
        target = Path(moved.read_text(encoding="utf-8").strip())
        if target.exists() and target.is_dir() and target != ctx.dir:
            ctx.dir = target
            ctx.cmd_index = target / "command_index.tsv"
            ctx.time_summary = target / "time_v_summary.tsv"

    def run_step_capture(self, ctx: PackCtx, step: str, cmd: str) -> StepCapture:
        self._refresh_ctx_if_moved(ctx)
        step_dir = ctx.dir / "controller_commands" / step
        step_dir.mkdir(parents=True, exist_ok=True)

        cmdf = step_dir / "cmd.sh"
        outf = step_dir / "stdout.log"
        errf = step_dir / "stderr.log"
        timef = step_dir / "time-v.log"
        exitf = step_dir / "exit_code.txt"

        script = "set -euo pipefail\n" + cmd + "\n"
        cmdf.write_text(script, encoding="utf-8")
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
        tv = timef.read_text(encoding="utf-8", errors="replace") if timef.exists() else ""
        elapsed = parse_elapsed_seconds(tv)
        rss = parse_max_rss_kb(tv)
        self.cumulative_wall_s += elapsed
        self.peak_rss_kb = max(self.peak_rss_kb, rss)

        def rel(p: Path) -> str:
            try:
                return str(p.relative_to(ctx.dir))
            except Exception:
                return str(p)

        append_tsv_row(
            ctx.cmd_index,
            [step, ec, rss, f"{elapsed:.6f}", f"{self.cumulative_wall_s:.6f}", rel(cmdf), rel(outf), rel(errf), rel(timef), rel(exitf)],
        )
        append_tsv_row(ctx.time_summary, [step, ec, f"{elapsed:.6f}", rss, f"{self.cumulative_wall_s:.6f}"])
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
        cap = self.run_step_capture(ctx=ctx, step=step, cmd=cmd)
        if cap.exit_code != 0:
            raise RuntimeError(f"step_failed:{step}:exit={cap.exit_code}")


def worker_main(args: argparse.Namespace) -> int:
    job_id = str(args.worker_job_id)
    symbol = str(args.worker_symbol)
    date = str(args.worker_date)
    if not job_id or not symbol or not date:
        raise SystemExit("worker args missing")

    object_rows = parse_tsv(Path(args.worker_object_keys_tsv))
    cell_rows = parse_tsv(Path(args.worker_selected_cells_tsv))
    downloads_dir = Path(args.worker_downloads_dir)
    out_tsv = Path(args.worker_out_tsv)
    out_summary = Path(args.worker_summary_json)

    job_keys = parse_object_key_rows(object_rows, symbol=symbol, date=date)
    object_keys_count = len(job_keys)
    by_ex = {str(r["exchange"]).strip().lower() for r in job_keys}
    coverage_pass = object_keys_count == 3 and by_ex == set(EXCHANGES)

    out_rows: List[List[object]] = []
    rows_loaded = {ex: 0 for ex in EXCHANGES}
    seed_rows = {
        "binance": int(args.worker_seed_rows_binance),
        "okx": int(args.worker_seed_rows_okx),
        "bybit": int(args.worker_seed_rows_bybit),
    }
    seed_available = all(seed_rows.get(ex, -1) >= 0 for ex in EXCHANGES)
    notes_global: List[str] = []

    try:
        downloaded = {}
        if coverage_pass:
            downloaded = download_day_parquets(job_keys, downloads_dir=downloads_dir)
            events_by_ex = {}
            for ex in EXCHANGES:
                ev = load_exchange_bbo(downloaded[ex])
                events_by_ex[ex] = ev
                rows_loaded[ex] = int(ev["rows"])
        else:
            events_by_ex = {}

        for c in cell_rows:
            cell_id = str(c.get("cell_id", "")).strip()
            pair = normalize_pair(str(c.get("pair", "")))
            dt_ms = int(c.get("dt_ms", "0"))
            h_ms = int(c.get("h_ms", "0"))
            tol_ms = int(c.get("tol_ms", "20"))
            metric_kind = str(c.get("metric_kind", "log_return"))
            value_def = str(c.get("value_def", "mid"))

            if metric_kind != "log_return" or value_def != "mid":
                raise RuntimeError(f"unsupported_metric_contract:{metric_kind}:{value_def}")
            if "->" not in pair:
                raise RuntimeError(f"invalid_pair:{pair}")
            leader_ex, follower_ex = pair.split("->", 1)
            leader_ex = leader_ex.strip().lower()
            follower_ex = follower_ex.strip().lower()

            support_rows = seed_rows if seed_available else rows_loaded
            support_rows_source = "source_results_days" if seed_available else "parquet_count"
            support_pass = coverage_pass and all(int(support_rows.get(ex, 0)) >= 200 for ex in EXCHANGES)
            if not support_pass:
                day_label = "INSUFFICIENT_SUPPORT"
                notes = f"support_guard_failed;rows_source={support_rows_source}"
                out_rows.append(
                    [
                        args.worker_selection_rank,
                        job_id,
                        symbol,
                        date,
                        cell_id,
                        pair,
                        dt_ms,
                        h_ms,
                        tol_ms,
                        object_keys_count,
                        support_rows["binance"],
                        support_rows["okx"],
                        support_rows["bybit"],
                        "false",
                        0,
                        0,
                        0,
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        f"{(float(args.maker_fee_bps) + float(args.taker_fee_bps)) if args.execution_mode == 'maker_entry_taker_exit' else (float(args.taker_fee_bps) * 2.0):.15f}",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        "0.000000000000000",
                        day_label,
                        notes,
                    ]
                )
                continue

            metrics = compute_one_cell(
                leader=events_by_ex[leader_ex],
                follower=events_by_ex[follower_ex],
                dt_ms=dt_ms,
                h_ms=h_ms,
                tol_ms=tol_ms,
                execution_mode=str(args.execution_mode),
                taker_fee_bps=float(args.taker_fee_bps),
                maker_fee_bps=float(args.maker_fee_bps),
                max_signals_per_day=max(1, int(args.max_signals_per_day)),
            )
            day_label = "OK"
            notes = f"rows_source={support_rows_source}"
            if metrics["event_count"] == 0:
                day_label = "NO_MATCHED_SIGNALS"
                notes = notes + ";matched_zero"
            elif metrics["cap_hit"]:
                notes = notes + ";max_signals_cap_hit"

            out_rows.append(
                [
                    args.worker_selection_rank,
                    job_id,
                    symbol,
                    date,
                    cell_id,
                    pair,
                    dt_ms,
                    h_ms,
                    tol_ms,
                    object_keys_count,
                    support_rows["binance"],
                    support_rows["okx"],
                    support_rows["bybit"],
                    "true",
                    int(metrics["signals_total"]),
                    int(metrics["signals_matched"]),
                    int(metrics["event_count"]),
                    f"{float(metrics['gross_mean_bps']):.15f}",
                    f"{float(metrics['gross_median_bps']):.15f}",
                    f"{float(metrics['gross_p10_bps']):.15f}",
                    f"{float(metrics['gross_min_bps']):.15f}",
                    f"{float(metrics['penalty_mean_bps']):.15f}",
                    f"{float(metrics['penalty_median_bps']):.15f}",
                    f"{float(metrics['penalty_p10_bps']):.15f}",
                    f"{float(metrics['penalty_max_bps']):.15f}",
                    f"{float(metrics['fee_bps_total']):.15f}",
                    f"{float(metrics['net_mean_bps']):.15f}",
                    f"{float(metrics['net_median_bps']):.15f}",
                    f"{float(metrics['net_p10_bps']):.15f}",
                    f"{float(metrics['net_min_bps']):.15f}",
                    f"{float(metrics['pos_rate']):.15f}",
                    day_label,
                    notes,
                ]
            )

        write_tsv(
            out_tsv,
            [
                "selection_rank",
                "job_id",
                "symbol",
                "date",
                "cell_id",
                "pair",
                "dt_ms",
                "h_ms",
                "tol_ms",
                "object_keys_count",
                "rows_binance",
                "rows_okx",
                "rows_bybit",
                "support_pass",
                "signals_total",
                "signals_matched",
                "event_count",
                "gross_mean_bps",
                "gross_median_bps",
                "gross_p10_bps",
                "gross_min_bps",
                "penalty_mean_bps",
                "penalty_median_bps",
                "penalty_p10_bps",
                "penalty_max_bps",
                "fee_bps_total",
                "net_mean_bps",
                "net_median_bps",
                "net_p10_bps",
                "net_min_bps",
                "pos_rate",
                "day_label",
                "notes",
            ],
            out_rows,
        )
        summary = {
            "job_id": job_id,
            "symbol": symbol,
            "date": date,
            "object_keys_count": object_keys_count,
            "coverage_pass": coverage_pass,
            "rows_loaded_by_exchange": rows_loaded,
            "row_count_out": len(out_rows),
            "notes": ";".join(notes_global),
        }
        write_text(out_summary, json.dumps(summary, indent=2, ensure_ascii=True) + "\n")
        return 0
    finally:
        shutil.rmtree(downloads_dir, ignore_errors=True)


def read_progress_latest(path: Path) -> Dict[Tuple[int, str, str], dict]:
    out: Dict[Tuple[int, str, str], dict] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (int(r["selection_rank"]), r["symbol"], r["date"])
            out[key] = r
    return out


def ensure_progress_file(path: Path) -> None:
    if path.exists():
        return
    write_tsv(path, PROGRESS_HEADER, [])


def append_progress(
    path: Path,
    selection_rank: int,
    symbol: str,
    date: str,
    status: str,
    exit_code: int,
    elapsed_s: float,
    max_rss_kb: int,
    notes: str,
) -> None:
    ensure_progress_file(path)
    append_tsv_row(
        path,
        [
            selection_rank,
            symbol,
            date,
            status,
            exit_code,
            f"{elapsed_s:.6f}",
            int(max_rss_kb),
            notes,
        ],
    )


def parse_selected_cells(robust_pack: Path) -> List[dict]:
    robust_file = robust_pack / "cell_summary_robust.tsv"
    out: List[dict] = []
    if robust_file.exists():
        rows = parse_tsv(robust_file)
        for r in rows:
            if str(r.get("cell_class_robust", "")).strip() != "ROBUST":
                continue
            cell_id = str(r["cell_id"]).strip()
            parsed = parse_cell_id(cell_id)
            if parsed["stream"] != STREAM:
                continue
            pair = normalize_pair(parsed["pair"])
            out.append(
                {
                    "cell_id": cell_id,
                    "stream": STREAM,
                    "pair": pair,
                    "leader_exchange": pair.split("->", 1)[0],
                    "follower_exchange": pair.split("->", 1)[1],
                    "dt_ms": parsed["dt_ms"],
                    "h_ms": parsed["h_ms"],
                    "tol_ms": parsed["tol_ms"],
                    "metric_kind": "log_return",
                    "value_def": "mid",
                    "selection_source": "cell_summary_robust.tsv",
                }
            )
    if out:
        out.sort(key=lambda x: x["cell_id"])
        return out

    label = robust_pack / "label_report.txt"
    if not label.exists():
        raise RuntimeError("missing_robust_selection_source")
    best_id = ""
    for line in label.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("robust_best_cell_id="):
            best_id = line.split("=", 1)[1].strip()
            break
    if not best_id:
        raise RuntimeError("robust_best_cell_missing")
    parsed = parse_cell_id(best_id)
    pair = normalize_pair(parsed["pair"])
    return [
        {
            "cell_id": best_id,
            "stream": STREAM,
            "pair": pair,
            "leader_exchange": pair.split("->", 1)[0],
            "follower_exchange": pair.split("->", 1)[1],
            "dt_ms": parsed["dt_ms"],
            "h_ms": parsed["h_ms"],
            "tol_ms": parsed["tol_ms"],
            "metric_kind": "log_return",
            "value_def": "mid",
            "selection_source": "label_report.robust_best_cell_id",
        }
    ]


def build_jobs(selected_days: Path, object_keys: Path) -> List[dict]:
    days_rows = parse_tsv(selected_days)
    obj_rows = parse_tsv(object_keys)
    by_job: Dict[str, List[Dict[str, str]]] = {}
    for r in obj_rows:
        by_job.setdefault(str(r["job_id"]), []).append(r)
    jobs: List[dict] = []
    for r in days_rows:
        rank = int(r["selection_rank"])
        jid = f"job_{rank:03d}"
        jobs.append(
            {
                "selection_rank": rank,
                "job_id": jid,
                "symbol": str(r["symbol"]),
                "date": str(r["date"]),
                "day_quality": str(r.get("day_quality", "")),
                "rows_total_day": int(r.get("rows_total_day") or 0),
                "object_rows": sorted(by_job.get(jid, []), key=lambda x: (x.get("exchange", ""), x.get("data_key", ""))),
            }
        )
    jobs.sort(key=lambda x: int(x["selection_rank"]))
    return jobs


def build_source_rows_seed_map(source_results_days: Path) -> Dict[Tuple[int, str, str], Dict[str, int]]:
    out: Dict[Tuple[int, str, str], Dict[str, int]] = {}
    if not source_results_days.exists():
        return out
    rows = parse_tsv(source_results_days)
    if not rows:
        return out
    if "rows_binance" not in rows[0] or "rows_okx" not in rows[0] or "rows_bybit" not in rows[0]:
        return out
    for r in rows:
        try:
            key = (int(r["selection_rank"]), str(r["symbol"]), str(r["date"]))
            rb = int(float(r.get("rows_binance") or 0))
            ro = int(float(r.get("rows_okx") or 0))
            ry = int(float(r.get("rows_bybit") or 0))
            out[key] = {"binance": rb, "okx": ro, "bybit": ry}
        except Exception:
            continue
    return out


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def write_label_report(
    pack_dir: Path,
    result: str,
    best_cell: str,
    selected_cells_count: int,
    selected_days_count: int,
    n_done: int,
    n_failed_oom: int,
    n_failed_other: int,
    source_fullscan_pack: str,
    source_robust_pack: str,
    analysis_kind: str,
    execution_mode: str,
    maker_fee_bps: float,
    taker_fee_bps: float,
) -> None:
    code_ref = (pack_dir / "analysis" / "code_version_ref.txt").read_text(encoding="utf-8").strip()
    label = result.split(":", 1)[0]
    lines = [
        f"label={label}",
        "artifact_manifest=source_packs.tsv,selected_cells.tsv,selected_days.tsv,object_keys_selected.tsv,progress.tsv,per_day_results.tsv,cell_summary_feasibility.tsv,label_report",
        (
            "decision_inputs="
            f"selected_cells={selected_cells_count},selected_days={selected_days_count},done={n_done},"
            f"failed_oom={n_failed_oom},failed_other={n_failed_other},best_cell={best_cell}"
        ),
        f"final_result={result}",
        f"campaign_category={CAMPAIGN_CATEGORY}",
        f"campaign_importance={CAMPAIGN_IMPORTANCE}",
        f"analysis_kind={analysis_kind}",
        f"execution_mode={execution_mode}",
        f"fee_model=maker_fee_bps={maker_fee_bps};taker_fee_bps={taker_fee_bps}",
        f"source_fullscan_pack={source_fullscan_pack}",
        f"source_robust_pack={source_robust_pack}",
        f"run_id={pack_dir.name}",
        f"code_version_ref={code_ref}",
        "scope_guard=No new fields beyond listed",
        "run_trace=no_reruns=true;analysis_only=true",
    ]
    write_text(pack_dir / "label_report.txt", "\n".join(lines) + "\n")


def write_artifact_manifest(pack_dir: Path) -> None:
    write_tsv(
        pack_dir / "artifact_manifest.tsv",
        ["expected_relpath", "resolved_relpath", "status"],
        [
            ["source_packs.tsv", "source_packs.tsv", "OK"],
            ["selected_cells.tsv", "selected_cells.tsv", "OK"],
            ["selected_days.tsv", "selected_days.tsv", "OK"],
            ["object_keys_selected.tsv", "object_keys_selected.tsv", "OK"],
            ["progress.tsv", "progress.tsv", "OK"],
            ["per_day_results.tsv", "per_day_results.tsv", "OK"],
            ["cell_summary_feasibility.tsv", "cell_summary_feasibility.tsv", "OK"],
            ["campaign_meta.tsv", "campaign_meta.tsv", "OK"],
            ["old_pack_unchanged.txt", "old_pack_unchanged.txt", "OK"],
            ["result.txt", "result.txt", "OK"],
            ["label_report.txt", "label_report.txt", "OK"],
            ["artifact_manifest.tsv", "artifact_manifest.tsv", "OK"],
            ["command_index.tsv", "command_index.tsv", "OK"],
            ["time_v_summary.tsv", "time_v_summary.tsv", "OK"],
        ],
    )


def write_integrity(pack_dir: Path, required: List[str]) -> None:
    missing = [p for p in required if not (pack_dir / p).exists()]
    write_text(pack_dir / "integrity_check.txt", "missing_count=" + str(len(missing)) + "\n" + "\n".join(missing) + "\n")
    if missing:
        raise RuntimeError(f"missing_artifacts:{','.join(missing)}")


def controller_main(args: argparse.Namespace) -> int:
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    analysis_kind = ANALYSIS_KIND_V2 if str(args.execution_mode) == "maker_entry_taker_exit" else ANALYSIS_KIND_V1
    pack_name = f"multi-hypothesis-phase5-latency-leadlag-v1-bbo-feasibility-quoteonly-v2-{ts}__FULLSCAN_MAJOR"
    archive_root = Path(f"/home/deploy/quantlab-evidence-archive/{dt.datetime.utcnow().strftime('%Y%m%d')}_slim")
    runner = StepRunner(REPO)
    pack = runner.init_pack(pack_name)

    if not str(args.source_fullscan_pack).strip() or not str(args.source_robust_pack).strip():
        raise RuntimeError("controller_requires_source_packs")
    source_fullscan = resolve_pack_dir(args.source_fullscan_pack)
    source_robust = resolve_pack_dir(args.source_robust_pack)

    required_src = [
        source_fullscan / "selected_days.tsv",
        source_fullscan / "object_keys_selected.tsv",
        source_fullscan / "results_days.tsv",
        source_robust / "label_report.txt",
    ]
    for p in required_src:
        if not p.exists():
            raise RuntimeError(f"missing_source:{p}")

    full_tar = REPO / "evidence" / f"{source_fullscan.name}.tar.gz"
    robust_tar = REPO / "evidence" / f"{source_robust.name}.tar.gz"
    if not full_tar.exists() or not robust_tar.exists():
        raise RuntimeError("missing_source_tar_for_unchanged_proof")
    full_sha_before = sha256_file(full_tar)
    robust_sha_before = sha256_file(robust_tar)

    write_text(
        pack.dir / "analysis" / "pre_note.txt",
        (
            "Phase-5 quote-only feasibility analysis for latency_leadlag_v1 robust cells.\n"
            "No rerun of fullscan/robust campaigns; source packs are read-only.\n"
            f"DONE = new FULLSCAN_MAJOR SLIM pack with feasibility summary + sha verify OK. execution_mode={args.execution_mode}\n"
        ),
    )

    runner.run_step(
        pack,
        "precheck",
        " && ".join(
            [
                "test -x tools/slim_finalize.sh",
                f"test -d {q(source_fullscan)}",
                f"test -d {q(source_robust)}",
                f"test -f {q(source_fullscan / 'selected_days.tsv')}",
                f"test -f {q(source_fullscan / 'object_keys_selected.tsv')}",
                f"test -f {q(source_robust / 'label_report.txt')}",
            ]
        ),
    )
    runner.run_step(pack, "code_version", f"git rev-parse HEAD > {q(pack.dir / 'analysis' / 'code_version_ref.txt')}")
    runner.run_step(
        pack,
        "copy_sources",
        " && ".join(
            [
                f"cp -f {q(source_fullscan / 'selected_days.tsv')} {q(pack.dir / 'selected_days.tsv')}",
                f"cp -f {q(source_fullscan / 'object_keys_selected.tsv')} {q(pack.dir / 'object_keys_selected.tsv')}",
            ]
        ),
    )

    selected_cells = parse_selected_cells(source_robust)
    write_tsv(
        pack.dir / "selected_cells.tsv",
        ["cell_id", "stream", "pair", "leader_exchange", "follower_exchange", "dt_ms", "h_ms", "tol_ms", "metric_kind", "value_def", "selection_source"],
        [
            [
                r["cell_id"],
                r["stream"],
                r["pair"],
                r["leader_exchange"],
                r["follower_exchange"],
                r["dt_ms"],
                r["h_ms"],
                r["tol_ms"],
                r["metric_kind"],
                r["value_def"],
                r["selection_source"],
            ]
            for r in selected_cells
        ],
    )

    write_tsv(
        pack.dir / "source_packs.tsv",
        ["pack_role", "path", "pack_name"],
        [
            ["fullscan", str(source_fullscan), source_fullscan.name],
            ["robust", str(source_robust), source_robust.name],
        ],
    )

    write_tsv(
        pack.dir / "campaign_meta.tsv",
        ["key", "value"],
        [
            ["campaign_category", CAMPAIGN_CATEGORY],
            ["campaign_importance", CAMPAIGN_IMPORTANCE],
            ["analysis_kind", analysis_kind],
            ["execution_mode", str(args.execution_mode)],
            ["fee_model", f"maker_fee_bps={float(args.maker_fee_bps)};taker_fee_bps={float(args.taker_fee_bps)}"],
            ["campaign_kind", CAMPAIGN_KIND],
            ["source_fullscan_pack", source_fullscan.name],
            ["source_robust_pack", source_robust.name],
            ["created_at_utc", dt.datetime.utcnow().isoformat() + "Z"],
        ],
    )

    per_day_results_path = pack.dir / "per_day_results.tsv"
    write_tsv(
        per_day_results_path,
        [
            "selection_rank",
            "job_id",
            "symbol",
            "date",
            "cell_id",
            "pair",
            "dt_ms",
            "h_ms",
            "tol_ms",
            "object_keys_count",
            "rows_binance",
            "rows_okx",
            "rows_bybit",
            "support_pass",
            "signals_total",
            "signals_matched",
            "event_count",
            "gross_mean_bps",
            "gross_median_bps",
            "gross_p10_bps",
            "gross_min_bps",
            "penalty_mean_bps",
            "penalty_median_bps",
            "penalty_p10_bps",
            "penalty_max_bps",
            "fee_bps_total",
            "net_mean_bps",
            "net_median_bps",
            "net_p10_bps",
            "net_min_bps",
            "pos_rate",
            "day_label",
            "notes",
        ],
        [],
    )
    progress_path = pack.dir / "progress.tsv"
    ensure_progress_file(progress_path)

    jobs = build_jobs(pack.dir / "selected_days.tsv", pack.dir / "object_keys_selected.tsv")
    source_rows_seed_map = build_source_rows_seed_map(source_fullscan / "results_days.tsv")
    n_done = 0
    n_failed_oom = 0
    n_failed_other = 0
    last_emit = dt.datetime.utcnow()

    for job in jobs:
        rank = int(job["selection_rank"])
        jid = str(job["job_id"])
        symbol = str(job["symbol"])
        date = str(job["date"])

        attempt_dir = pack.dir / "attempts" / jid
        attempt_dir.mkdir(parents=True, exist_ok=True)
        worker_keys_tsv = attempt_dir / "object_keys_day.tsv"
        write_tsv(
            worker_keys_tsv,
            ["selection_rank", "job_id", "symbol", "date", "label", "exchange", "partition_key", "data_key", "meta_key", "bucket"],
            [
                [
                    r.get("selection_rank", rank),
                    r.get("job_id", jid),
                    r.get("symbol", symbol),
                    r.get("date", date),
                    r.get("label", "day"),
                    r.get("exchange", ""),
                    r.get("partition_key", ""),
                    r.get("data_key", ""),
                    r.get("meta_key", ""),
                    r.get("bucket", "quantlab-compact"),
                ]
                for r in job["object_rows"]
            ],
        )

        step = f"job_{jid}"
        cmd = (
            f"timeout {int(max(1, args.job_timeout_sec))}s "
            "python3 tools/phase5_latency_feasibility_quoteonly.py --worker "
            f"--worker-job-id {q(jid)} "
            f"--worker-selection-rank {q(rank)} "
            f"--worker-symbol {q(symbol)} "
            f"--worker-date {q(date)} "
            f"--worker-object-keys-tsv {q(worker_keys_tsv)} "
            f"--worker-selected-cells-tsv {q(pack.dir / 'selected_cells.tsv')} "
            f"--worker-out-tsv {q(attempt_dir / 'worker_per_day.tsv')} "
            f"--worker-summary-json {q(attempt_dir / 'worker_summary.json')} "
            f"--worker-downloads-dir {q(attempt_dir / 'downloads')} "
            f"--taker-fee-bps {q(args.taker_fee_bps)} "
            f"--maker-fee-bps {q(args.maker_fee_bps)} "
            f"--execution-mode {q(args.execution_mode)} "
            f"--worker-seed-rows-binance {q(source_rows_seed_map.get((rank, symbol, date), {}).get('binance', -1))} "
            f"--worker-seed-rows-okx {q(source_rows_seed_map.get((rank, symbol, date), {}).get('okx', -1))} "
            f"--worker-seed-rows-bybit {q(source_rows_seed_map.get((rank, symbol, date), {}).get('bybit', -1))} "
            f"--max-signals-per-day {q(int(max(1, args.max_signals_per_day)))}"
        )
        cap = runner.run_step_capture(pack, step, cmd)
        stderr_text = cap.stderr_path.read_text(encoding="utf-8", errors="replace") if cap.stderr_path.exists() else ""
        status = classify_failure(cap.exit_code, stderr_text)

        if status == "DONE":
            out_file = attempt_dir / "worker_per_day.tsv"
            if not out_file.exists():
                status = "FAILED_OTHER"
            else:
                rows = parse_tsv(out_file)
                for r in rows:
                    append_tsv_row(
                        per_day_results_path,
                        [
                            r["selection_rank"],
                            r["job_id"],
                            r["symbol"],
                            r["date"],
                            r["cell_id"],
                            r["pair"],
                            r["dt_ms"],
                            r["h_ms"],
                            r["tol_ms"],
                            r["object_keys_count"],
                            r["rows_binance"],
                            r["rows_okx"],
                            r["rows_bybit"],
                            r["support_pass"],
                            r["signals_total"],
                            r["signals_matched"],
                            r["event_count"],
                            r["gross_mean_bps"],
                            r["gross_median_bps"],
                            r["gross_p10_bps"],
                            r["gross_min_bps"],
                            r["penalty_mean_bps"],
                            r["penalty_median_bps"],
                            r["penalty_p10_bps"],
                            r["penalty_max_bps"],
                            r["fee_bps_total"],
                            r["net_mean_bps"],
                            r["net_median_bps"],
                            r["net_p10_bps"],
                            r["net_min_bps"],
                            r["pos_rate"],
                            r["day_label"],
                            r["notes"],
                        ],
                    )

        if status == "DONE":
            n_done += 1
        elif status == "FAILED_OOM":
            n_failed_oom += 1
        else:
            n_failed_other += 1

        append_progress(
            progress_path,
            selection_rank=rank,
            symbol=symbol,
            date=date,
            status=status,
            exit_code=cap.exit_code,
            elapsed_s=cap.elapsed_s,
            max_rss_kb=cap.max_rss_kb,
            notes=f"job_id={jid}",
        )
        runner.run_step_capture(pack, f"cleanup_{jid}", f"rm -rf {q(attempt_dir / 'downloads')} || true")

        now = dt.datetime.utcnow()
        if (now - last_emit).total_seconds() >= max(10, int(args.progress_interval_sec)):
            print(
                f"PROGRESS stage=run done={n_done} total={len(jobs)} failed_oom={n_failed_oom} failed_other={n_failed_other} "
                f"last_job={jid} cumulative_wall_s={runner.cumulative_wall_s:.3f} peak_rss_so_far={runner.peak_rss_kb}",
                flush=True,
            )
            last_emit = now
        print(
            f"PROGRESS stage=job_done job_id={jid} symbol={symbol} date={date} status={status}",
            flush=True,
        )

    per_rows = parse_tsv(per_day_results_path)
    by_cell: Dict[str, List[Dict[str, str]]] = {}
    for r in per_rows:
        by_cell.setdefault(str(r["cell_id"]), []).append(r)

    summary_rows: List[List[object]] = []
    best_cell = "NONE"
    best_tuple: Optional[Tuple[float, float, float, int, str]] = None
    robust_found = False

    for cell in sorted(selected_cells, key=lambda x: x["cell_id"]):
        cid = str(cell["cell_id"])
        rows = by_cell.get(cid, [])
        n_total = len(rows)
        support_rows = [r for r in rows if str(r["support_pass"]).lower() == "true"]
        computed_rows = [r for r in support_rows if int(r.get("event_count") or 0) > 0]

        net_vals = [float(r["net_mean_bps"]) for r in computed_rows]
        gross_vals = [float(r["gross_mean_bps"]) for r in computed_rows]
        pen_vals = [float(r["penalty_mean_bps"]) for r in computed_rows]
        n_actual = len(computed_rows)

        med_net = median(net_vals)
        p10_net = percentile_p10(net_vals)
        min_net = min(net_vals) if net_vals else 0.0
        pos_rate = (sum(1 for x in net_vals if x > 0.0) / n_actual) if n_actual else 0.0
        med_gross = median(gross_vals)
        med_pen = median(pen_vals)
        fee_total = (float(args.maker_fee_bps) + float(args.taker_fee_bps)) if str(args.execution_mode) == "maker_entry_taker_exit" else (float(args.taker_fee_bps) * 2.0)

        feasible = n_actual >= 12 and med_net > 0.0 and p10_net > 0.0 and pos_rate >= 0.60
        if feasible:
            robust_found = True
            flabel = "FEASIBLE"
            reason = "FEASIBLE_PASS"
        else:
            flabel = "NOT_FEASIBLE"
            reason_parts = []
            if n_actual < 12:
                reason_parts.append("N<12")
            if med_net <= 0.0:
                reason_parts.append("median_net<=0")
            if p10_net <= 0.0:
                reason_parts.append("p10_net<=0")
            if pos_rate < 0.60:
                reason_parts.append("pos_rate<0.60")
            reason = "|".join(reason_parts) if reason_parts else "NO_DATA"

        summary_rows.append(
            [
                cid,
                cell["pair"],
                cell["dt_ms"],
                cell["h_ms"],
                cell["tol_ms"],
                n_total,
                len(support_rows),
                n_actual,
                f"{med_net:.15f}",
                f"{p10_net:.15f}",
                f"{min_net:.15f}",
                f"{pos_rate:.6f}",
                f"{med_gross:.15f}",
                f"{med_pen:.15f}",
                f"{fee_total:.15f}",
                flabel,
                reason,
            ]
        )
        rank_key = (med_net, p10_net, pos_rate, n_actual, cid)
        if best_tuple is None or rank_key > best_tuple:
            best_tuple = rank_key
            best_cell = cid

    summary_rows.sort(
        key=lambda r: (
            0 if str(r[15]) == "FEASIBLE" else 1,
            -float(r[8]),
            -float(r[9]),
            -float(r[11]),
            str(r[0]),
        )
    )
    write_tsv(
        pack.dir / "cell_summary_feasibility.tsv",
        [
            "cell_id",
            "pair",
            "dt_ms",
            "h_ms",
            "tol_ms",
            "N_days_total",
            "N_days_support_pass",
            "N_days_computed",
            "median_net_bps",
            "p10_net_bps",
            "min_net_bps",
            "pos_rate",
            "median_gross_bps",
            "median_penalty_bps",
            "fee_bps_total",
            "feasibility_label",
            "feasibility_reason",
        ],
        summary_rows,
    )

    result = f"PASS/FEASIBLE:{best_cell}" if robust_found else f"FAIL/NOT_FEASIBLE:best={best_cell}"
    if n_failed_oom > 0 or n_failed_other > 0:
        result = result + ";detail=partial_job_failures"
    write_text(pack.dir / "result.txt", result + "\n")

    write_label_report(
        pack_dir=pack.dir,
        result=result,
        best_cell=best_cell,
        selected_cells_count=len(selected_cells),
        selected_days_count=len(jobs),
        n_done=n_done,
        n_failed_oom=n_failed_oom,
        n_failed_other=n_failed_other,
        source_fullscan_pack=source_fullscan.name,
        source_robust_pack=source_robust.name,
        analysis_kind=analysis_kind,
        execution_mode=str(args.execution_mode),
        maker_fee_bps=float(args.maker_fee_bps),
        taker_fee_bps=float(args.taker_fee_bps),
    )
    write_artifact_manifest(pack.dir)

    full_sha_after = sha256_file(full_tar)
    robust_sha_after = sha256_file(robust_tar)
    unchanged = full_sha_before == full_sha_after and robust_sha_before == robust_sha_after
    write_text(
        pack.dir / "old_pack_unchanged.txt",
        (
            f"source_fullscan_pack={source_fullscan.name}\n"
            f"source_fullscan_tar={full_tar}\n"
            f"source_fullscan_sha_before={full_sha_before}\n"
            f"source_fullscan_sha_after={full_sha_after}\n"
            f"source_robust_pack={source_robust.name}\n"
            f"source_robust_tar={robust_tar}\n"
            f"source_robust_sha_before={robust_sha_before}\n"
            f"source_robust_sha_after={robust_sha_after}\n"
            f"unchanged={str(unchanged).lower()}\n"
        ),
    )
    if not unchanged:
        result = "FAIL/SOURCE_PACK_MUTATION_DETECTED"
        write_text(pack.dir / "result.txt", result + "\n")
        write_label_report(
            pack_dir=pack.dir,
            result=result,
            best_cell=best_cell,
            selected_cells_count=len(selected_cells),
            selected_days_count=len(jobs),
            n_done=n_done,
            n_failed_oom=n_failed_oom,
            n_failed_other=n_failed_other,
            source_fullscan_pack=source_fullscan.name,
            source_robust_pack=source_robust.name,
            analysis_kind=analysis_kind,
            execution_mode=str(args.execution_mode),
            maker_fee_bps=float(args.maker_fee_bps),
            taker_fee_bps=float(args.taker_fee_bps),
        )

    required = [
        "source_packs.tsv",
        "selected_cells.tsv",
        "selected_days.tsv",
        "object_keys_selected.tsv",
        "progress.tsv",
        "per_day_results.tsv",
        "cell_summary_feasibility.tsv",
        "result.txt",
        "label_report.txt",
        "campaign_meta.tsv",
        "artifact_manifest.tsv",
        "command_index.tsv",
        "time_v_summary.tsv",
        "old_pack_unchanged.txt",
    ]
    try:
        write_integrity(pack.dir, required)
    except Exception as exc:
        write_text(pack.dir / "analysis" / "integrity_error.txt", f"{type(exc).__name__}:{exc}\n")

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
        write_text(pack.dir / "analysis" / "finalize_error.txt", f"{type(exc).__name__}:{exc}\n")

    moved_to_file = REPO / "evidence" / f"{pack.name}.moved_to.txt"
    if moved_to_file.exists():
        moved_to = Path(moved_to_file.read_text(encoding="utf-8").strip())
        if moved_to.exists():
            print(f"PACK={pack.name}")
            print(f"MOVED_TO={moved_to}")
            print(f"FINAL_RESULT={result}")
            print(f"GLOBAL_CUM_WALL={runner.cumulative_wall_s:.6f}")
            print(f"PEAK_RSS_KB={runner.peak_rss_kb}")

    return 0 if finalize_ok else 2


def main() -> int:
    args = parse_args()
    if args.worker:
        return worker_main(args)
    return controller_main(args)


if __name__ == "__main__":
    raise SystemExit(main())
