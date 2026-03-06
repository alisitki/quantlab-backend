#!/usr/bin/env python3
"""Phase-5 day benchmark v0: auto-discover one benchmark day and run six lanes."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from phase5_big_hunt_plan_v2 import ensure_inventory_state
    from phase5_state_selection_v1 import build_object_keys_tsv, filter_rows, load_inventory
except ImportError:  # pragma: no cover
    from tools.phase5_big_hunt_plan_v2 import ensure_inventory_state
    from tools.phase5_state_selection_v1 import build_object_keys_tsv, filter_rows, load_inventory


DEFAULT_EXCHANGES = "binance,bybit,okx"
DEFAULT_STREAMS = "trade,bbo"
DEFAULT_ARCHIVE_ROOT = "/home/deploy/quantlab-evidence-archive/20260306_slim"
DEFAULT_STATE_DIR = "tools/phase5_state"
DEFAULT_INVENTORY_STATE_JSON = "/tmp/compacted__state.json"
DEFAULT_INVENTORY_BUCKET = "quantlab-compact"
DEFAULT_INVENTORY_KEY = "compacted/_state.json"
DEFAULT_INVENTORY_S3_TOOL = "/tmp/s3_compact_tool.py"
BENCHMARK_STATUS_OK = "OK"
BENCHMARK_STATUS_SKIP_NO_COVERAGE = "SKIP_NO_COVERAGE"
BENCHMARK_STATUS_PHASE5_FAIL = "PHASE5_FAIL"
BENCHMARK_STATUS_PHASE6_V2_FAIL = "PHASE6_V2_FAIL"
ELIGIBLE_DECISIONS = {"PROMOTE", "PROMOTE_STRONG"}
SUMMARY_COLUMNS = [
    "date",
    "exchange",
    "stream",
    "status",
    "selected_symbol_count",
    "selected_row_count",
    "elapsed_sec",
    "max_rss_kb",
    "archive_dir",
    "phase6_v2_decision",
    "candidate_export_delta",
]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 day benchmark v0")
    p.add_argument("--auto-date", dest="auto_date", action="store_true", default=True)
    p.add_argument("--date", default="", help="Manual override YYYYMMDD")
    p.add_argument("--exchanges", default=DEFAULT_EXCHANGES)
    p.add_argument("--streams", default=DEFAULT_STREAMS)
    p.add_argument("--max-symbols", type=int, default=20)
    p.add_argument("--per-run-timeout-min", type=int, default=12)
    p.add_argument("--max-wall-min", type=int, default=180)
    p.add_argument("--archive-root", default=DEFAULT_ARCHIVE_ROOT)
    p.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    p.add_argument("--inventory-state-json", default=DEFAULT_INVENTORY_STATE_JSON)
    p.add_argument("--inventory-bucket", default=DEFAULT_INVENTORY_BUCKET)
    p.add_argument("--inventory-key", default=DEFAULT_INVENTORY_KEY)
    p.add_argument("--inventory-s3-tool", default=DEFAULT_INVENTORY_S3_TOOL)
    p.add_argument(
        "--require-quality-pass",
        dest="require_quality_pass",
        action="store_true",
        default=True,
    )
    p.add_argument(
        "--allow-bad-quality",
        dest="require_quality_pass",
        action="store_false",
    )
    return p.parse_args(argv)


def parse_csv_lower(raw: str) -> List[str]:
    vals = [str(x).strip().lower() for x in str(raw).split(",") if str(x).strip()]
    return sorted(set(vals))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def benchmark_pairs(exchanges_csv: str, streams_csv: str) -> List[Tuple[str, str]]:
    exchanges = parse_csv_lower(exchanges_csv)
    streams = parse_csv_lower(streams_csv)
    if not exchanges or not streams:
        raise ValueError("missing_exchanges_or_streams")
    return [(ex, st) for ex in exchanges for st in streams]


def is_quality_acceptable(day_quality_post: str, require_quality_pass: bool) -> bool:
    if not require_quality_pass:
        return True
    return str(day_quality_post or "").strip().upper() != "BAD"


def pair_presence_by_day(
    rows: Sequence[Any],
    pairs: Sequence[Tuple[str, str]],
    *,
    require_quality_pass: bool,
) -> Dict[str, set[Tuple[str, str]]]:
    pair_set = set(pairs)
    coverage: Dict[str, set[Tuple[str, str]]] = {}
    for row in rows:
        pair = (str(row.exchange), str(row.stream))
        if pair not in pair_set:
            continue
        if str(row.status) != "success":
            continue
        if not is_quality_acceptable(str(row.day_quality_post), require_quality_pass):
            continue
        coverage.setdefault(str(row.date), set()).add(pair)
    return coverage


def discovery_payload(
    chosen_day: str,
    coverage_score: int,
    pair_presence: Dict[str, bool],
    selection_rule: str,
) -> Dict[str, Any]:
    ordered = {f"{ex}/{st}": bool(pair_presence.get(f"{ex}/{st}", False)) for ex, st in sorted((k.split("/")[0], k.split("/")[1]) for k in pair_presence)}
    return {
        "chosen_day": chosen_day,
        "coverage_score": coverage_score,
        "pair_presence": ordered,
        "selection_rule": selection_rule,
    }


def discover_benchmark_day(
    rows: Sequence[Any],
    pairs: Sequence[Tuple[str, str]],
    *,
    require_quality_pass: bool,
) -> Dict[str, Any]:
    coverage = pair_presence_by_day(rows, pairs, require_quality_pass=require_quality_pass)
    if not coverage:
        raise RuntimeError("no_benchmark_dates_from_inventory")

    pair_keys = [f"{ex}/{st}" for ex, st in pairs]
    full_count = len(pair_keys)
    full_candidates = sorted(day for day, present in coverage.items() if len(present) == full_count)
    if full_candidates:
        chosen_day = full_candidates[0]
        selection_rule = "oldest_full_coverage_score=6"
    else:
        max_score = max(len(present) for present in coverage.values())
        chosen_day = sorted(day for day, present in coverage.items() if len(present) == max_score)[0]
        selection_rule = "oldest_highest_coverage_score_fallback"
    present_pairs = coverage[chosen_day]
    pair_presence = {key: False for key in pair_keys}
    for ex, st in present_pairs:
        pair_presence[f"{ex}/{st}"] = True
    return {
        "chosen_day": chosen_day,
        "coverage_score": len(present_pairs),
        "pair_presence": pair_presence,
        "selection_rule": selection_rule,
    }


def parse_hms_to_seconds(raw: str) -> float:
    text = str(raw or "").strip()
    if not text:
        return 0.0
    chunks = text.split(":")
    if len(chunks) == 1:
        return float(chunks[0])
    if len(chunks) == 2:
        minutes = int(chunks[0])
        seconds = float(chunks[1])
        return minutes * 60.0 + seconds
    if len(chunks) == 3:
        hours = int(chunks[0])
        minutes = int(chunks[1])
        seconds = float(chunks[2])
        return hours * 3600.0 + minutes * 60.0 + seconds
    raise ValueError(f"invalid_elapsed_format:{raw}")


def parse_time_v_metrics(path: Path) -> Dict[str, float]:
    elapsed_sec = 0.0
    max_rss_kb = 0.0
    if not path.exists():
        return {"elapsed_sec": elapsed_sec, "max_rss_kb": max_rss_kb}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "Elapsed (wall clock) time" in line:
            if "):" in line:
                elapsed_text = line.split("):", 1)[1].strip()
            else:
                elapsed_text = line.split(":", 1)[1].strip()
            elapsed_sec = parse_hms_to_seconds(elapsed_text)
        elif "Maximum resident set size" in line:
            max_rss_kb = float(line.split(":", 1)[1].strip() or "0")
    return {"elapsed_sec": elapsed_sec, "max_rss_kb": max_rss_kb}


def parse_kv_lines(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in str(text).splitlines():
        line = raw.strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def run_timed_command(
    *,
    cmd: List[str],
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
    time_path: Path,
) -> Tuple[int, Dict[str, str], Dict[str, float]]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        ["/usr/bin/time", "-v", "-o", str(time_path)] + cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    stdout_path.write_text(proc.stdout, encoding="utf-8")
    stderr_path.write_text(proc.stderr, encoding="utf-8")
    return proc.returncode, parse_kv_lines(proc.stdout), parse_time_v_metrics(time_path)


def build_summary_row(
    *,
    date: str,
    exchange: str,
    stream: str,
    status: str,
    selected_symbol_count: int,
    selected_row_count: int,
    elapsed_sec: float,
    max_rss_kb: float,
    archive_dir: str,
    phase6_v2_decision: str,
    candidate_export_delta: int,
) -> Dict[str, str]:
    return {
        "date": str(date),
        "exchange": str(exchange),
        "stream": str(stream),
        "status": str(status),
        "selected_symbol_count": str(int(selected_symbol_count)),
        "selected_row_count": str(int(selected_row_count)),
        "elapsed_sec": f"{float(elapsed_sec):.6f}",
        "max_rss_kb": f"{float(max_rss_kb):.1f}",
        "archive_dir": str(archive_dir),
        "phase6_v2_decision": str(phase6_v2_decision),
        "candidate_export_delta": str(int(candidate_export_delta)),
    }


def candidate_count(state_dir: Path) -> int:
    path = state_dir / "candidate_index.json"
    if not path.exists():
        return 0
    obj = json.loads(path.read_text(encoding="utf-8"))
    return int(obj.get("record_count", 0) or 0)


def write_tsv(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(SUMMARY_COLUMNS)
        for row in rows:
            writer.writerow([row.get(col, "") for col in SUMMARY_COLUMNS])


def run_pair(
    *,
    repo: Path,
    bench_root: Path,
    state_dir: Path,
    archive_root: Path,
    date: str,
    exchange: str,
    stream: str,
    rows: Sequence[Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    pair_dir = bench_root / f"{exchange}_{stream}"
    object_keys_tsv = pair_dir / "state_selection" / "object_keys_selected.tsv"
    selected_rows, selected_symbols, _days = filter_rows(
        rows,
        exchange=exchange,
        stream=stream,
        start=date,
        end=date,
        require_status="success",
        require_quality_pass=bool(args.require_quality_pass),
        max_symbols=int(args.max_symbols),
    )
    if not selected_rows or not selected_symbols:
        build_object_keys_tsv([], object_keys_tsv)
        summary = build_summary_row(
            date=date,
            exchange=exchange,
            stream=stream,
            status=BENCHMARK_STATUS_SKIP_NO_COVERAGE,
            selected_symbol_count=0,
            selected_row_count=0,
            elapsed_sec=0.0,
            max_rss_kb=0.0,
            archive_dir="",
            phase6_v2_decision="",
            candidate_export_delta=0,
        )
        return {
            "summary": summary,
            "detail": {
                "phase5_exit_code": 0,
                "phase6_v2_exit_code": 0,
                "phase5_stdout_path": str(pair_dir / "phase5_stdout.log"),
                "phase5_stderr_path": str(pair_dir / "phase5_stderr.log"),
                "phase5_time_path": str(pair_dir / "phase5_time-v.log"),
                "phase6_stdout_path": str(pair_dir / "phase6_stdout.log"),
                "phase6_stderr_path": str(pair_dir / "phase6_stderr.log"),
                "phase6_time_path": str(pair_dir / "phase6_time-v.log"),
                "selected_symbols": [],
            },
        }

    build_object_keys_tsv(selected_rows, object_keys_tsv)
    run_id = (
        "multi-hypothesis-phase5-day-benchmark-"
        f"{exchange}-{stream}-{date}-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}__FULLSCAN_MAJOR"
    )
    phase5_stdout = pair_dir / "phase5_stdout.log"
    phase5_stderr = pair_dir / "phase5_stderr.log"
    phase5_time = pair_dir / "phase5_time-v.log"
    phase5_cmd = [
        "python3",
        "tools/phase5_big_hunt_v0.py",
        "--exchange",
        exchange,
        "--stream",
        stream,
        "--start",
        date,
        "--end",
        date,
        "--objectKeysTsv",
        str(object_keys_tsv),
        "--max-symbols",
        str(int(args.max_symbols)),
        "--per-run-timeout-min",
        str(int(args.per_run_timeout_min)),
        "--max-wall-min",
        str(int(args.max_wall_min)),
        "--archive-root",
        str(archive_root),
        "--run-id",
        run_id,
        "--phase6-state-dir",
        str(repo / "tools" / "phase6_state"),
        "--phase6-policy",
        str(repo / "tools" / "phase6_state" / "promotion_policy.json"),
    ]
    if phase5_stdout.exists() and phase5_time.exists():
        phase5_kv = parse_kv_lines(phase5_stdout.read_text(encoding="utf-8", errors="replace"))
        if str(phase5_kv.get("ARCHIVE_DIR", "")).strip():
            phase5_ec = 0
            phase5_metrics = parse_time_v_metrics(phase5_time)
        else:
            phase5_ec, phase5_kv, phase5_metrics = run_timed_command(
                cmd=phase5_cmd,
                cwd=repo,
                stdout_path=phase5_stdout,
                stderr_path=phase5_stderr,
                time_path=phase5_time,
            )
    else:
        phase5_ec, phase5_kv, phase5_metrics = run_timed_command(
            cmd=phase5_cmd,
            cwd=repo,
            stdout_path=phase5_stdout,
            stderr_path=phase5_stderr,
            time_path=phase5_time,
        )
    archive_dir = str(phase5_kv.get("ARCHIVE_DIR", "")).strip()
    if phase5_ec != 0 or not archive_dir:
        summary = build_summary_row(
            date=date,
            exchange=exchange,
            stream=stream,
            status=BENCHMARK_STATUS_PHASE5_FAIL,
            selected_symbol_count=len(selected_symbols),
            selected_row_count=len(selected_rows),
            elapsed_sec=phase5_metrics["elapsed_sec"],
            max_rss_kb=phase5_metrics["max_rss_kb"],
            archive_dir=archive_dir,
            phase6_v2_decision="",
            candidate_export_delta=0,
        )
        return {
            "summary": summary,
            "detail": {
                "phase5_exit_code": phase5_ec,
                "phase6_v2_exit_code": 0,
                "phase5_stdout_path": str(phase5_stdout),
                "phase5_stderr_path": str(phase5_stderr),
                "phase5_time_path": str(phase5_time),
                "phase6_stdout_path": str(pair_dir / "phase6_stdout.log"),
                "phase6_stderr_path": str(pair_dir / "phase6_stderr.log"),
                "phase6_time_path": str(pair_dir / "phase6_time-v.log"),
                "selected_symbols": selected_symbols,
                "run_id": run_id,
                "phase5_stdout_kv": phase5_kv,
            },
        }

    phase6_stdout = pair_dir / "phase6_stdout.log"
    phase6_stderr = pair_dir / "phase6_stderr.log"
    phase6_time = pair_dir / "phase6_time-v.log"
    phase6_cmd = [
        "python3",
        "tools/phase6_promotion_guards_v2.py",
        "--pack",
        archive_dir,
    ]
    phase6_ec, phase6_kv, _phase6_metrics = run_timed_command(
        cmd=phase6_cmd,
        cwd=repo,
        stdout_path=phase6_stdout,
        stderr_path=phase6_stderr,
        time_path=phase6_time,
    )
    decision = str(phase6_kv.get("decision", "")).strip()
    status = BENCHMARK_STATUS_OK if phase6_ec == 0 else BENCHMARK_STATUS_PHASE6_V2_FAIL
    candidate_export_delta = 1 if decision in ELIGIBLE_DECISIONS else 0
    summary = build_summary_row(
        date=date,
        exchange=exchange,
        stream=stream,
        status=status,
        selected_symbol_count=len(selected_symbols),
        selected_row_count=len(selected_rows),
        elapsed_sec=phase5_metrics["elapsed_sec"],
        max_rss_kb=phase5_metrics["max_rss_kb"],
        archive_dir=archive_dir,
        phase6_v2_decision=decision,
        candidate_export_delta=candidate_export_delta,
    )
    return {
        "summary": summary,
        "detail": {
            "phase5_exit_code": phase5_ec,
            "phase6_v2_exit_code": phase6_ec,
            "phase5_stdout_path": str(phase5_stdout),
            "phase5_stderr_path": str(phase5_stderr),
            "phase5_time_path": str(phase5_time),
            "phase6_stdout_path": str(phase6_stdout),
            "phase6_stderr_path": str(phase6_stderr),
            "phase6_time_path": str(phase6_time),
            "selected_symbols": selected_symbols,
            "run_id": run_id,
            "phase5_stdout_kv": phase5_kv,
            "phase6_stdout_kv": phase6_kv,
        },
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    state_dir = Path(args.state_dir).resolve()
    archive_root = Path(args.archive_root).resolve()
    inventory_state_json = ensure_inventory_state(
        state_path=Path(args.inventory_state_json).resolve(),
        bucket=str(args.inventory_bucket),
        key=str(args.inventory_key),
        s3_tool=str(args.inventory_s3_tool),
        repo=repo,
    )
    rows = load_inventory(inventory_state_json, bucket=str(args.inventory_bucket))
    pairs = benchmark_pairs(args.exchanges, args.streams)
    discovery = (
        {
            "chosen_day": str(args.date).strip(),
            "coverage_score": 0,
            "pair_presence": {f"{ex}/{st}": False for ex, st in pairs},
            "selection_rule": "manual_override",
        }
        if str(args.date).strip()
        else discover_benchmark_day(rows, pairs, require_quality_pass=bool(args.require_quality_pass))
    )

    chosen_day = str(discovery["chosen_day"])
    coverage = pair_presence_by_day(rows, pairs, require_quality_pass=bool(args.require_quality_pass))
    present_pairs = coverage.get(chosen_day, set())
    discovery["coverage_score"] = len(present_pairs)
    discovery["pair_presence"] = {
        f"{ex}/{st}": (ex, st) in present_pairs
        for ex, st in pairs
    }

    candidate_before = candidate_count(repo / "tools" / "phase6_state")
    bench_root = state_dir / f"day_benchmark_{chosen_day}"
    started_monotonic = time.monotonic()
    rows_out: List[Dict[str, str]] = []
    details_out: List[Dict[str, Any]] = []
    for exchange, stream in pairs:
        result = run_pair(
            repo=repo,
            bench_root=bench_root,
            state_dir=state_dir,
            archive_root=archive_root,
            date=chosen_day,
            exchange=exchange,
            stream=stream,
            rows=rows,
            args=args,
        )
        rows_out.append(result["summary"])
        details_out.append(result["detail"])

    export_stdout = bench_root / "candidate_export.stdout.log"
    export_stderr = bench_root / "candidate_export.stderr.log"
    export_time = bench_root / "candidate_export.time-v.log"
    export_ec, export_kv, _export_metrics = run_timed_command(
        cmd=["python3", "tools/phase6_candidate_export_v0.py"],
        cwd=repo,
        stdout_path=export_stdout,
        stderr_path=export_stderr,
        time_path=export_time,
    )
    review_stdout = bench_root / "candidate_review.stdout.log"
    review_stderr = bench_root / "candidate_review.stderr.log"
    review_time = bench_root / "candidate_review.time-v.log"
    review_ec, review_kv, _review_metrics = run_timed_command(
        cmd=["python3", "tools/phase6_candidate_review_v0.py"],
        cwd=repo,
        stdout_path=review_stdout,
        stderr_path=review_stderr,
        time_path=review_time,
    )
    candidate_after = candidate_count(repo / "tools" / "phase6_state")
    total_wall_sec = time.monotonic() - started_monotonic

    tsv_path = state_dir / f"day_benchmark_{chosen_day}.tsv"
    json_path = state_dir / f"day_benchmark_{chosen_day}.json"
    write_tsv(tsv_path, rows_out)
    payload = {
        "chosen_day": chosen_day,
        "candidate_count_before": candidate_before,
        "candidate_count_after": candidate_after,
        "candidate_export_summary": {
            "exit_code": export_ec,
            "kv": export_kv,
            "stdout_path": str(export_stdout),
            "stderr_path": str(export_stderr),
            "time_path": str(export_time),
        },
        "candidate_review_summary": {
            "exit_code": review_ec,
            "kv": review_kv,
            "stdout_path": str(review_stdout),
            "stderr_path": str(review_stderr),
            "time_path": str(review_time),
        },
        "discovery": discovery,
        "generated_ts_utc": utc_now_iso(),
        "rows": rows_out,
        "row_details": details_out,
        "summary_tsv": str(tsv_path),
        "summary_json": str(json_path),
        "total_wall_sec": round(total_wall_sec, 6),
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    pair_presence_text = ";".join(
        f"{pair}={'1' if present else '0'}"
        for pair, present in sorted(discovery["pair_presence"].items())
    )
    print(f"chosen_day={chosen_day}")
    print(f"coverage_score={discovery['coverage_score']}")
    print(f"pair_presence={pair_presence_text}")
    print(f"selection_rule={discovery['selection_rule']}")
    print(f"candidate_count_before={candidate_before}")
    print(f"candidate_count_after={candidate_after}")
    print(f"candidate_export_exit={export_ec}")
    print(f"candidate_review_exit={review_ec}")
    print(f"summary_tsv={tsv_path}")
    print(f"summary_json={json_path}")
    print(f"total_wall_sec={round(total_wall_sec, 6):.6f}")
    for row in rows_out:
        print(
            "pair_result="
            + ",".join(
                [
                    row["exchange"],
                    row["stream"],
                    row["status"],
                    row["elapsed_sec"],
                    row["max_rss_kb"],
                    row["phase6_v2_decision"],
                    row["archive_dir"],
                ]
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
