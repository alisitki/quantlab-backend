#!/usr/bin/env python3
"""Phase-5 Big Hunt orchestrator with Phase-6 post-eval hook."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import subprocess
import tarfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


REQUIRED_EVIDENCE_FILES = [
    "determinism_compare.tsv",
    "artifact_manifest.tsv",
    "label_report.txt",
    "integrity_check.txt",
]


@dataclass
class KeyRow:
    original: Dict[str, str]
    exchange: str
    stream: str
    symbol: str
    date: str
    partition_key: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-5 Big Hunt v0 orchestrator")
    p.add_argument("--objectKeysTsv", required=True)
    p.add_argument("--exchange", required=True)
    p.add_argument("--stream", required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--max-symbols", type=int, default=20)
    p.add_argument("--per-run-timeout-min", type=int, default=12)
    p.add_argument("--max-wall-min", type=int, default=120)
    p.add_argument("--archive-root", default="")
    p.add_argument("--run-id", default="")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--plan-out", default="")
    p.add_argument("--phase6-policy", default="")
    p.add_argument("--phase6-state-dir", default="")
    return p.parse_args()


def utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def today_utc_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def default_run_id(exchange: str, stream: str, start: str, end: str) -> str:
    return f"multi-hypothesis-phase5-bighunt-{exchange}-{stream}-{start}..{end}-{utc_now_stamp()}__FULLSCAN_MAJOR"


def parse_ymd(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y%m%d")


def in_range(day: str, start: str, end: str) -> bool:
    return start <= day <= end


def parse_partition_key(partition_key: str) -> Optional[Tuple[str, str, str, str]]:
    parts = [p.strip().lower() for p in str(partition_key or "").split("/")]
    if len(parts) != 4:
        return None
    ex, stream, symbol, day = parts
    if not ex or not stream or not symbol or not re.fullmatch(r"\d{8}", day):
        return None
    return ex, stream, symbol, day


def parse_data_key_date(data_key: str) -> str:
    m = re.search(r"date=(\d{8})", str(data_key or ""))
    return m.group(1) if m else ""


def normalize_symbol(v: str) -> str:
    return str(v or "").strip().lower()


def load_object_keys_rows(tsv_path: Path) -> Tuple[List[str], List[KeyRow]]:
    if not tsv_path.exists():
        raise FileNotFoundError(f"object_keys_tsv_missing:{tsv_path}")
    with tsv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows: List[KeyRow] = []
        for row in reader:
            row = {str(k): str(v) for k, v in row.items()}
            ex = normalize_symbol(row.get("exchange", ""))
            st = normalize_symbol(row.get("stream", ""))
            sym = normalize_symbol(row.get("symbol", ""))
            day = str(row.get("date", "")).strip()
            pkey = str(row.get("partition_key", "")).strip()

            if (not ex or not st or not sym or not re.fullmatch(r"\d{8}", day)) and pkey:
                parsed = parse_partition_key(pkey)
                if parsed is not None:
                    ex, st, sym, day = parsed

            if not day:
                day = parse_data_key_date(row.get("data_key", ""))

            if not ex or not st or not sym or not re.fullmatch(r"\d{8}", day):
                continue

            if not pkey:
                pkey = f"{ex}/{st}/{sym}/{day}"

            rows.append(
                KeyRow(
                    original=row,
                    exchange=ex,
                    stream=st,
                    symbol=sym,
                    date=day,
                    partition_key=pkey,
                )
            )
    return headers, rows


def select_symbols(
    rows: Sequence[KeyRow],
    exchange: str,
    stream: str,
    start: str,
    end: str,
    max_symbols: int,
    max_wall_min: int,
    per_run_timeout_min: int,
) -> Dict[str, object]:
    ex = exchange.lower()
    st = stream.lower()
    filtered = [
        r
        for r in rows
        if r.exchange == ex and r.stream == st and in_range(r.date, start, end)
    ]
    available_symbols = sorted({r.symbol for r in filtered})
    available = len(available_symbols)
    if per_run_timeout_min <= 0:
        raise ValueError("per_run_timeout_min must be > 0")
    k_wall = max(0, math.floor(max_wall_min / per_run_timeout_min))
    selected_count = min(max_symbols, available, k_wall)
    selected_symbols = available_symbols[:selected_count]
    return {
        "filtered_rows": filtered,
        "available_symbols": available_symbols,
        "available": available,
        "k_wall": k_wall,
        "selected_count": selected_count,
        "selected_symbols": selected_symbols,
    }


def canonical_header(input_header: List[str]) -> List[str]:
    out = list(input_header)
    for col in ["label", "partition_key", "date", "data_key", "meta_key", "bucket", "exchange", "stream", "symbol"]:
        if col not in out:
            out.append(col)
    return out


def selected_rows_for_symbols(rows: Sequence[KeyRow], selected_symbols: Sequence[str]) -> List[KeyRow]:
    sel = set(selected_symbols)
    chosen = [r for r in rows if r.symbol in sel]
    chosen.sort(
        key=lambda r: (
            r.symbol,
            r.date,
            r.partition_key,
            str(r.original.get("data_key", "")),
            str(r.original.get("meta_key", "")),
            str(r.original.get("bucket", "")),
        )
    )
    return chosen


def write_object_keys_tsv(out_path: Path, header: Sequence[str], rows: Sequence[KeyRow]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(list(header))
        for r in rows:
            row = dict(r.original)
            row.setdefault("partition_key", r.partition_key)
            row.setdefault("date", r.date)
            row.setdefault("exchange", r.exchange)
            row.setdefault("stream", r.stream)
            row.setdefault("symbol", r.symbol)
            writer.writerow([str(row.get(col, "")) for col in header])


def write_tsv(path: Path, header: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(list(header))
        for r in rows:
            w.writerow(list(r))


def read_first_line(path: Path) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return text[0].strip() if text else ""


def parse_time_v(path: Path) -> Tuple[float, int]:
    text = path.read_text(encoding="utf-8", errors="replace")
    elapsed = 0.0
    rss = 0
    m = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*(.+)", text)
    if m:
        raw = m.group(1).strip()
        parts = raw.split(":")
        try:
            if len(parts) == 2:
                elapsed = int(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3:
                elapsed = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        except ValueError:
            elapsed = 0.0
    m2 = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", text)
    if m2:
        rss = int(m2.group(1))
    return elapsed, rss


def parse_determinism_statuses(path: Path) -> str:
    statuses = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            status = str(row.get("determinism_status", "")).strip()
            if status:
                statuses.add(status)
    return "|".join(sorted(statuses))


def tail_lines(path: Path, n: int) -> List[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-n:]


def run_cmd(
    cmd: Sequence[str],
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
) -> int:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("w", encoding="utf-8") as out, stderr_path.open("w", encoding="utf-8") as err:
        proc = subprocess.run(list(cmd), cwd=str(cwd), stdout=out, stderr=err, text=True)
    return int(proc.returncode)


def read_nonempty_jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    c = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.strip():
            c += 1
    return c


def read_phase6_index(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"record_count": 0, "pack_latest": {}, "promote_packs": []}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"record_count": 0, "pack_latest": {}, "promote_packs": []}
    if not isinstance(obj, dict):
        return {"record_count": 0, "pack_latest": {}, "promote_packs": []}
    return obj


def build_phase6_state_snapshot(state_dir: Path, run_id: str) -> Dict[str, object]:
    records = state_dir / "promotion_records.jsonl"
    index = state_dir / "promotion_index.json"
    idx_obj = read_phase6_index(index)
    pack_latest = idx_obj.get("pack_latest", {})
    keys = []
    if isinstance(pack_latest, dict):
        keys = sorted([k for k in pack_latest.keys() if str(k).startswith(run_id)])
    return {
        "records_nonempty": read_nonempty_jsonl_count(records),
        "index_record_count": int(idx_obj.get("record_count", 0) or 0),
        "target_pack_keys": keys,
        "promote_packs_count": len(idx_obj.get("promote_packs", []) or []),
    }


def ensure_integrity_ok(integrity_path: Path) -> bool:
    if not integrity_path.exists():
        return False
    for line in integrity_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.strip() == "missing_count=0":
            return True
    return False


def verify_tar_members(pack_tgz: Path) -> List[str]:
    pat = re.compile(
        r"(campaign_meta\.tsv|run_summary\.tsv|state_selection/object_keys_selected\.tsv|"
        r"runs/.*/artifacts/multi_hypothesis/(determinism_compare\.tsv|artifact_manifest\.tsv|label_report\.txt|integrity_check\.txt)|"
        r"runs/.*/artifacts/context/context_summary\.(tsv|json))$"
    )
    matched: List[str] = []
    with tarfile.open(pack_tgz, "r:gz") as tf:
        for name in sorted(tf.getnames()):
            if pat.search(name):
                matched.append(name)
    return matched


def read_first_tsv_row(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            return {str(k): str(v) for k, v in row.items()}
    return {}


def write_campaign_report(pack_dir: Path, report: Dict[str, object]) -> None:
    txt_path = pack_dir / "campaign_report.txt"
    json_path = pack_dir / "campaign_report.json"
    lines = [
        f"status={report.get('status','')}",
        f"run_id={report.get('run_id','')}",
        f"pack_dir={report.get('pack_dir','')}",
        f"archive_dir={report.get('archive_dir','')}",
        f"selected_symbols_count={report.get('selected_symbols_count',0)}",
        "selected_symbols=" + ",".join(report.get("selected_symbols", [])),
        f"available_symbols={report.get('available_symbols',0)}",
        f"selection_rule={report.get('selection_rule','')}",
    ]
    stop_reason = str(report.get("stop_reason", ""))
    if stop_reason:
        lines.append(f"stop_reason={stop_reason}")
    finalize = report.get("finalize", {})
    if isinstance(finalize, dict):
        for k in ["sha256_ok", "sha_verify_present", "unpacked_absent_ok"]:
            if k in finalize:
                lines.append(f"{k}={finalize[k]}")
    post_eval = report.get("post_eval", {})
    if isinstance(post_eval, dict):
        for k in ["decision", "record_appended", "exit_code"]:
            if k in post_eval:
                lines.append(f"post_eval_{k}={post_eval[k]}")
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[1]
    exchange = args.exchange.lower()
    stream = args.stream.lower()
    parse_ymd(args.start)
    parse_ymd(args.end)
    if args.start > args.end:
        raise ValueError(f"invalid date range {args.start}..{args.end}")
    if args.max_symbols <= 0:
        raise ValueError("max-symbols must be > 0")
    if args.per_run_timeout_min <= 0:
        raise ValueError("per-run-timeout-min must be > 0")
    if args.max_wall_min <= 0:
        raise ValueError("max-wall-min must be > 0")

    run_id = args.run_id or default_run_id(exchange, stream, args.start, args.end)
    pack_dir = repo / "evidence" / run_id
    archive_root = Path(args.archive_root).resolve() if args.archive_root else Path(f"/home/deploy/quantlab-evidence-archive/{today_utc_yyyymmdd()}_slim")
    phase6_policy = Path(args.phase6_policy).resolve() if args.phase6_policy else (repo / "tools" / "phase6_state" / "promotion_policy.json")
    phase6_state_dir = Path(args.phase6_state_dir).resolve() if args.phase6_state_dir else (repo / "tools" / "phase6_state")
    object_keys_input = Path(args.objectKeysTsv).resolve()

    headers_in, raw_rows = load_object_keys_rows(object_keys_input)
    selection = select_symbols(
        raw_rows,
        exchange=exchange,
        stream=stream,
        start=args.start,
        end=args.end,
        max_symbols=args.max_symbols,
        max_wall_min=args.max_wall_min,
        per_run_timeout_min=args.per_run_timeout_min,
    )
    available_symbols = selection["available_symbols"]
    selected_symbols = selection["selected_symbols"]

    report: Dict[str, object] = {
        "run_id": run_id,
        "pack_dir": str(pack_dir),
        "archive_root": str(archive_root),
        "archive_dir": "",
        "inputs": {
            "object_keys_tsv": str(object_keys_input),
            "exchange": exchange,
            "stream": stream,
            "start": args.start,
            "end": args.end,
            "max_symbols": args.max_symbols,
            "per_run_timeout_min": args.per_run_timeout_min,
            "max_wall_min": args.max_wall_min,
            "phase6_policy": str(phase6_policy),
            "phase6_state_dir": str(phase6_state_dir),
        },
        "selection_rule": "sorted_first_N",
        "available_symbols": selection["available"],
        "selected_symbols_count": selection["selected_count"],
        "selected_symbols": selected_symbols,
        "k_wall": selection["k_wall"],
        "status": "",
        "stop_reason": "",
        "run_results": [],
        "finalize": {},
        "post_eval": {},
    }

    if selection["available"] == 0:
        report["status"] = "STOP_NO_COVERAGE"
        report["stop_reason"] = "available_symbols=0"
        pack_dir.mkdir(parents=True, exist_ok=True)
        write_campaign_report(pack_dir, report)
        print("STOP: available_symbols=0")
        return 2
    if selection["selected_count"] == 0:
        report["status"] = "STOP_MAX_WALL_BUDGET"
        report["stop_reason"] = "max_wall too small for one run"
        pack_dir.mkdir(parents=True, exist_ok=True)
        write_campaign_report(pack_dir, report)
        print("STOP: max_wall too small for one run")
        return 2

    state_selection_dir = pack_dir / "state_selection"
    state_selection_dir.mkdir(parents=True, exist_ok=True)
    selected_rows = selected_rows_for_symbols(selection["filtered_rows"], selected_symbols)
    header_out = canonical_header(headers_in)
    selected_tsv = state_selection_dir / "object_keys_selected.tsv"
    write_object_keys_tsv(selected_tsv, header_out, selected_rows)

    symbols_file = pack_dir / "symbols_selected.txt"
    symbols_file.write_text("\n".join(selected_symbols) + "\n", encoding="utf-8")

    campaign_meta = pack_dir / "campaign_meta.tsv"
    write_tsv(
        campaign_meta,
        [
            "run_id",
            "category",
            "exchange",
            "stream",
            "start",
            "end",
            "max_symbols",
            "max_wall_min",
            "per_run_timeout_min",
            "available_symbols",
            "selected_symbols_count",
            "selected_symbols_csv",
            "selection_rule",
            "object_keys_tsv_input",
            "object_keys_tsv_selected",
            "archive_root",
            "phase6_policy_path",
            "phase6_state_dir",
        ],
        [[
            run_id,
            "FULLSCAN_MAJOR",
            exchange,
            stream,
            args.start,
            args.end,
            str(args.max_symbols),
            str(args.max_wall_min),
            str(args.per_run_timeout_min),
            str(selection["available"]),
            str(selection["selected_count"]),
            ",".join(selected_symbols),
            "sorted_first_N",
            str(object_keys_input),
            str(selected_tsv),
            str(archive_root),
            str(phase6_policy),
            str(phase6_state_dir),
        ]],
    )

    runs_dir = pack_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    planned_commands = []
    for symbol in selected_symbols:
        out_dir = runs_dir / symbol
        symbol_tsv = out_dir / "state_selection" / "object_keys_selected.tsv"
        cmd = [
            "/usr/bin/time",
            "-v",
            "-o",
            str(out_dir / "time-v.log"),
            "timeout",
            "--signal=INT",
            f"{args.per_run_timeout_min}m",
            "node",
            "tools/run-multi-hypothesis.js",
            "--exchange",
            exchange,
            "--stream",
            stream,
            "--symbol",
            symbol,
            "--start",
            args.start,
            "--end",
            args.end,
            "--objectKeysTsv",
            str(symbol_tsv),
            "--outDir",
            str(out_dir),
            "--downloadsDir",
            str(out_dir / "downloads"),
            "--evidenceOn",
            "true",
        ]
        planned_commands.append({"symbol": symbol, "cmd": cmd, "out_dir": str(out_dir), "symbol_tsv": str(symbol_tsv)})

    plan_out = Path(args.plan_out).resolve() if args.plan_out else (pack_dir / "campaign_plan.json")
    plan_out.parent.mkdir(parents=True, exist_ok=True)
    plan_out.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "pack_dir": str(pack_dir),
                "archive_root": str(archive_root),
                "selected_symbols": selected_symbols,
                "available_symbols": available_symbols,
                "selection_rule": "sorted_first_N",
                "commands": planned_commands,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    if args.dry_run:
        report["status"] = "DRY_RUN"
        report["plan_out"] = str(plan_out)
        write_campaign_report(pack_dir, report)
        print(f"DRY_RUN run_id={run_id}")
        print(f"PACK_DIR={pack_dir}")
        print(f"PLAN_OUT={plan_out}")
        print(f"SELECTED_SYMBOLS={','.join(selected_symbols)}")
        return 0

    start_wall = time.monotonic()
    run_summary_rows: List[List[str]] = []
    failure_count = 0

    for entry in planned_commands:
        elapsed_min = (time.monotonic() - start_wall) / 60.0
        if elapsed_min >= args.max_wall_min:
            report["status"] = "STOP_MAX_WALL_BUDGET"
            report["stop_reason"] = f"max_wall_exceeded elapsed_min={elapsed_min:.3f}"
            break

        symbol = str(entry["symbol"])
        out_dir = Path(entry["out_dir"])
        out_dir.mkdir(parents=True, exist_ok=True)
        symbol_rows = [r for r in selected_rows if r.symbol == symbol]
        write_object_keys_tsv(Path(entry["symbol_tsv"]), header_out, symbol_rows)

        stdout_log = out_dir / "stdout.log"
        stderr_log = out_dir / "stderr.log"
        cmd = entry["cmd"]
        ec = run_cmd(cmd, repo, stdout_log, stderr_log)
        symbol_result: Dict[str, object] = {
            "symbol": symbol,
            "exit_code": ec,
            "cmd": cmd,
            "stdout_log": str(stdout_log),
            "stderr_log": str(stderr_log),
            "time_log": str(out_dir / "time-v.log"),
        }

        if ec != 0:
            failure_count += 1
            symbol_result["stderr_tail_120"] = tail_lines(stderr_log, 120)
            symbol_result["stdout_tail_60"] = tail_lines(stdout_log, 60)
            report["run_results"].append(symbol_result)
            report["status"] = "STOP_RUN_FAILURE"
            report["stop_reason"] = f"symbol={symbol} ec={ec}"
            break

        base = out_dir / "artifacts" / "multi_hypothesis"
        det = base / "determinism_compare.tsv"
        man = base / "artifact_manifest.tsv"
        lab = base / "label_report.txt"
        integ = base / "integrity_check.txt"
        missing = [str(p) for p in [det, man, lab, integ] if not p.exists()]
        if missing:
            failure_count += 1
            symbol_result["missing_artifacts"] = missing
            symbol_result["stderr_tail_120"] = tail_lines(stderr_log, 120)
            report["run_results"].append(symbol_result)
            report["status"] = "STOP_RUN_FAILURE"
            report["stop_reason"] = f"symbol={symbol} missing_artifacts"
            break

        if not ensure_integrity_ok(integ):
            failure_count += 1
            symbol_result["integrity_tail"] = tail_lines(integ, 20)
            report["run_results"].append(symbol_result)
            report["status"] = "STOP_RUN_FAILURE"
            report["stop_reason"] = f"symbol={symbol} integrity_missing_count_nonzero"
            break

        context_dir = out_dir / "artifacts" / "context"
        context_stdout = out_dir / "context_stdout.log"
        context_stderr = out_dir / "context_stderr.log"
        context_time = out_dir / "context_time-v.log"
        context_cmd = [
            "/usr/bin/time",
            "-v",
            "-o",
            str(context_time),
            "python3",
            "tools/context_pack_v0.py",
            "--exchange",
            exchange,
            "--symbol",
            symbol,
            "--core-stream",
            stream,
            "--start",
            args.start,
            "--end",
            args.end,
            "--selection-tsv",
            str(Path(entry["symbol_tsv"])),
            "--downloads-dir",
            str(out_dir / "downloads"),
            "--out-dir",
            str(context_dir),
        ]
        context_ec = run_cmd(context_cmd, repo, context_stdout, context_stderr)
        context_tsv = context_dir / "context_summary.tsv"
        context_json = context_dir / "context_summary.json"
        symbol_result.update(
            {
                "context_stdout_log": str(context_stdout),
                "context_stderr_log": str(context_stderr),
                "context_time_log": str(context_time),
                "context_summary_tsv": str(context_tsv),
                "context_summary_json": str(context_json),
            }
        )
        if context_ec != 0 or not context_tsv.exists() or not context_json.exists():
            symbol_result["context_status"] = "HELPER_ERROR"
            symbol_result["context_exit_code"] = context_ec
            symbol_result["context_stderr_tail_120"] = tail_lines(context_stderr, 120)
            symbol_result["context_stdout_tail_60"] = tail_lines(context_stdout, 60)
        else:
            context_row = read_first_tsv_row(context_tsv)
            if not context_row:
                symbol_result["context_status"] = "HELPER_ERROR"
                symbol_result["context_exit_code"] = context_ec
                symbol_result["context_error"] = "context_summary_empty"
            else:
                symbol_result["context_status"] = "OK"
                symbol_result["context_exit_code"] = context_ec
                for key in ["ctx_mark_price_status", "ctx_funding_status", "ctx_oi_status", "notes"]:
                    symbol_result[key] = str(context_row.get(key, ""))

        elapsed_sec, max_rss = parse_time_v(out_dir / "time-v.log")
        statuses = parse_determinism_statuses(det)
        label = read_first_line(lab)
        run_summary_rows.append([
            symbol,
            "0",
            f"{elapsed_sec:.6f}",
            str(max_rss),
            statuses,
            label,
        ])
        symbol_result.update(
            {
                "elapsed_sec": elapsed_sec,
                "max_rss_kb": max_rss,
                "determinism_statuses": statuses,
                "label": label,
            }
        )
        report["run_results"].append(symbol_result)

        if failure_count > 2:
            report["status"] = "STOP_RUN_FAILURE"
            report["stop_reason"] = f"failure_count={failure_count}"
            break

    run_summary = pack_dir / "run_summary.tsv"
    write_tsv(
        run_summary,
        ["symbol", "exit_code", "elapsed_sec", "max_rss_kb", "determinism_statuses", "label"],
        run_summary_rows,
    )

    if not report["status"]:
        report["status"] = "RUNS_COMPLETED"

    if str(report["status"]).startswith("STOP_"):
        write_campaign_report(pack_dir, report)
        print(f"STOP: {report['status']} reason={report.get('stop_reason','')}")
        return 2

    finalize_stdout = Path(f"/tmp/{run_id}.finalize.stdout.log")
    finalize_stderr = Path(f"/tmp/{run_id}.finalize.stderr.log")
    finalize_time = Path(f"/tmp/{run_id}.finalize.time-v.log")
    finalize_cmd = [
        "/usr/bin/time",
        "-v",
        "-o",
        str(finalize_time),
        "tools/slim_finalize.sh",
        run_id,
        str(pack_dir),
        str(archive_root),
    ]
    finalize_ec = run_cmd(finalize_cmd, repo, finalize_stdout, finalize_stderr)
    report["finalize"] = {
        "exit_code": finalize_ec,
        "stdout_log": str(finalize_stdout),
        "stderr_log": str(finalize_stderr),
        "time_log": str(finalize_time),
    }
    if finalize_ec != 0:
        report["status"] = "STOP_FINALIZE_FAIL"
        report["stop_reason"] = "slim_finalize_nonzero"
        report["finalize"]["stderr_tail_120"] = tail_lines(finalize_stderr, 120)
        write_campaign_report(pack_dir, report)
        print("STOP: STOP_FINALIZE_FAIL")
        return 2

    pack_tgz = repo / "evidence" / f"{run_id}.tar.gz"
    pack_sha = repo / "evidence" / f"{run_id}.tar.gz.sha256"
    moved_txt = repo / "evidence" / f"{run_id}.moved_to.txt"
    evidence_sha_verify = repo / "evidence" / f"{run_id}.sha_verify.txt"
    sha_check = subprocess.run(
        ["sha256sum", "-c", str(pack_sha)],
        cwd=str(repo),
        capture_output=True,
        text=True,
    )
    tar_members = verify_tar_members(pack_tgz)
    archive_dir = Path(moved_txt.read_text(encoding="utf-8").strip())
    archive_sha_verify = archive_dir / "sha_verify.txt"
    unpacked_absent_ok = (not pack_dir.exists()) and archive_dir.exists()

    report["archive_dir"] = str(archive_dir)
    report["finalize"].update(
        {
            "pack_tgz": str(pack_tgz),
            "pack_sha": str(pack_sha),
            "moved_to_txt": str(moved_txt),
            "sha256_check_exit": int(sha_check.returncode),
            "sha256_check_stdout": sha_check.stdout.strip().splitlines(),
            "sha256_ok": sha_check.returncode == 0,
            "tar_members_matched": tar_members,
            "moved_to": str(archive_dir),
            "evidence_sha_verify_present": evidence_sha_verify.exists(),
            "sha_verify_present": archive_sha_verify.exists(),
            "unpacked_absent_ok": unpacked_absent_ok,
        }
    )

    if sha_check.returncode != 0 or not archive_sha_verify.exists() or not unpacked_absent_ok:
        report["status"] = "STOP_FINALIZE_FAIL"
        report["stop_reason"] = "finalize_proof_failed"
        write_campaign_report(archive_dir if archive_dir.exists() else (repo / "evidence"), report)
        print("STOP: STOP_FINALIZE_FAIL proof failed")
        return 2

    state_before = build_phase6_state_snapshot(phase6_state_dir, run_id)
    eval_stdout = Path(f"/tmp/{run_id}.phase6.stdout.log")
    eval_stderr = Path(f"/tmp/{run_id}.phase6.stderr.log")
    eval_time = Path(f"/tmp/{run_id}.phase6.time-v.log")
    eval_cmd = [
        "/usr/bin/time",
        "-v",
        "-o",
        str(eval_time),
        "python3",
        "tools/phase6_promotion_guards_v1.py",
        "--pack",
        str(archive_dir),
        "--policy",
        str(phase6_policy),
        "--state-dir",
        str(phase6_state_dir),
    ]
    eval_ec = run_cmd(eval_cmd, repo, eval_stdout, eval_stderr)
    eval_stdout_lines = eval_stdout.read_text(encoding="utf-8", errors="replace").splitlines()
    kv = {}
    for line in eval_stdout_lines:
        if "=" in line:
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()
    state_after = build_phase6_state_snapshot(phase6_state_dir, run_id)
    target_pack_key_added = len(state_after["target_pack_keys"]) > len(state_before["target_pack_keys"])

    decision_report = archive_dir / "guards" / "decision_report.txt"
    decision_head = decision_report.read_text(encoding="utf-8", errors="replace").splitlines()[:30] if decision_report.exists() else []

    report["post_eval"] = {
        "exit_code": eval_ec,
        "stdout_log": str(eval_stdout),
        "stderr_log": str(eval_stderr),
        "time_log": str(eval_time),
        "decision": kv.get("decision", ""),
        "record_appended": kv.get("record_appended", ""),
        "state_before": state_before,
        "state_after": state_after,
        "state_diff": {
            "records_nonempty": f"{state_before['records_nonempty']}->{state_after['records_nonempty']}",
            "index_record_count": f"{state_before['index_record_count']}->{state_after['index_record_count']}",
            "target_pack_key_added": target_pack_key_added,
        },
        "decision_report_head": decision_head,
    }

    if eval_ec != 0:
        report["status"] = "STOP_PHASE6_EVAL_FAIL"
        report["stop_reason"] = "phase6_eval_nonzero"
        report["post_eval"]["stderr_tail_120"] = tail_lines(eval_stderr, 120)
        write_campaign_report(archive_dir, report)
        print("STOP: STOP_PHASE6_EVAL_FAIL")
        return 2

    report["status"] = "PASS"
    write_campaign_report(archive_dir, report)

    print(f"RUN_ID={run_id}")
    print(f"PACK_DIR={pack_dir}")
    print(f"ARCHIVE_DIR={archive_dir}")
    print(f"SELECTED_SYMBOLS={','.join(selected_symbols)}")
    print(f"record_appended={kv.get('record_appended','')}")
    print(f"decision={kv.get('decision','')}")
    print("FINAL=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
