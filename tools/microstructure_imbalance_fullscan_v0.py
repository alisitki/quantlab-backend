#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import shlex
import subprocess
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FAMILY_ID = "microstructure_imbalance_v1"
RESULT_HEADER = [
    "exchange",
    "symbol",
    "date",
    "stream",
    "feature",
    "delta_ms",
    "h_ms",
    "pressure_threshold",
    "event_count",
    "mean_abs_pressure",
    "mean_signed_fwd_return_bps",
    "t_stat",
    "label",
]
QUALITY_OK = {"GOOD", "DEGRADED"}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run one Phase5 microstructure imbalance fullscan campaign")
    p.add_argument("--state-json", default="tools/microstructure_imbalance_fullscan_output/compacted_state_snapshot.json")
    p.add_argument("--out-dir", default="tools/microstructure_imbalance_fullscan_output/fullscan_trade")
    p.add_argument("--result-json", default="tools/microstructure_imbalance_fullscan_output/microstructure_imbalance_fullscan_result_v0.json")
    p.add_argument("--report-md", default="tools/microstructure_imbalance_fullscan_output/microstructure_imbalance_fullscan_report_v0.md")
    p.add_argument("--stream", default="trade")
    p.add_argument("--min-window-days", type=int, default=5)
    p.add_argument("--max-window-days", type=int, default=7)
    p.add_argument("--max-parallel", type=int, default=3)
    p.add_argument("--per-run-timeout-sec", type=int, default=1800)
    p.add_argument("--max-wall-sec", type=int, default=10800)
    p.add_argument("--bbo-max-bytes", type=int, default=1_000_000_000)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def add_days(day: str, n: int) -> str:
    base = dt.datetime.strptime(day, "%Y%m%d").date()
    return (base + dt.timedelta(days=n)).strftime("%Y%m%d")


def load_rows(state_path: Path, bucket_default: str = "quantlab-compact") -> list[dict[str, Any]]:
    obj = json.loads(state_path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for partition_key, payload in (obj.get("partitions") or {}).items():
        parts = str(partition_key).split("/")
        if len(parts) != 4:
            continue
        ex, stream, symbol, day = [x.strip().lower() for x in parts]
        if not day.isdigit() or len(day) != 8:
            continue
        payload = payload if isinstance(payload, dict) else {}
        rows.append(
            {
                "exchange": ex,
                "stream": stream,
                "symbol": symbol,
                "date": day,
                "status": str(payload.get("status", "")).strip().lower(),
                "day_quality_post": str(payload.get("day_quality_post", "")).strip().upper(),
                "partition_key": f"{ex}/{stream}/{symbol}/{day}",
                "data_key": f"exchange={ex}/stream={stream}/symbol={symbol}/date={day}/data.parquet",
                "meta_key": f"exchange={ex}/stream={stream}/symbol={symbol}/date={day}/meta.json",
                "bucket": bucket_default,
                "rows": int(payload.get("rows") or 0),
                "total_size_bytes": int(payload.get("total_size_bytes") or 0),
            }
        )
    return rows


def eligible_rows(rows: list[dict[str, Any]], stream: str) -> list[dict[str, Any]]:
    return [
        r
        for r in rows
        if r["stream"] == stream
        and r["status"] == "success"
        and r["day_quality_post"] in QUALITY_OK
    ]


def select_latest_window(rows: list[dict[str, Any]], *, min_days: int, max_days: int) -> dict[str, Any]:
    by_day: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for r in rows:
        by_day[r["date"]].add((r["exchange"], r["symbol"]))

    windows: list[dict[str, Any]] = []
    for start in sorted(by_day):
        for window_days in range(int(min_days), int(max_days) + 1):
            days = [add_days(start, i) for i in range(window_days)]
            if not all(day in by_day for day in days):
                continue
            pairs = set.intersection(*(by_day[day] for day in days))
            if not pairs:
                continue
            windows.append(
                {
                    "start": days[0],
                    "end": days[-1],
                    "days": days,
                    "window_days": window_days,
                    "pairs": sorted(pairs),
                }
            )
    if not windows:
        raise RuntimeError("no_valid_contiguous_window")
    return sorted(windows, key=lambda x: (x["end"], x["window_days"], x["start"]))[-1]


def rows_for_pair_days(rows: list[dict[str, Any]], pair: tuple[str, str], days: list[str]) -> list[dict[str, Any]]:
    exchange, symbol = pair
    wanted = set(days)
    out = [
        r
        for r in rows
        if r["exchange"] == exchange
        and r["symbol"] == symbol
        and r["date"] in wanted
    ]
    out.sort(key=lambda r: (r["date"], r["exchange"], r["symbol"], r["data_key"]))
    return out


def write_object_keys_tsv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = ["label", "partition_key", "date", "data_key", "meta_key", "bucket", "exchange", "stream", "symbol"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for idx, row in enumerate(rows, start=1):
            writer.writerow(
                {
                    "label": f"day{idx}",
                    "partition_key": row["partition_key"],
                    "date": row["date"],
                    "data_key": row["data_key"],
                    "meta_key": row["meta_key"],
                    "bucket": row["bucket"],
                    "exchange": row["exchange"],
                    "stream": row["stream"],
                    "symbol": row["symbol"],
                }
            )


def read_tsv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def write_tsv(path: Path, header: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in header})


def parse_time_v(path: Path) -> tuple[float, int]:
    if not path.exists():
        return 0.0, 0
    text = path.read_text(encoding="utf-8", errors="replace")
    elapsed = 0.0
    rss = 0
    for line in text.splitlines():
        if line.strip().startswith("Elapsed (wall clock) time"):
            raw = line.split(":", 1)[1].strip()
            parts = raw.split(":")
            try:
                if len(parts) == 2:
                    elapsed = int(parts[0]) * 60 + float(parts[1])
                elif len(parts) == 3:
                    elapsed = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
            except ValueError:
                elapsed = 0.0
        if line.strip().startswith("Maximum resident set size"):
            try:
                rss = int(line.rsplit(":", 1)[1].strip())
            except ValueError:
                rss = 0
    return elapsed, rss


def quantiles(vals: list[float]) -> dict[str, float | None]:
    clean = sorted(v for v in vals if math.isfinite(v))
    if not clean:
        return {"min": None, "p25": None, "median": None, "p75": None, "max": None}
    def q(p: float) -> float:
        if len(clean) == 1:
            return clean[0]
        pos = (len(clean) - 1) * p
        lo = math.floor(pos)
        hi = math.ceil(pos)
        if lo == hi:
            return clean[lo]
        return clean[lo] * (hi - pos) + clean[hi] * (pos - lo)
    return {"min": clean[0], "p25": q(0.25), "median": q(0.5), "p75": q(0.75), "max": clean[-1]}


def run_pair(
    *,
    pair_index: int,
    pair: tuple[str, str],
    days: list[str],
    rows: list[dict[str, Any]],
    out_dir: Path,
    per_run_timeout_sec: int,
) -> dict[str, Any]:
    exchange, symbol = pair
    run_name = f"{pair_index:03d}_{exchange}_{symbol}"
    run_dir = out_dir / "runs" / run_name
    object_keys = run_dir / "state_selection" / "object_keys_selected.tsv"
    pair_rows = rows_for_pair_days(rows, pair, days)
    write_object_keys_tsv(object_keys, pair_rows)
    time_log = run_dir / "time-v.log"
    stdout_log = run_dir / "stdout.log"
    stderr_log = run_dir / "stderr.log"
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "/usr/bin/time",
        "-v",
        "-o",
        str(time_log),
        "timeout",
        "--signal=INT",
        f"{int(per_run_timeout_sec)}s",
        "node",
        "tools/run-multi-hypothesis.js",
        "--exchange",
        exchange,
        "--stream",
        "trade",
        "--symbol",
        symbol,
        "--start",
        days[0],
        "--end",
        days[-1],
        "--objectKeysTsv",
        str(object_keys),
        "--outDir",
        str(run_dir),
        "--downloadsDir",
        str(run_dir / "downloads"),
        "--evidenceOn",
        "true",
        "--miDeltaMsList",
        "100,250,500",
        "--miHMsList",
        "100,250,500",
        "--miPressureThresholdList",
        "0.05,0.1,0.2",
    ]
    started = time.time()
    with stdout_log.open("w", encoding="utf-8") as out, stderr_log.open("w", encoding="utf-8") as err:
        proc = subprocess.run(cmd, cwd=str(ROOT), stdout=out, stderr=err, text=True)
    # Keep strict output slim: downloaded parquet is only a transport cache.
    downloads = run_dir / "downloads"
    if downloads.exists():
        subprocess.run(["rm", "-rf", str(downloads)], cwd=str(ROOT), check=False)
    elapsed, rss = parse_time_v(time_log)
    artifact_root = run_dir / "artifacts" / "multi_hypothesis"
    mi_results = artifact_root / "family_microstructure_imbalance_primary_results.tsv"
    mi_report = artifact_root / "family_microstructure_imbalance_report.json"
    mi_label = artifact_root / "family_microstructure_imbalance_label_report.json"
    det = artifact_root / "determinism_compare.tsv"
    selected_cell = None
    label_counts: dict[str, int] = {}
    pass_signal = False
    if mi_report.exists():
        try:
            obj = json.loads(mi_report.read_text(encoding="utf-8"))
            selected_cell = (obj.get("result") or {}).get("selected_cell")
            label_counts = dict((obj.get("result") or {}).get("label_counts") or {})
            pass_signal = bool((obj.get("result") or {}).get("pass_signal"))
        except json.JSONDecodeError:
            pass
    det_status = ""
    for row in read_tsv_rows(det):
        if row.get("family_id") == FAMILY_ID:
            det_status = row.get("determinism_status", "")
            break
    return {
        "pair_index": pair_index,
        "exchange": exchange,
        "symbol": symbol,
        "run_dir": str(run_dir),
        "object_keys_tsv": str(object_keys),
        "cmd": shlex.join(cmd),
        "exit_code": int(proc.returncode),
        "elapsed_sec_time_v": elapsed,
        "elapsed_sec_wall": round(time.time() - started, 3),
        "max_rss_kb": rss,
        "stdout_log": str(stdout_log),
        "stderr_log": str(stderr_log),
        "time_log": str(time_log),
        "microstructure_results_tsv": str(mi_results),
        "microstructure_report_json": str(mi_report),
        "microstructure_label_report_json": str(mi_label),
        "determinism_status": det_status,
        "selected_cell": selected_cell,
        "label_counts": label_counts,
        "pass_signal": pass_signal,
    }


def analyze_results(out_dir: Path, run_results: list[dict[str, Any]]) -> dict[str, Any]:
    artifact_dir = out_dir / "artifacts" / "microstructure_imbalance"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict[str, str]] = []
    symbol_rows: list[dict[str, Any]] = []
    for rr in sorted(run_results, key=lambda r: (r["exchange"], r["symbol"])):
        path = Path(rr["microstructure_results_tsv"])
        rows = read_tsv_rows(path)
        all_rows.extend(rows)
        counts = Counter(row.get("label", "") for row in rows)
        directional = counts.get("DIRECTIONAL", 0)
        anti = counts.get("ANTI_EDGE", 0)
        no_edge = counts.get("NO_EDGE", 0)
        insufficient = counts.get("INSUFFICIENT_SUPPORT", 0)
        selected = rr.get("selected_cell") or {}
        symbol_rows.append(
            {
                "exchange": rr["exchange"],
                "symbol": rr["symbol"],
                "exit_code": rr["exit_code"],
                "determinism_status": rr.get("determinism_status", ""),
                "directional_cells": directional,
                "anti_edge_cells": anti,
                "no_edge_cells": no_edge,
                "insufficient_support_cells": insufficient,
                "dominant_label": counts.most_common(1)[0][0] if counts else "",
                "selected_label": selected.get("label", ""),
                "selected_mean_signed_fwd_return_bps": selected.get("mean_signed_fwd_return_bps", ""),
                "selected_t_stat": selected.get("t_stat", ""),
                "selected_event_count": selected.get("event_count", ""),
                "selected_delta_ms": selected.get("delta_ms", ""),
                "selected_h_ms": selected.get("h_ms", ""),
                "selected_pressure_threshold": selected.get("pressure_threshold", ""),
            }
        )

    result_path = artifact_dir / "family_microstructure_imbalance_primary_results.tsv"
    summary_path = artifact_dir / "family_microstructure_imbalance_primary_summary.tsv"
    write_tsv(result_path, RESULT_HEADER, all_rows)
    write_tsv(summary_path, RESULT_HEADER, all_rows)

    label_counts = Counter(row.get("label", "") for row in all_rows)
    t_stats = [float(row["t_stat"]) for row in all_rows if row.get("t_stat", "")]
    means = [float(row["mean_signed_fwd_return_bps"]) for row in all_rows if row.get("mean_signed_fwd_return_bps", "")]
    events = [float(row["event_count"]) for row in all_rows if row.get("event_count", "")]
    directionals = [row for row in all_rows if row.get("label") == "DIRECTIONAL"]
    anti_edges = [row for row in all_rows if row.get("label") == "ANTI_EDGE"]
    cells_above_support = sum(1 for row in all_rows if int(float(row.get("event_count") or 0)) >= 200)
    symbols_with_directional = sorted({(row["exchange"], row["symbol"]) for row in directionals})
    symbols_with_anti = sorted({(row["exchange"], row["symbol"]) for row in anti_edges})
    total_cells = len(all_rows)
    dir_count = label_counts.get("DIRECTIONAL", 0)
    anti_count = label_counts.get("ANTI_EDGE", 0)
    no_edge_count = label_counts.get("NO_EDGE", 0)
    insufficient_count = label_counts.get("INSUFFICIENT_SUPPORT", 0)
    if dir_count >= 6 and len(symbols_with_directional) >= 3 and dir_count >= anti_count:
        classification = "STRONG_SIGNAL"
        classification_reason = "multiple symbols produced directional cells with sufficient support and directional count was not dominated by anti-edge cells"
    elif dir_count > 0:
        classification = "WEAK_SIGNAL"
        classification_reason = "some directional cells exist, but consistency or anti-edge dominance is not strong enough for STRONG_SIGNAL"
    else:
        classification = "NO_SIGNAL"
        classification_reason = "no DIRECTIONAL cells were produced; output is dominated by NO_EDGE/ANTI_EDGE/INSUFFICIENT_SUPPORT"

    symbol_rows.sort(
        key=lambda r: (
            -int(r["directional_cells"]),
            int(r["anti_edge_cells"]),
            -float(r["selected_mean_signed_fwd_return_bps"] or -1e9),
            r["exchange"],
            r["symbol"],
        )
    )
    write_tsv(
        artifact_dir / "symbol_rollup.tsv",
        [
            "exchange",
            "symbol",
            "exit_code",
            "determinism_status",
            "directional_cells",
            "anti_edge_cells",
            "no_edge_cells",
            "insufficient_support_cells",
            "dominant_label",
            "selected_label",
            "selected_mean_signed_fwd_return_bps",
            "selected_t_stat",
            "selected_event_count",
            "selected_delta_ms",
            "selected_h_ms",
            "selected_pressure_threshold",
        ],
        symbol_rows,
    )
    label_rows = [{"label": label, "count": count} for label, count in sorted(label_counts.items())]
    write_tsv(artifact_dir / "label_distribution.tsv", ["label", "count"], label_rows)

    report_payload = {
        "family_id": FAMILY_ID,
        "status": "ok",
        "result": {
            "rows_produced": total_cells,
            "label_counts": dict(sorted(label_counts.items())),
            "pass_signal": dir_count > 0,
            "selected_cell": max(
                directionals,
                key=lambda row: (
                    float(row.get("mean_signed_fwd_return_bps") or 0.0),
                    float(row.get("t_stat") or 0.0),
                    int(float(row.get("event_count") or 0)),
                ),
                default=None,
            ),
        },
    }
    label_payload = {
        "family_id": FAMILY_ID,
        "label_counts": dict(sorted(label_counts.items())),
        "criteria": {
            "DIRECTIONAL": "event_count >= 200 and mean_signed_fwd_return_bps >= 0.2 and t_stat >= 2.0",
            "ANTI_EDGE": "event_count >= 200 and mean_signed_fwd_return_bps <= -0.2 and t_stat <= -2.0",
            "NO_EDGE": "event_count >= 200 but no directional or anti-edge pass",
            "INSUFFICIENT_SUPPORT": "event_count < 200",
        },
    }
    (artifact_dir / "family_microstructure_imbalance_report.json").write_text(
        json.dumps(report_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "family_microstructure_imbalance_label_report.json").write_text(
        json.dumps(label_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return {
        "artifact_dir": str(artifact_dir),
        "primary_results_tsv": str(result_path),
        "primary_summary_tsv": str(summary_path),
        "report_json": str(artifact_dir / "family_microstructure_imbalance_report.json"),
        "label_report_json": str(artifact_dir / "family_microstructure_imbalance_label_report.json"),
        "symbol_rollup_tsv": str(artifact_dir / "symbol_rollup.tsv"),
        "label_distribution_tsv": str(artifact_dir / "label_distribution.tsv"),
        "total_cells": total_cells,
        "directional_cells": dir_count,
        "anti_edge_cells": anti_count,
        "no_edge_cells": no_edge_count,
        "insufficient_support_cells": insufficient_count,
        "directional_to_anti_edge_ratio": None if anti_count == 0 else dir_count / anti_count,
        "t_stat_distribution": quantiles(t_stats),
        "mean_signed_fwd_return_bps_distribution": quantiles(means),
        "event_count_distribution": quantiles(events),
        "cells_above_support_200": cells_above_support,
        "symbols_with_directional_cells": len(symbols_with_directional),
        "symbols_with_anti_edge_cells": len(symbols_with_anti),
        "best_symbols": symbol_rows[:5],
        "worst_symbols": sorted(
            symbol_rows,
            key=lambda r: (
                -int(r["anti_edge_cells"]),
                int(r["directional_cells"]),
                float(r["selected_mean_signed_fwd_return_bps"] or 0.0),
                r["exchange"],
                r["symbol"],
            ),
        )[:5],
        "classification": classification,
        "classification_reason": classification_reason,
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    analysis = payload["analysis"]
    plan = payload["campaign_plan"]
    lines = [
        "# Microstructure Imbalance Fullscan v0",
        "",
        f"- generated_ts_utc: `{payload['generated_ts_utc']}`",
        f"- status: `{payload['status']}`",
        f"- classification: `{analysis['classification']}`",
        f"- window: `{plan['start']}..{plan['end']}` ({plan['window_days']} days)",
        f"- trade pairs scanned: `{plan['target_pair_count']}`",
        f"- total cells: `{analysis['total_cells']}`",
        f"- directional cells: `{analysis['directional_cells']}`",
        f"- anti-edge cells: `{analysis['anti_edge_cells']}`",
        f"- no-edge cells: `{analysis['no_edge_cells']}`",
        f"- insufficient-support cells: `{analysis['insufficient_support_cells']}`",
        f"- symbols with directional cells: `{analysis['symbols_with_directional_cells']}`",
        f"- bbo decision: `{plan['bbo_resource_decision']['decision']}` - {plan['bbo_resource_decision']['reason']}",
        "",
        "## Best Symbols",
    ]
    for row in analysis["best_symbols"]:
        lines.append(
            f"- `{row['exchange']}/{row['symbol']}` directional={row['directional_cells']} anti={row['anti_edge_cells']} "
            f"selected={row['selected_label']} mean={row['selected_mean_signed_fwd_return_bps']} t={row['selected_t_stat']}"
        )
    lines.extend(["", "## Worst Symbols"])
    for row in analysis["worst_symbols"]:
        lines.append(
            f"- `{row['exchange']}/{row['symbol']}` anti={row['anti_edge_cells']} directional={row['directional_cells']} "
            f"selected={row['selected_label']} mean={row['selected_mean_signed_fwd_return_bps']} t={row['selected_t_stat']}"
        )
    lines.extend(
        [
            "",
            "## Decision",
            f"{analysis['classification_reason']}.",
            "",
            f"Next step: `{payload['next_step_recommendation']}`.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    out_dir = (ROOT / args.out_dir).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)
    result_json = (ROOT / args.result_json).resolve() if not Path(args.result_json).is_absolute() else Path(args.result_json)
    report_md = (ROOT / args.report_md).resolve() if not Path(args.report_md).is_absolute() else Path(args.report_md)
    state_path = (ROOT / args.state_json).resolve() if not Path(args.state_json).is_absolute() else Path(args.state_json)
    out_dir.mkdir(parents=True, exist_ok=True)
    result_json.parent.mkdir(parents=True, exist_ok=True)

    all_rows = load_rows(state_path)
    trade_rows = eligible_rows(all_rows, "trade")
    window = select_latest_window(trade_rows, min_days=args.min_window_days, max_days=args.max_window_days)
    days = list(window["days"])
    pairs = list(window["pairs"])
    selected_rows: list[dict[str, Any]] = []
    for pair in pairs:
        selected_rows.extend(rows_for_pair_days(trade_rows, pair, days))
    write_object_keys_tsv(out_dir / "state_selection" / "object_keys_selected.tsv", selected_rows)

    bbo_rows = eligible_rows(all_rows, "bbo")
    bbo_by_day: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for r in bbo_rows:
        if r["date"] in set(days):
            bbo_by_day[r["date"]].add((r["exchange"], r["symbol"]))
    bbo_pairs = sorted(set.intersection(*(bbo_by_day.get(day, set()) for day in days))) if all(day in bbo_by_day for day in days) else []
    bbo_selected = []
    for pair in bbo_pairs:
        bbo_selected.extend(rows_for_pair_days(bbo_rows, pair, days))
    bbo_bytes = sum(int(r["total_size_bytes"]) for r in bbo_selected)
    bbo_row_count = sum(int(r["rows"]) for r in bbo_selected)
    bbo_decision = {
        "decision": "SKIPPED_RESOURCE_SAFETY",
        "eligible_pair_count": len(bbo_pairs),
        "estimated_total_size_bytes": bbo_bytes,
        "estimated_row_count": bbo_row_count,
        "reason": f"full-window bbo estimate {bbo_bytes} bytes / {bbo_row_count} rows exceeds slim resource threshold {args.bbo_max_bytes} bytes",
    }

    campaign_plan = {
        "schema_version": "microstructure_imbalance_fullscan_plan_v0",
        "generated_ts_utc": utc_now_iso(),
        "state_json": str(state_path),
        "stream": "trade",
        "start": days[0],
        "end": days[-1],
        "window_days": len(days),
        "days": days,
        "target_pairs": [{"exchange": ex, "symbol": sym} for ex, sym in pairs],
        "target_pair_count": len(pairs),
        "selected_partition_count": len(selected_rows),
        "selected_total_size_bytes": sum(int(r["total_size_bytes"]) for r in selected_rows),
        "selected_total_rows": sum(int(r["rows"]) for r in selected_rows),
        "fixed_grid": {
            "delta_ms_list": [100, 250, 500],
            "h_ms_list": [100, 250, 500],
            "pressure_threshold_list": [0.05, 0.1, 0.2],
        },
        "runner": "tools/run-multi-hypothesis.js",
        "other_families_enabled_for_comparison": True,
        "bbo_resource_decision": bbo_decision,
        "max_parallel": int(args.max_parallel),
        "per_run_timeout_sec": int(args.per_run_timeout_sec),
        "max_wall_sec": int(args.max_wall_sec),
    }
    (out_dir / "campaign_plan.json").write_text(
        json.dumps(campaign_plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if args.dry_run:
        payload = {
            "schema_version": "microstructure_imbalance_fullscan_result_v0",
            "generated_ts_utc": utc_now_iso(),
            "status": "DRY_RUN",
            "campaign_plan": campaign_plan,
        }
        result_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"DRY_RUN target_pair_count={len(pairs)} window={days[0]}..{days[-1]}")
        return 0

    started = time.monotonic()
    run_results: list[dict[str, Any]] = []
    max_parallel = max(1, int(args.max_parallel))
    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        future_map = {
            pool.submit(
                run_pair,
                pair_index=i,
                pair=pair,
                days=days,
                rows=trade_rows,
                out_dir=out_dir,
                per_run_timeout_sec=int(args.per_run_timeout_sec),
            ): pair
            for i, pair in enumerate(pairs, start=1)
        }
        for future in as_completed(future_map):
            run_results.append(future.result())
            completed = len(run_results)
            print(f"MICROSTRUCTURE_FULLSCAN_PROGRESS completed={completed} remaining={len(pairs) - completed}")
            if time.monotonic() - started > int(args.max_wall_sec):
                print("MICROSTRUCTURE_FULLSCAN_WALL_EXCEEDED no_new_retries")
                break

    run_results.sort(key=lambda r: (r["exchange"], r["symbol"]))
    analysis = analyze_results(out_dir, run_results)
    failed = [r for r in run_results if int(r["exit_code"]) != 0]
    det_bad = [r for r in run_results if r.get("determinism_status") not in {"PASS", "SKIPPED_UNSUPPORTED_STREAM"}]
    status = "COMPLETED" if len(run_results) == len(pairs) and not failed and not det_bad else "PARTIAL_OR_FAILED"
    next_step = {
        "STRONG_SIGNAL": "proceed_to_phase6_selection",
        "WEAK_SIGNAL": "refine_family_or_run_targeted_confirmatory_discovery",
        "NO_SIGNAL": "discard_or_redesign_family_before_phase6",
    }[analysis["classification"]]
    payload = {
        "schema_version": "microstructure_imbalance_fullscan_result_v0",
        "generated_ts_utc": utc_now_iso(),
        "status": status,
        "campaign_plan": campaign_plan,
        "run_results": run_results,
        "failed_runs": failed,
        "determinism_issues": det_bad,
        "analysis": analysis,
        "next_step_recommendation": next_step,
    }
    result_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown(report_md, payload)
    print(f"MICROSTRUCTURE_FULLSCAN_COMPLETE status={status} classification={analysis['classification']}")
    print(f"RESULT_JSON={result_json}")
    print(f"REPORT_MD={report_md}")
    return 0 if status == "COMPLETED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
