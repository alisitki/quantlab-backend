#!/usr/bin/env python3
"""Build deterministic object_keys_selected.tsv from compacted/_state.json."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="State-driven state_selection builder")
    p.add_argument("--state-json", required=True, help="Path to compacted/_state.json")
    p.add_argument("--exchange", required=True, help="Exchange (e.g. binance)")
    p.add_argument("--stream", required=True, help="Stream (e.g. trade)")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--max-symbols", type=int, default=20)
    p.add_argument("--core-symbols", default="", help="CSV, always include first if present")
    p.add_argument("--bucket", default="quantlab-compact")
    p.add_argument("--s3-tool", default="/tmp/s3_compact_tool.py")
    p.add_argument("--output-tsv", required=True)
    p.add_argument("--selected-symbols-out", default="")
    p.add_argument("--exclude-bad-day-quality", action="store_true")
    p.add_argument("--skip-s3-head-check", action="store_true")
    return p.parse_args()


def ymd_days(start: str, end: str) -> List[str]:
    d0 = dt.datetime.strptime(start, "%Y%m%d").date()
    d1 = dt.datetime.strptime(end, "%Y%m%d").date()
    if d1 < d0:
        raise ValueError(f"invalid date range {start}..{end}")
    out: List[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%Y%m%d"))
        cur += dt.timedelta(days=1)
    return out


def norm_symbol(sym: str) -> str:
    return re.sub(r"[/_-]+", "", str(sym or "").strip().lower())


def head_exists(s3_tool: str, bucket: str, key: str) -> bool:
    cmd = ["python3", s3_tool, "head", bucket, key]
    res = subprocess.run(cmd, capture_output=True, text=True)
    return res.returncode == 0


def main() -> int:
    args = parse_args()
    state_path = Path(args.state_json).resolve()
    if not state_path.exists():
        raise FileNotFoundError(f"state_json_missing: {state_path}")

    obj = json.loads(state_path.read_text(encoding="utf-8"))
    parts = obj.get("partitions", {})
    if not isinstance(parts, dict):
        raise RuntimeError("invalid_state_json: partitions must be object")

    exchange = args.exchange.strip().lower()
    stream = args.stream.strip().lower()
    days = ymd_days(args.start, args.end)
    day_set = set(days)

    symbol_days: Dict[str, set[str]] = {}
    for part_key, payload in parts.items():
        if not isinstance(part_key, str):
            continue
        chunks = part_key.split("/")
        if len(chunks) != 4:
            continue
        ex, st, sym, day = [c.strip().lower() for c in chunks]
        if ex != exchange or st != stream:
            continue
        if day not in day_set:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("status", "")).strip().lower() != "success":
            continue
        if args.exclude_bad_day_quality:
            if str(payload.get("day_quality_post", "")).strip().upper() == "BAD":
                continue
        symbol_days.setdefault(sym, set()).add(day)

    eligible = sorted(sym for sym, dset in symbol_days.items() if day_set.issubset(dset))

    core_order: List[str] = []
    for raw in str(args.core_symbols or "").split(","):
        n = norm_symbol(raw)
        if n and n not in core_order:
            core_order.append(n)

    selected: List[str] = []
    for core in core_order:
        if core in eligible and core not in selected:
            selected.append(core)
    for sym in eligible:
        if sym not in selected:
            selected.append(sym)
    if args.max_symbols > 0:
        selected = selected[: args.max_symbols]

    if not selected:
        raise RuntimeError("no_symbols_selected_from_state")

    rows: List[dict] = []
    for sym in selected:
        for idx, day in enumerate(days, start=1):
            part_key = f"{exchange}/{stream}/{sym}/{day}"
            data_key = f"exchange={exchange}/stream={stream}/symbol={sym}/date={day}/data.parquet"
            meta_key = f"exchange={exchange}/stream={stream}/symbol={sym}/date={day}/meta.json"

            if not args.skip_s3_head_check:
                if not head_exists(args.s3_tool, args.bucket, data_key):
                    raise RuntimeError(
                        f"missing_data_key_from_s3 sym={sym} day={day} key={data_key}"
                    )
                if not head_exists(args.s3_tool, args.bucket, meta_key):
                    meta_key = ""

            rows.append(
                {
                    "label": f"day{idx}",
                    "partition_key": part_key,
                    "date": day,
                    "data_key": data_key,
                    "meta_key": meta_key,
                    "bucket": args.bucket,
                }
            )

    rows.sort(key=lambda r: (r["partition_key"], r["date"]))

    out_tsv = Path(args.output_tsv).resolve()
    out_tsv.parent.mkdir(parents=True, exist_ok=True)
    with out_tsv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["label", "partition_key", "date", "data_key", "meta_key", "bucket"])
        for r in rows:
            w.writerow(
                [
                    r["label"],
                    r["partition_key"],
                    r["date"],
                    r["data_key"],
                    r["meta_key"],
                    r["bucket"],
                ]
            )

    if args.selected_symbols_out:
        sym_out = Path(args.selected_symbols_out).resolve()
        sym_out.parent.mkdir(parents=True, exist_ok=True)
        sym_out.write_text("\n".join(selected) + "\n", encoding="utf-8")

    print(f"STATE_JSON={state_path}")
    print(f"OUTPUT_TSV={out_tsv}")
    print(f"WINDOW={args.start}..{args.end}")
    print(f"SELECTED_SYMBOL_COUNT={len(selected)}")
    print("SELECTED_SYMBOLS=" + ",".join(selected))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR={type(exc).__name__}:{exc}", file=sys.stderr)
        raise
