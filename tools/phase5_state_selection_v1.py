#!/usr/bin/env python3
"""State-driven selection helpers for Big Hunt scheduler."""

from __future__ import annotations

import csv
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple


@dataclass(frozen=True)
class InventoryRow:
    exchange: str
    stream: str
    symbol: str
    date: str
    status: str
    day_quality_post: str
    partition_key: str
    data_key: str
    meta_key: str
    bucket: str


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


def load_inventory(state_path: Path, *, bucket: str = "quantlab-compact") -> List[InventoryRow]:
    if not state_path.exists():
        raise FileNotFoundError(f"state_json_missing:{state_path}")
    obj = json.loads(state_path.read_text(encoding="utf-8"))
    parts = obj.get("partitions", {})
    if not isinstance(parts, dict):
        raise RuntimeError("invalid_state_json: partitions must be object")

    rows: List[InventoryRow] = []
    for partition_key, payload in parts.items():
        if not isinstance(partition_key, str):
            continue
        chunks = partition_key.split("/")
        if len(chunks) != 4:
            continue
        ex, stream, symbol, day = [str(c).strip().lower() for c in chunks]
        if len(day) != 8 or not day.isdigit():
            continue
        if not isinstance(payload, dict):
            payload = {}
        status = str(payload.get("status", "")).strip().lower()
        dqp = str(payload.get("day_quality_post", "")).strip().upper()
        data_key = f"exchange={ex}/stream={stream}/symbol={symbol}/date={day}/data.parquet"
        meta_key = f"exchange={ex}/stream={stream}/symbol={symbol}/date={day}/meta.json"
        rows.append(
            InventoryRow(
                exchange=ex,
                stream=stream,
                symbol=symbol,
                date=day,
                status=status,
                day_quality_post=dqp,
                partition_key=f"{ex}/{stream}/{symbol}/{day}",
                data_key=data_key,
                meta_key=meta_key,
                bucket=bucket,
            )
        )
    return rows


def filter_rows(
    rows: Sequence[InventoryRow],
    *,
    exchange: str,
    stream: str,
    start: str,
    end: str,
    require_status: str = "success",
    require_quality_pass: bool = True,
    max_symbols: int = 20,
) -> Tuple[List[InventoryRow], List[str], List[str]]:
    exchange = str(exchange).strip().lower()
    stream = str(stream).strip().lower()
    require_status = str(require_status).strip().lower()
    days = ymd_days(start, end)
    day_set = set(days)
    if max_symbols <= 0:
        raise ValueError("max_symbols must be > 0")

    scoped = [
        r
        for r in rows
        if r.exchange == exchange and r.stream == stream and start <= r.date <= end
    ]
    by_symbol_days: Dict[str, set[str]] = {}
    by_symbol_rows: Dict[str, List[InventoryRow]] = {}
    for r in scoped:
        if r.status != require_status:
            continue
        if require_quality_pass and r.day_quality_post == "BAD":
            continue
        by_symbol_days.setdefault(r.symbol, set()).add(r.date)
        by_symbol_rows.setdefault(r.symbol, []).append(r)

    eligible_symbols = sorted(
        sym for sym, covered_days in by_symbol_days.items() if day_set.issubset(covered_days)
    )
    selected_symbols = eligible_symbols[:max_symbols]

    selected_rows: List[InventoryRow] = []
    for sym in selected_symbols:
        sym_rows = by_symbol_rows.get(sym, [])
        # Include only required window days deterministically.
        wanted = {d for d in days}
        for r in sym_rows:
            if r.date in wanted:
                selected_rows.append(r)

    # deterministic canonical ordering
    selected_rows.sort(key=lambda r: (r.date, r.symbol, r.data_key))
    return selected_rows, selected_symbols, days


def build_object_keys_tsv(rows: Sequence[InventoryRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["label", "partition_key", "date", "data_key", "meta_key", "bucket"])
        day_order: Dict[str, int] = {}
        for r in rows:
            if r.date not in day_order:
                day_order[r.date] = len(day_order) + 1
            w.writerow(
                [
                    f"day{day_order[r.date]}",
                    r.partition_key,
                    r.date,
                    r.data_key,
                    r.meta_key,
                    r.bucket,
                ]
            )

