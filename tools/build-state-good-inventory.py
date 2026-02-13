#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


@dataclass
class PartitionRow:
    exchange: str
    stream: str
    symbol: str
    date: str
    rows: int | None
    total_size_bytes: int | None
    day_quality_post: str | None
    updated_at: str | None
    partition_key: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build SUCCESS+GOOD inventory and 2-day candidates from compaction state JSON."
    )
    p.add_argument("--state-json", required=True, help="Path to compaction state JSON")
    p.add_argument("--out-inventory", required=True, help="Output TSV path for filtered inventory")
    p.add_argument("--out-candidates", required=True, help="Output TSV path for 2-day candidates")
    p.add_argument("--exchange", default="", help="Optional exchange filter, e.g. binance")
    p.add_argument("--stream", default="", help="Optional stream filter, e.g. trade")
    p.add_argument("--day-quality-post", default="GOOD", help="Target day_quality_post value")
    p.add_argument("--top-n", type=int, default=20, help="Top-N candidates (asc rows_total) to include")
    return p.parse_args()


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_date_yyyymmdd(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d")


def iter_success_good_partitions(
    partitions: dict[str, Any],
    exchange_filter: str,
    stream_filter: str,
    day_quality_post: str,
) -> Iterable[PartitionRow]:
    exchange_filter = exchange_filter.strip().lower()
    stream_filter = stream_filter.strip().lower()
    day_quality_post = day_quality_post.strip()

    for key, meta in partitions.items():
        parts = key.split("/")
        if len(parts) != 4:
            continue
        exchange, stream, symbol, date = parts
        if exchange_filter and exchange.lower() != exchange_filter:
            continue
        if stream_filter and stream.lower() != stream_filter:
            continue
        status = str(meta.get("status", "")).lower()
        if status != "success":
            continue
        dqp = meta.get("day_quality_post")
        if dqp != day_quality_post:
            continue
        try:
            _ = parse_date_yyyymmdd(date)
        except ValueError:
            continue
        yield PartitionRow(
            exchange=exchange,
            stream=stream,
            symbol=symbol,
            date=date,
            rows=parse_int(meta.get("rows")),
            total_size_bytes=parse_int(meta.get("total_size_bytes")),
            day_quality_post=dqp,
            updated_at=meta.get("updated_at"),
            partition_key=key,
        )


def write_inventory(rows: list[PartitionRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(
            [
                "exchange",
                "stream",
                "symbol",
                "date",
                "rows",
                "total_size_bytes",
                "day_quality_post",
                "updated_at",
                "partition_key",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r.exchange,
                    r.stream,
                    r.symbol,
                    r.date,
                    "" if r.rows is None else r.rows,
                    "" if r.total_size_bytes is None else r.total_size_bytes,
                    r.day_quality_post or "",
                    r.updated_at or "",
                    r.partition_key,
                ]
            )


def candidate_sort_key(c: dict[str, Any]) -> tuple[int, int, str, str, str, str]:
    rows_total = c.get("rows_total")
    if rows_total is None:
        rows_rank = 1
        rows_value = 10**18
    else:
        rows_rank = 0
        rows_value = int(rows_total)
    return (
        rows_rank,
        rows_value,
        c["exchange"],
        c["stream"],
        c["symbol"],
        c["start"],
    )


def build_candidates(rows: list[PartitionRow], top_n: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[PartitionRow]] = {}
    for r in rows:
        grouped.setdefault((r.exchange, r.stream, r.symbol), []).append(r)

    candidates: list[dict[str, Any]] = []
    for (exchange, stream, symbol), g_rows in grouped.items():
        g_rows.sort(key=lambda x: x.date)
        for i in range(len(g_rows) - 1):
            a = g_rows[i]
            b = g_rows[i + 1]
            if parse_date_yyyymmdd(b.date) - parse_date_yyyymmdd(a.date) != timedelta(days=1):
                continue
            if a.rows is not None and b.rows is not None:
                rows_total: int | None = a.rows + b.rows
            else:
                rows_total = None

            updated_vals = [x for x in [a.updated_at, b.updated_at] if x]
            updated_at_min = min(updated_vals) if updated_vals else ""

            candidates.append(
                {
                    "exchange": exchange,
                    "stream": stream,
                    "symbol": symbol,
                    "start": a.date,
                    "end": b.date,
                    "rows_total": rows_total,
                    "day1_rows": a.rows,
                    "day2_rows": b.rows,
                    "updated_at_min": updated_at_min,
                }
            )

    candidates.sort(key=candidate_sort_key)
    if top_n > 0:
        return candidates[:top_n]
    return candidates


def write_candidates(candidates: list[dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(
            [
                "exchange",
                "stream",
                "symbol",
                "start",
                "end",
                "rows_total",
                "day1_rows",
                "day2_rows",
                "updated_at_min",
            ]
        )
        for c in candidates:
            w.writerow(
                [
                    c["exchange"],
                    c["stream"],
                    c["symbol"],
                    c["start"],
                    c["end"],
                    "" if c["rows_total"] is None else c["rows_total"],
                    "" if c["day1_rows"] is None else c["day1_rows"],
                    "" if c["day2_rows"] is None else c["day2_rows"],
                    c["updated_at_min"],
                ]
            )


def main() -> int:
    args = parse_args()
    state = json.loads(Path(args.state_json).read_text(encoding="utf-8"))
    partitions = state.get("partitions")
    if not isinstance(partitions, dict):
        raise SystemExit("state json missing 'partitions' dict")

    inventory = list(
        iter_success_good_partitions(
            partitions=partitions,
            exchange_filter=args.exchange,
            stream_filter=args.stream,
            day_quality_post=args.day_quality_post,
        )
    )
    inventory.sort(key=lambda r: (r.exchange, r.stream, r.symbol, r.date))
    write_inventory(inventory, Path(args.out_inventory))

    candidates = build_candidates(inventory, args.top_n)
    write_candidates(candidates, Path(args.out_candidates))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
