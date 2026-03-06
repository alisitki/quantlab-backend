#!/usr/bin/env python3
"""Build deterministic per-symbol auxiliary context artifacts for Phase-5 packs."""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pyarrow.parquet as pq


NA = "NA"
STATUS_OK = "OK"
STATUS_ABSENT = "ABSENT"
STATUS_UNSUPPORTED_EXCHANGE = "UNSUPPORTED_EXCHANGE"
DEFAULT_BUCKET = "quantlab-compact"
DEFAULT_S3_TOOL = "/tmp/s3_compact_tool.py"
SUPPORTED_OI_EXCHANGES = {"bybit", "okx"}

TSV_COLUMNS = [
    "exchange",
    "symbol",
    "date_start",
    "date_end",
    "core_stream",
    "ctx_mark_price_status",
    "ctx_mark_price_first",
    "ctx_mark_price_last",
    "ctx_mark_price_change_bps",
    "ctx_mark_trade_basis_mean_bps",
    "ctx_mark_trade_basis_max_abs_bps",
    "ctx_funding_status",
    "ctx_funding_count",
    "ctx_funding_first",
    "ctx_funding_last",
    "ctx_funding_mean",
    "ctx_funding_min",
    "ctx_funding_max",
    "ctx_oi_status",
    "ctx_oi_count",
    "ctx_oi_first",
    "ctx_oi_last",
    "ctx_oi_change_pct",
    "ctx_oi_min",
    "ctx_oi_max",
    "notes",
]


@dataclass(frozen=True)
class Point:
    ts_event: int
    seq: int
    value: float
    day: str


@dataclass(frozen=True)
class ResolvedInput:
    path: Path
    source: str


@dataclass
class SeriesLoad:
    points: List[Point]
    missing_days: List[str]
    empty_days: List[str]
    error_days: List[str]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build Context Pack v0 artifacts for one Phase-5 symbol run")
    p.add_argument("--exchange", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--core-stream", required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--selection-tsv", default="")
    p.add_argument("--downloads-dir", default="")
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--s3-tool", default=DEFAULT_S3_TOOL)
    p.add_argument("--curated-root", default="")
    return p.parse_args(argv)


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


def normalize_name(value: str) -> str:
    return str(value or "").strip().lower()


def parse_data_key_date(data_key: str) -> str:
    m = re.search(r"date=(\d{8})", str(data_key or ""))
    return m.group(1) if m else ""


def load_selection_buckets(path: Path) -> Dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"selection_tsv_missing:{path}")
    out: Dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            date = str(row.get("date", "")).strip() or parse_data_key_date(row.get("data_key", ""))
            if not re.fullmatch(r"\d{8}", date):
                continue
            bucket = str(row.get("bucket", "")).strip() or DEFAULT_BUCKET
            out.setdefault(date, bucket)
    return out


def is_nonempty_file(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def fixed15(value: float) -> str:
    return f"{float(value):.15f}"


def format_optional_float(value: Optional[float]) -> str:
    if value is None or not math.isfinite(value):
        return NA
    return fixed15(value)


def add_missing_day_notes(notes: set[str], prefix: str, load: SeriesLoad) -> None:
    if load.missing_days:
        notes.add(f"{prefix}_missing_days={','.join(sorted(load.missing_days))}")
    if load.empty_days:
        notes.add(f"{prefix}_empty_days={','.join(sorted(load.empty_days))}")
    if load.error_days:
        notes.add(f"{prefix}_error_days={','.join(sorted(load.error_days))}")


def status_from_load(load: SeriesLoad) -> str:
    if load.points and not load.missing_days and not load.empty_days and not load.error_days:
        return STATUS_OK
    return STATUS_ABSENT


def sort_points(points: List[Point]) -> List[Point]:
    indexed = list(enumerate(points))
    indexed.sort(key=lambda item: (item[1].ts_event, item[1].seq, item[0]))
    return [point for _, point in indexed]


def stream_value_column(stream: str) -> str:
    st = normalize_name(stream)
    if st == "trade":
        return "price"
    if st == "mark_price":
        return "mark_price"
    if st == "funding":
        return "funding_rate"
    if st == "open_interest":
        return "open_interest"
    raise ValueError(f"unsupported_stream:{stream}")


def read_stream_points(path: Path, stream: str, day: str) -> List[Point]:
    value_col = stream_value_column(stream)
    columns = ["ts_event", "seq", value_col]
    out: List[Point] = []
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(columns=columns, batch_size=131072):
        cols = batch.to_pydict()
        ts_col = cols.get("ts_event", [])
        seq_col = cols.get("seq", [])
        value_list = cols.get(value_col, [])
        for idx in range(len(ts_col)):
            ts_raw = ts_col[idx]
            seq_raw = seq_col[idx]
            value_raw = value_list[idx]
            if ts_raw is None or seq_raw is None or value_raw is None:
                continue
            value = float(value_raw)
            if not math.isfinite(value):
                continue
            if normalize_name(stream) in {"trade", "mark_price"} and value <= 0.0:
                continue
            out.append(Point(ts_event=int(ts_raw), seq=int(seq_raw), value=value, day=day))
    return sort_points(out)


def resolve_bucket(day: str, selection_buckets: Dict[str, str], default_bucket: str) -> str:
    bucket = selection_buckets.get(day, "")
    return bucket or default_bucket or DEFAULT_BUCKET


def resolve_day_input(
    *,
    exchange: str,
    stream: str,
    symbol: str,
    day: str,
    prefer_downloads: bool,
    downloads_dir: Optional[Path],
    curated_root: Path,
    default_bucket: str,
    selection_buckets: Dict[str, str],
    s3_tool: Path,
    temp_root: Path,
) -> Optional[ResolvedInput]:
    if prefer_downloads and downloads_dir is not None:
        download_path = downloads_dir / f"date={day}" / "data.parquet"
        if is_nonempty_file(download_path):
            return ResolvedInput(path=download_path, source="downloads")

    curated_path = (
        curated_root
        / f"exchange={exchange}"
        / f"stream={stream}"
        / f"symbol={symbol}"
        / f"date={day}"
        / "data.parquet"
    )
    if is_nonempty_file(curated_path):
        return ResolvedInput(path=curated_path, source="curated")

    if not s3_tool.exists():
        return None

    bucket = resolve_bucket(day, selection_buckets, default_bucket)
    key = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={day}/data.parquet"
    tmp_path = temp_root / f"exchange={exchange}" / f"stream={stream}" / f"symbol={symbol}" / f"date={day}" / "data.parquet"
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        ["python3", str(s3_tool), "get", bucket, key, str(tmp_path)],
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0 and is_nonempty_file(tmp_path):
        return ResolvedInput(path=tmp_path, source="s3")
    return None


def load_series(
    *,
    exchange: str,
    stream: str,
    symbol: str,
    days: Sequence[str],
    prefer_downloads: bool,
    downloads_dir: Optional[Path],
    curated_root: Path,
    default_bucket: str,
    selection_buckets: Dict[str, str],
    s3_tool: Path,
    temp_root: Path,
) -> SeriesLoad:
    points: List[Point] = []
    missing_days: List[str] = []
    empty_days: List[str] = []
    error_days: List[str] = []

    for day in days:
        resolved = resolve_day_input(
            exchange=exchange,
            stream=stream,
            symbol=symbol,
            day=day,
            prefer_downloads=prefer_downloads,
            downloads_dir=downloads_dir,
            curated_root=curated_root,
            default_bucket=default_bucket,
            selection_buckets=selection_buckets,
            s3_tool=s3_tool,
            temp_root=temp_root,
        )
        if resolved is None:
            missing_days.append(day)
            continue
        try:
            day_points = read_stream_points(resolved.path, stream, day)
        except Exception:
            error_days.append(day)
            continue
        if not day_points:
            empty_days.append(day)
            continue
        points.extend(day_points)

    return SeriesLoad(
        points=sort_points(points),
        missing_days=sorted(missing_days),
        empty_days=sorted(empty_days),
        error_days=sorted(error_days),
    )


def mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def compute_ratio_change(first: float, last: float, scale: float) -> Optional[float]:
    if not math.isfinite(first) or not math.isfinite(last) or first == 0.0:
        return None
    return scale * (last - first) / first


def point_arrays(points: Sequence[Point]) -> Tuple[List[int], List[int], List[float]]:
    ts_values: List[int] = []
    seq_values: List[int] = []
    scalar_values: List[float] = []
    for point in points:
        ts_values.append(int(point.ts_event))
        seq_values.append(int(point.seq))
        scalar_values.append(float(point.value))
    return ts_values, seq_values, scalar_values


def nearest_trade_index(trade_ts: Sequence[int], trade_seq: Sequence[int], query_ts: int) -> int:
    if not trade_ts:
        return -1
    pos = bisect.bisect_left(trade_ts, query_ts)
    candidates: List[Tuple[int, int, int, int]] = []
    if pos > 0:
        left_ts = int(trade_ts[pos - 1])
        left_seq = int(trade_seq[pos - 1])
        candidates.append((abs(query_ts - left_ts), left_ts, left_seq, pos - 1))
    if pos < len(trade_ts):
        right_ts = int(trade_ts[pos])
        right_seq = int(trade_seq[pos])
        candidates.append((abs(query_ts - right_ts), right_ts, right_seq, pos))
    candidates.sort()
    return candidates[0][3] if candidates else -1


def compute_basis_stats(mark_points: Sequence[Point], trade_points: Sequence[Point]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    if not trade_points:
        return None, None, "mark_trade_basis_unavailable=no_trade_rows"
    trade_ts, trade_seq, trade_values = point_arrays(trade_points)
    mark_ts, _, mark_values = point_arrays(mark_points)
    basis_values: List[float] = []
    for mark_ts_event, mark_value in zip(mark_ts, mark_values):
        idx = nearest_trade_index(trade_ts, trade_seq, mark_ts_event)
        if idx < 0:
            continue
        if mark_value <= 0.0:
            continue
        basis_values.append(10000.0 * (trade_values[idx] - mark_value) / mark_value)
    if not basis_values:
        return None, None, "mark_trade_basis_unavailable=no_aligned_pairs"
    basis_mean = mean(basis_values)
    basis_max_abs = max(abs(value) for value in basis_values)
    return basis_mean, basis_max_abs, None


def summarize_mark_price(
    *,
    exchange: str,
    symbol: str,
    core_stream: str,
    days: Sequence[str],
    downloads_dir: Optional[Path],
    curated_root: Path,
    default_bucket: str,
    selection_buckets: Dict[str, str],
    s3_tool: Path,
    temp_root: Path,
    notes: set[str],
) -> Dict[str, str]:
    load = load_series(
        exchange=exchange,
        stream="mark_price",
        symbol=symbol,
        days=days,
        prefer_downloads=False,
        downloads_dir=downloads_dir,
        curated_root=curated_root,
        default_bucket=default_bucket,
        selection_buckets=selection_buckets,
        s3_tool=s3_tool,
        temp_root=temp_root,
    )
    add_missing_day_notes(notes, "mark_price", load)
    status = status_from_load(load)
    row = {
        "ctx_mark_price_status": status,
        "ctx_mark_price_first": NA,
        "ctx_mark_price_last": NA,
        "ctx_mark_price_change_bps": NA,
        "ctx_mark_trade_basis_mean_bps": NA,
        "ctx_mark_trade_basis_max_abs_bps": NA,
    }
    if status != STATUS_OK:
        return row

    values = [point.value for point in load.points]
    first = values[0]
    last = values[-1]
    row["ctx_mark_price_first"] = format_optional_float(first)
    row["ctx_mark_price_last"] = format_optional_float(last)
    row["ctx_mark_price_change_bps"] = format_optional_float(compute_ratio_change(first, last, 10000.0))

    if normalize_name(core_stream) != "trade":
        return row

    trade_load = load_series(
        exchange=exchange,
        stream="trade",
        symbol=symbol,
        days=days,
        prefer_downloads=True,
        downloads_dir=downloads_dir,
        curated_root=curated_root,
        default_bucket=default_bucket,
        selection_buckets=selection_buckets,
        s3_tool=s3_tool,
        temp_root=temp_root,
    )
    if status_from_load(trade_load) != STATUS_OK:
        add_missing_day_notes(notes, "trade_for_basis", trade_load)
        notes.add("mark_trade_basis_unavailable=incomplete_trade_window")
        return row

    basis_mean, basis_max_abs, basis_note = compute_basis_stats(load.points, trade_load.points)
    row["ctx_mark_trade_basis_mean_bps"] = format_optional_float(basis_mean)
    row["ctx_mark_trade_basis_max_abs_bps"] = format_optional_float(basis_max_abs)
    if basis_note:
        notes.add(basis_note)
    return row


def summarize_funding(
    *,
    exchange: str,
    symbol: str,
    days: Sequence[str],
    downloads_dir: Optional[Path],
    curated_root: Path,
    default_bucket: str,
    selection_buckets: Dict[str, str],
    s3_tool: Path,
    temp_root: Path,
    notes: set[str],
) -> Dict[str, str]:
    load = load_series(
        exchange=exchange,
        stream="funding",
        symbol=symbol,
        days=days,
        prefer_downloads=False,
        downloads_dir=downloads_dir,
        curated_root=curated_root,
        default_bucket=default_bucket,
        selection_buckets=selection_buckets,
        s3_tool=s3_tool,
        temp_root=temp_root,
    )
    add_missing_day_notes(notes, "funding", load)
    row = {
        "ctx_funding_status": status_from_load(load),
        "ctx_funding_count": NA,
        "ctx_funding_first": NA,
        "ctx_funding_last": NA,
        "ctx_funding_mean": NA,
        "ctx_funding_min": NA,
        "ctx_funding_max": NA,
    }
    if row["ctx_funding_status"] != STATUS_OK:
        return row

    values = [point.value for point in load.points]
    row["ctx_funding_count"] = str(len(values))
    row["ctx_funding_first"] = format_optional_float(values[0])
    row["ctx_funding_last"] = format_optional_float(values[-1])
    row["ctx_funding_mean"] = format_optional_float(mean(values))
    row["ctx_funding_min"] = format_optional_float(min(values))
    row["ctx_funding_max"] = format_optional_float(max(values))
    return row


def summarize_open_interest(
    *,
    exchange: str,
    symbol: str,
    days: Sequence[str],
    downloads_dir: Optional[Path],
    curated_root: Path,
    default_bucket: str,
    selection_buckets: Dict[str, str],
    s3_tool: Path,
    temp_root: Path,
    notes: set[str],
) -> Dict[str, str]:
    row = {
        "ctx_oi_status": STATUS_UNSUPPORTED_EXCHANGE,
        "ctx_oi_count": NA,
        "ctx_oi_first": NA,
        "ctx_oi_last": NA,
        "ctx_oi_change_pct": NA,
        "ctx_oi_min": NA,
        "ctx_oi_max": NA,
    }
    if exchange not in SUPPORTED_OI_EXCHANGES:
        return row

    load = load_series(
        exchange=exchange,
        stream="open_interest",
        symbol=symbol,
        days=days,
        prefer_downloads=False,
        downloads_dir=downloads_dir,
        curated_root=curated_root,
        default_bucket=default_bucket,
        selection_buckets=selection_buckets,
        s3_tool=s3_tool,
        temp_root=temp_root,
    )
    add_missing_day_notes(notes, "open_interest", load)
    row["ctx_oi_status"] = status_from_load(load)
    if row["ctx_oi_status"] != STATUS_OK:
        return row

    values = [point.value for point in load.points]
    first = values[0]
    last = values[-1]
    row["ctx_oi_count"] = str(len(values))
    row["ctx_oi_first"] = format_optional_float(first)
    row["ctx_oi_last"] = format_optional_float(last)
    row["ctx_oi_min"] = format_optional_float(min(values))
    row["ctx_oi_max"] = format_optional_float(max(values))
    change_pct = None
    if first > 0.0:
        change_pct = compute_ratio_change(first, last, 100.0)
    else:
        notes.add("open_interest_change_pct_undefined=nonpositive_first")
    row["ctx_oi_change_pct"] = format_optional_float(change_pct)
    return row


def build_summary_row(
    *,
    exchange: str,
    symbol: str,
    core_stream: str,
    start: str,
    end: str,
    selection_buckets: Dict[str, str],
    downloads_dir: Optional[Path],
    curated_root: Path,
    default_bucket: str,
    s3_tool: Path,
) -> Dict[str, str]:
    notes: set[str] = set()
    days = ymd_days(start, end)
    with tempfile.TemporaryDirectory(prefix="context_pack_v0_") as tmp_dir:
        temp_root = Path(tmp_dir)
        row: Dict[str, str] = {
            "exchange": exchange,
            "symbol": symbol,
            "date_start": start,
            "date_end": end,
            "core_stream": core_stream,
        }
        row.update(
            summarize_mark_price(
                exchange=exchange,
                symbol=symbol,
                core_stream=core_stream,
                days=days,
                downloads_dir=downloads_dir,
                curated_root=curated_root,
                default_bucket=default_bucket,
                selection_buckets=selection_buckets,
                s3_tool=s3_tool,
                temp_root=temp_root,
                notes=notes,
            )
        )
        row.update(
            summarize_funding(
                exchange=exchange,
                symbol=symbol,
                days=days,
                downloads_dir=downloads_dir,
                curated_root=curated_root,
                default_bucket=default_bucket,
                selection_buckets=selection_buckets,
                s3_tool=s3_tool,
                temp_root=temp_root,
                notes=notes,
            )
        )
        row.update(
            summarize_open_interest(
                exchange=exchange,
                symbol=symbol,
                days=days,
                downloads_dir=downloads_dir,
                curated_root=curated_root,
                default_bucket=default_bucket,
                selection_buckets=selection_buckets,
                s3_tool=s3_tool,
                temp_root=temp_root,
                notes=notes,
            )
        )
    row["notes"] = ";".join(sorted(notes))
    for key in TSV_COLUMNS:
        row.setdefault(key, "")
    return row


def write_tsv(path: Path, row: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(TSV_COLUMNS)
        writer.writerow([str(row.get(col, "")) for col in TSV_COLUMNS])


def write_json(path: Path, row: Dict[str, str]) -> None:
    payload = {"schema_version": "context_pack_v0", "rows": [{col: str(row.get(col, "")) for col in TSV_COLUMNS}]}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    exchange = normalize_name(args.exchange)
    symbol = normalize_name(args.symbol)
    core_stream = normalize_name(args.core_stream)
    days = ymd_days(args.start, args.end)
    if not exchange or not symbol or not core_stream:
        raise ValueError("exchange/symbol/core-stream must be non-empty")

    out_dir = Path(args.out_dir).resolve()
    selection_buckets: Dict[str, str] = {}
    if str(args.selection_tsv).strip():
        selection_buckets = load_selection_buckets(Path(args.selection_tsv).resolve())
    for day in days:
        selection_buckets.setdefault(day, str(args.bucket or DEFAULT_BUCKET))

    downloads_dir = Path(args.downloads_dir).resolve() if str(args.downloads_dir).strip() else None
    curated_root = Path(args.curated_root).resolve() if str(args.curated_root).strip() else (Path(__file__).resolve().parents[1] / "data" / "curated")
    s3_tool = Path(args.s3_tool).resolve()

    row = build_summary_row(
        exchange=exchange,
        symbol=symbol,
        core_stream=core_stream,
        start=args.start,
        end=args.end,
        selection_buckets=selection_buckets,
        downloads_dir=downloads_dir,
        curated_root=curated_root,
        default_bucket=str(args.bucket or DEFAULT_BUCKET),
        s3_tool=s3_tool,
    )
    write_tsv(out_dir / "context_summary.tsv", row)
    write_json(out_dir / "context_summary.json", row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
