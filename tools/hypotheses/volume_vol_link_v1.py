#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyarrow.parquet as pq


TSV_HEADER = [
    "exchange",
    "symbol",
    "date",
    "stream",
    "delta_ms",
    "h_ms",
    "sample_count",
    "mean_activity",
    "mean_rv_fwd",
    "corr",
    "t_stat",
]

OPTIONAL_SIZE_COLUMNS = ["qty", "size", "quantity", "volume", "amount"]


class CorrStats:
    def __init__(self) -> None:
        self.n = 0
        self.sum_x = 0.0
        self.sum_y = 0.0
        self.sum_x2 = 0.0
        self.sum_y2 = 0.0
        self.sum_xy = 0.0

    def add(self, x: float, y: float) -> None:
        self.n += 1
        self.sum_x += x
        self.sum_y += y
        self.sum_x2 += x * x
        self.sum_y2 += y * y
        self.sum_xy += x * y

    def final(self) -> Tuple[int, float, float, float, float]:
        if self.n <= 0:
            return 0, 0.0, 0.0, 0.0, 0.0

        mean_x = self.sum_x / self.n
        mean_y = self.sum_y / self.n
        if self.n <= 2:
            return self.n, mean_x, mean_y, 0.0, 0.0

        sxx = self.sum_x2 - (self.sum_x * self.sum_x) / self.n
        syy = self.sum_y2 - (self.sum_y * self.sum_y) / self.n
        sxy = self.sum_xy - (self.sum_x * self.sum_y) / self.n
        if sxx <= 0.0 or syy <= 0.0:
            return self.n, mean_x, mean_y, 0.0, 0.0

        corr = sxy / math.sqrt(sxx * syy)
        if not math.isfinite(corr):
            corr = 0.0
        corr = max(-1.0, min(1.0, corr))

        denom = 1.0 - (corr * corr)
        if denom <= 0.0:
            t_stat = 0.0
        else:
            t_stat = corr * math.sqrt((self.n - 2.0) / denom)
            if not math.isfinite(t_stat):
                t_stat = 0.0
        return self.n, mean_x, mean_y, corr, t_stat


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="volume_vol_link_v1 family runner")
    p.add_argument("--exchange", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--stream", required=True, help="trade only")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--vvlDeltaMsList", "--vvl-delta-ms-list", "--delta-ms-list", dest="delta_ms_list", required=True)
    p.add_argument("--vvlHMsList", "--vvl-h-ms-list", "--h-ms-list", dest="h_ms_list", required=True)
    p.add_argument("--results-out", "--resultsOut", dest="results_out", required=True)
    p.add_argument("--summary-out", "--summaryOut", dest="summary_out", required=True)
    p.add_argument("--report-out", "--reportOut", dest="report_out", default="")

    # Compatibility pass-through args; currently unused.
    p.add_argument("--downloads-dir", "--downloadsDir", dest="downloads_dir", default="")
    p.add_argument("--object-keys-tsv", "--objectKeysTsv", dest="object_keys_tsv", default="")
    p.add_argument("--exchange-order", "--exchangeOrder", dest="exchange_order", default="")
    p.add_argument("--outDir", default="")
    return p.parse_args()


def parse_csv_ints(raw: str) -> List[int]:
    out: List[int] = []
    for tok in str(raw).split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(int(tok))
    if not out:
        raise ValueError("empty integer list")
    return sorted(set(out))


def normalize_symbol(sym: str) -> str:
    return str(sym).replace("/", "").replace("-", "").replace("_", "").strip().lower()


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


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_rows_tsv(path: Path, rows: List[Dict[str, object]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(TSV_HEADER)
        for r in rows:
            w.writerow(
                [
                    str(r["exchange"]),
                    str(r["symbol"]),
                    str(r["date"]),
                    str(r["stream"]),
                    int(r["delta_ms"]),
                    int(r["h_ms"]),
                    int(r["sample_count"]),
                    f"{float(r['mean_activity']):.15f}",
                    f"{float(r['mean_rv_fwd']):.15f}",
                    f"{float(r['corr']):.15f}",
                    f"{float(r['t_stat']):.15f}",
                ]
            )


def detect_optional_size_column(pf: pq.ParquetFile) -> str:
    names = [str(x) for x in pf.schema_arrow.names]
    lowered = {n.lower(): n for n in names}
    for cand in OPTIONAL_SIZE_COLUMNS:
        if cand in lowered:
            return lowered[cand]
    return ""


def load_day_events(
    repo: Path,
    exchange: str,
    symbol_slug: str,
    stream: str,
    day: str,
) -> Tuple[List[int], List[float], Optional[List[float]], Optional[str], str]:
    if stream != "trade":
        return [], [], None, None, ""

    parquet_path = (
        repo
        / "data"
        / "curated"
        / f"exchange={exchange.lower()}"
        / f"stream={stream}"
        / f"symbol={symbol_slug}"
        / f"date={day}"
        / "data.parquet"
    )
    if not parquet_path.is_file():
        return [], [], None, str(parquet_path.relative_to(repo)).replace("\\", "/"), ""

    ts_raw: List[int] = []
    seq_raw: List[int] = []
    px_raw: List[float] = []
    size_raw: List[float] = []
    have_size = False

    pf = pq.ParquetFile(parquet_path)
    size_col = detect_optional_size_column(pf)
    columns = ["ts_event", "seq", "price"]
    if size_col:
        columns.append(size_col)
        have_size = True

    for batch in pf.iter_batches(columns=columns, batch_size=131072):
        cols = batch.to_pydict()
        ts_col = cols["ts_event"]
        seq_col = cols["seq"]
        px_col = cols["price"]
        sz_col = cols.get(size_col, None) if size_col else None
        for i in range(len(ts_col)):
            ts = ts_col[i]
            seq = seq_col[i]
            px = px_col[i]
            if ts is None or seq is None or px is None:
                continue
            px_f = float(px)
            if px_f <= 0.0:
                continue

            ts_raw.append(int(ts))
            seq_raw.append(int(seq))
            px_raw.append(px_f)

            if have_size:
                raw_sz = sz_col[i] if sz_col is not None else None
                sz = 0.0 if raw_sz is None else float(raw_sz)
                if not math.isfinite(sz):
                    sz = 0.0
                size_raw.append(sz)

    idx = list(range(len(ts_raw)))
    idx.sort(key=lambda i: (ts_raw[i], seq_raw[i], i))

    ts_sorted = [ts_raw[i] for i in idx]
    px_sorted = [px_raw[i] for i in idx]
    size_sorted = [size_raw[i] for i in idx] if have_size else None
    return ts_sorted, px_sorted, size_sorted, str(parquet_path.relative_to(repo)).replace("\\", "/"), size_col


def build_prefix(vals: List[float]) -> List[float]:
    out = [0.0]
    acc = 0.0
    for v in vals:
        acc += v
        out.append(acc)
    return out


def range_sum(prefix: List[float], lo: int, hi_exclusive: int) -> float:
    if hi_exclusive <= lo:
        return 0.0
    return prefix[hi_exclusive] - prefix[lo]


def compute_day_rows(
    exchange: str,
    symbol: str,
    day: str,
    delta_list: List[int],
    h_list: List[int],
    ts: List[int],
    px: List[float],
    size: Optional[List[float]],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    n = len(ts)

    if n <= 1:
        for delta_ms in delta_list:
            for h_ms in h_list:
                rows.append(
                    {
                        "exchange": exchange.lower(),
                        "symbol": symbol,
                        "date": day,
                        "stream": "trade",
                        "delta_ms": int(delta_ms),
                        "h_ms": int(h_ms),
                        "sample_count": 0,
                        "mean_activity": 0.0,
                        "mean_rv_fwd": 0.0,
                        "corr": 0.0,
                        "t_stat": 0.0,
                        "mean_activity_size_past": 0.0,
                    }
                )
        return rows

    ret_sq = [0.0] * n
    for j in range(1, n):
        prev = px[j - 1]
        cur = px[j]
        if prev <= 0.0:
            continue
        ret_bps = 10000.0 * (cur - prev) / prev
        ret_sq[j] = ret_bps * ret_bps
    ret_prefix = build_prefix(ret_sq)
    size_prefix = build_prefix(size) if size is not None else None

    for delta_ms in delta_list:
        for h_ms in h_list:
            stats = CorrStats()
            size_sum_total = 0.0
            size_n = 0

            for i in range(1, n):
                t_cur = ts[i]

                left = bisect.bisect_right(ts, t_cur - delta_ms, 0, i + 1)
                past_activity = i - left + 1
                if past_activity <= 0:
                    continue

                fwd_lo = bisect.bisect_right(ts, t_cur, i + 1, n)
                fwd_hi_excl = bisect.bisect_right(ts, t_cur + h_ms, fwd_lo, n)
                if fwd_hi_excl <= fwd_lo:
                    continue

                sum_r2_fwd = range_sum(ret_prefix, fwd_lo, fwd_hi_excl)
                rv_fwd = math.sqrt(max(0.0, sum_r2_fwd))
                stats.add(float(past_activity), rv_fwd)

                if size_prefix is not None:
                    past_size_sum = range_sum(size_prefix, left, i + 1)
                    size_sum_total += past_size_sum
                    size_n += 1

            sample_count, mean_activity, mean_rv_fwd, corr, t_stat = stats.final()
            mean_activity_size_past = (size_sum_total / size_n) if size_n > 0 else 0.0

            rows.append(
                {
                    "exchange": exchange.lower(),
                    "symbol": symbol,
                    "date": day,
                    "stream": "trade",
                    "delta_ms": int(delta_ms),
                    "h_ms": int(h_ms),
                    "sample_count": int(sample_count),
                    "mean_activity": float(mean_activity),
                    "mean_rv_fwd": float(mean_rv_fwd),
                    "corr": float(corr),
                    "t_stat": float(t_stat),
                    "mean_activity_size_past": float(mean_activity_size_past),
                }
            )

    rows.sort(
        key=lambda r: (
            str(r["exchange"]),
            str(r["symbol"]),
            str(r["date"]),
            str(r["stream"]),
            int(r["delta_ms"]),
            int(r["h_ms"]),
        )
    )
    return rows


def pick_selected_row(rows: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
    candidates = [r for r in rows if int(r["sample_count"]) > 0]
    if not candidates:
        return None
    candidates.sort(
        key=lambda r: (
            -float(r["corr"]),
            -float(r["t_stat"]),
            -int(r["sample_count"]),
            str(r["date"]),
            int(r["delta_ms"]),
            int(r["h_ms"]),
        )
    )
    return candidates[0]


def write_optional_report(
    report_path: Path,
    args: argparse.Namespace,
    rows: List[Dict[str, object]],
    selected: Optional[Dict[str, object]],
    status: str,
    parquet_relpaths: List[str],
    optional_size_column: str,
) -> None:
    pass_signal = False
    selected_payload = None
    if selected is not None:
        selected_payload = {
            "exchange": str(selected["exchange"]),
            "symbol": str(selected["symbol"]),
            "date": str(selected["date"]),
            "stream": str(selected["stream"]),
            "delta_ms": int(selected["delta_ms"]),
            "h_ms": int(selected["h_ms"]),
            "sample_count": int(selected["sample_count"]),
            "mean_activity": float(f"{float(selected['mean_activity']):.15f}"),
            "mean_rv_fwd": float(f"{float(selected['mean_rv_fwd']):.15f}"),
            "corr": float(f"{float(selected['corr']):.15f}"),
            "t_stat": float(f"{float(selected['t_stat']):.15f}"),
            "mean_activity_size_past": float(f"{float(selected['mean_activity_size_past']):.15f}"),
        }
        pass_signal = (
            int(selected["sample_count"]) >= 200
            and float(selected["corr"]) > 0.0
            and float(selected["t_stat"]) >= 2.0
        )

    payload = {
        "family_id": "volume_vol_link_v1",
        "status": status,
        "exchange": args.exchange,
        "symbol": args.symbol,
        "stream": args.stream.lower(),
        "window": f"{args.start}..{args.end}",
        "params": {
            "delta_ms_list": parse_csv_ints(args.delta_ms_list),
            "h_ms_list": parse_csv_ints(args.h_ms_list),
        },
        "inputs": {
            "parquet_relpaths": parquet_relpaths,
            "optional_size_column": optional_size_column,
        },
        "result": {
            "rows_produced": len(rows),
            "selected_cell": selected_payload,
            "pass_signal": pass_signal,
        },
    }
    ensure_parent(report_path)
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[2]
    stream = args.stream.strip().lower()

    delta_list = parse_csv_ints(args.delta_ms_list)
    h_list = parse_csv_ints(args.h_ms_list)
    days = ymd_days(args.start, args.end)
    symbol_slug = normalize_symbol(args.symbol)

    out_results = Path(args.results_out)
    out_summary = Path(args.summary_out)
    out_report = Path(args.report_out) if args.report_out else None

    if stream != "trade":
        write_rows_tsv(out_results, [])
        write_rows_tsv(out_summary, [])
        if out_report is not None:
            write_optional_report(
                report_path=out_report,
                args=args,
                rows=[],
                selected=None,
                status="unsupported_stream",
                parquet_relpaths=[],
                optional_size_column="",
            )
        print(f"RESULTS_OUT={out_results}")
        print(f"SUMMARY_OUT={out_summary}")
        if out_report is not None:
            print(f"REPORT_OUT={out_report}")
        return 0

    all_rows: List[Dict[str, object]] = []
    parquet_relpaths: List[str] = []
    optional_size_column = ""
    for day in days:
        ts, px, size, relpath, size_col = load_day_events(
            repo=repo,
            exchange=args.exchange,
            symbol_slug=symbol_slug,
            stream=stream,
            day=day,
        )
        if relpath:
            parquet_relpaths.append(relpath)
        if size_col and not optional_size_column:
            optional_size_column = size_col
        day_rows = compute_day_rows(
            exchange=args.exchange,
            symbol=args.symbol,
            day=day,
            delta_list=delta_list,
            h_list=h_list,
            ts=ts,
            px=px,
            size=size,
        )
        all_rows.extend(day_rows)

    all_rows.sort(
        key=lambda r: (
            str(r["exchange"]),
            str(r["symbol"]),
            str(r["date"]),
            str(r["stream"]),
            int(r["delta_ms"]),
            int(r["h_ms"]),
        )
    )

    write_rows_tsv(out_results, all_rows)
    write_rows_tsv(out_summary, all_rows)

    if out_report is not None:
        selected = pick_selected_row(all_rows)
        write_optional_report(
            report_path=out_report,
            args=args,
            rows=all_rows,
            selected=selected,
            status="ok",
            parquet_relpaths=parquet_relpaths,
            optional_size_column=optional_size_column,
        )

    print(f"RESULTS_OUT={out_results}")
    print(f"SUMMARY_OUT={out_summary}")
    if out_report is not None:
        print(f"REPORT_OUT={out_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
