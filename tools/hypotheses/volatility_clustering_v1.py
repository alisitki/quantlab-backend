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
    "mean_rv_past",
    "mean_rv_fwd",
    "corr",
    "t_stat",
]

SUPPORTED_STREAM_COLUMNS = {
    "trade": "price",
    "mark_price": "mark_price",
}


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
    p = argparse.ArgumentParser(description="volatility_clustering_v1 family runner")
    p.add_argument("--exchange", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--stream", required=True, help="trade|mark_price")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--vcDeltaMsList", "--vc-delta-ms-list", "--delta-ms-list", dest="delta_ms_list", required=True)
    p.add_argument("--vcHMsList", "--vc-h-ms-list", "--h-ms-list", dest="h_ms_list", required=True)
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
                    f"{float(r['mean_rv_past']):.15f}",
                    f"{float(r['mean_rv_fwd']):.15f}",
                    f"{float(r['corr']):.15f}",
                    f"{float(r['t_stat']):.15f}",
                ]
            )


def load_day_events(
    repo: Path,
    exchange: str,
    symbol_slug: str,
    stream: str,
    day: str,
) -> Tuple[List[int], List[float], Optional[str]]:
    value_col = SUPPORTED_STREAM_COLUMNS.get(stream)
    if value_col is None:
        return [], [], None

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
        return [], [], str(parquet_path.relative_to(repo)).replace("\\", "/")

    ts_raw: List[int] = []
    seq_raw: List[int] = []
    val_raw: List[float] = []

    pf = pq.ParquetFile(parquet_path)
    for batch in pf.iter_batches(columns=["ts_event", "seq", value_col], batch_size=131072):
        cols = batch.to_pydict()
        ts_col = cols["ts_event"]
        seq_col = cols["seq"]
        px_col = cols[value_col]
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
            val_raw.append(px_f)

    idx = list(range(len(ts_raw)))
    idx.sort(key=lambda i: (ts_raw[i], seq_raw[i], i))
    ts_sorted = [ts_raw[i] for i in idx]
    val_sorted = [val_raw[i] for i in idx]
    return ts_sorted, val_sorted, str(parquet_path.relative_to(repo)).replace("\\", "/")


def compute_returns(ts: List[int], px: List[float]) -> Tuple[List[int], List[float]]:
    ret_ts: List[int] = []
    ret_sq: List[float] = []
    for i in range(1, len(ts)):
        prev = px[i - 1]
        cur = px[i]
        if prev <= 0.0:
            continue
        ret_bps = 10000.0 * (cur - prev) / prev
        ret_ts.append(ts[i])
        ret_sq.append(ret_bps * ret_bps)
    return ret_ts, ret_sq


def build_prefix(vals: List[float]) -> List[float]:
    out = [0.0]
    acc = 0.0
    for v in vals:
        acc += v
        out.append(acc)
    return out


def range_sum(prefix: List[float], lo: int, hi: int) -> float:
    if hi <= lo:
        return 0.0
    return prefix[hi] - prefix[lo]


def compute_day_rows(
    exchange: str,
    symbol: str,
    stream: str,
    day: str,
    delta_list: List[int],
    h_list: List[int],
    ts: List[int],
    px: List[float],
) -> List[Dict[str, object]]:
    ret_ts, ret_sq = compute_returns(ts, px)
    ret_prefix = build_prefix(ret_sq)
    rows: List[Dict[str, object]] = []

    if not ts or not ret_ts:
        for delta_ms in delta_list:
            for h_ms in h_list:
                rows.append(
                    {
                        "exchange": exchange.lower(),
                        "symbol": symbol,
                        "date": day,
                        "stream": stream,
                        "delta_ms": int(delta_ms),
                        "h_ms": int(h_ms),
                        "sample_count": 0,
                        "mean_rv_past": 0.0,
                        "mean_rv_fwd": 0.0,
                        "corr": 0.0,
                        "t_stat": 0.0,
                    }
                )
        return rows

    for delta_ms in delta_list:
        for h_ms in h_list:
            stats = CorrStats()
            for t in ts:
                past_start = t - delta_ms
                past_lo = bisect.bisect_right(ret_ts, past_start)
                past_hi = bisect.bisect_right(ret_ts, t)
                if past_hi <= past_lo:
                    continue

                fwd_lo = bisect.bisect_right(ret_ts, t)
                fwd_hi = bisect.bisect_right(ret_ts, t + h_ms)
                if fwd_hi <= fwd_lo:
                    continue

                rv_past = math.sqrt(max(0.0, range_sum(ret_prefix, past_lo, past_hi)))
                rv_fwd = math.sqrt(max(0.0, range_sum(ret_prefix, fwd_lo, fwd_hi)))
                stats.add(rv_past, rv_fwd)

            sample_count, mean_rv_past, mean_rv_fwd, corr, t_stat = stats.final()
            rows.append(
                {
                    "exchange": exchange.lower(),
                    "symbol": symbol,
                    "date": day,
                    "stream": stream,
                    "delta_ms": int(delta_ms),
                    "h_ms": int(h_ms),
                    "sample_count": int(sample_count),
                    "mean_rv_past": float(mean_rv_past),
                    "mean_rv_fwd": float(mean_rv_fwd),
                    "corr": float(corr),
                    "t_stat": float(t_stat),
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
            "mean_rv_past": float(f"{float(selected['mean_rv_past']):.15f}"),
            "mean_rv_fwd": float(f"{float(selected['mean_rv_fwd']):.15f}"),
            "corr": float(f"{float(selected['corr']):.15f}"),
            "t_stat": float(f"{float(selected['t_stat']):.15f}"),
        }
        pass_signal = (
            int(selected["sample_count"]) >= 200
            and float(selected["corr"]) > 0.0
            and float(selected["t_stat"]) >= 2.0
        )

    payload = {
        "family_id": "volatility_clustering_v1",
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

    if stream not in SUPPORTED_STREAM_COLUMNS:
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
            )
        print(f"RESULTS_OUT={out_results}")
        print(f"SUMMARY_OUT={out_summary}")
        if out_report is not None:
            print(f"REPORT_OUT={out_report}")
        return 0

    all_rows: List[Dict[str, object]] = []
    parquet_relpaths: List[str] = []
    for day in days:
        ts, px, relpath = load_day_events(
            repo=repo,
            exchange=args.exchange,
            symbol_slug=symbol_slug,
            stream=stream,
            day=day,
        )
        if relpath:
            parquet_relpaths.append(relpath)
        day_rows = compute_day_rows(
            exchange=args.exchange,
            symbol=args.symbol,
            stream=stream,
            day=day,
            delta_list=delta_list,
            h_list=h_list,
            ts=ts,
            px=px,
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
        )

    print(f"RESULTS_OUT={out_results}")
    print(f"SUMMARY_OUT={out_summary}")
    if out_report is not None:
        print(f"REPORT_OUT={out_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
