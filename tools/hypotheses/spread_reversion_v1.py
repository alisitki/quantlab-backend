#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
from dataclasses import dataclass
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
    "event_count",
    "mean_product",
    "t_stat",
]


@dataclass
class OnlineStats:
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add(self, x: float) -> None:
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2

    def final(self) -> Tuple[int, float, float]:
        if self.n <= 0:
            return 0, 0.0, 0.0
        if self.n <= 1:
            return self.n, self.mean, 0.0
        var = self.m2 / (self.n - 1)
        if var <= 0.0:
            return self.n, self.mean, 0.0
        std = math.sqrt(var)
        if std <= 0.0:
            return self.n, self.mean, 0.0
        return self.n, self.mean, self.mean / (std / math.sqrt(self.n))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="spread_reversion_v1 family runner")
    p.add_argument("--exchange", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--stream", required=True, help="bbo")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--srDeltaMsList", "--sr-delta-ms-list", "--delta-ms-list", dest="delta_ms_list", required=True)
    p.add_argument("--srHMsList", "--sr-h-ms-list", "--h-ms-list", dest="h_ms_list", required=True)
    p.add_argument("--results-out", "--resultsOut", dest="results_out", required=True)
    p.add_argument("--summary-out", "--summaryOut", dest="summary_out", required=True)
    p.add_argument("--report-out", "--reportOut", dest="report_out", default="")
    p.add_argument("--tolerance-ms", "--toleranceMs", dest="tolerance_ms", type=int, default=0)

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
                    int(r["event_count"]),
                    f"{float(r['mean_product']):.15f}",
                    f"{float(r['t_stat']):.15f}",
                ]
            )


def load_day_spread_events(
    repo: Path,
    exchange: str,
    symbol_slug: str,
    stream: str,
    day: str,
) -> Tuple[List[int], List[float], Optional[str]]:
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
    spread_raw: List[float] = []

    pf = pq.ParquetFile(parquet_path)
    for batch in pf.iter_batches(columns=["ts_event", "seq", "bid_price", "ask_price"], batch_size=131072):
        cols = batch.to_pydict()
        ts_col = cols["ts_event"]
        seq_col = cols["seq"]
        bid_col = cols["bid_price"]
        ask_col = cols["ask_price"]
        for i in range(len(ts_col)):
            ts = ts_col[i]
            seq = seq_col[i]
            bid = bid_col[i]
            ask = ask_col[i]
            if ts is None or seq is None or bid is None or ask is None:
                continue
            bid_f = float(bid)
            ask_f = float(ask)
            if bid_f <= 0.0 or ask_f <= 0.0 or ask_f < bid_f:
                continue
            mid = (bid_f + ask_f) / 2.0
            if mid <= 0.0:
                continue
            spread_bps = 10000.0 * (ask_f - bid_f) / mid
            ts_raw.append(int(ts))
            seq_raw.append(int(seq))
            spread_raw.append(spread_bps)

    idx = list(range(len(ts_raw)))
    idx.sort(key=lambda i: (ts_raw[i], seq_raw[i], i))
    ts_sorted = [ts_raw[i] for i in idx]
    spread_sorted = [spread_raw[i] for i in idx]
    return ts_sorted, spread_sorted, str(parquet_path.relative_to(repo)).replace("\\", "/")


def compute_day_rows(
    exchange: str,
    symbol: str,
    stream: str,
    day: str,
    delta_list: List[int],
    h_list: List[int],
    ts: List[int],
    spread: List[float],
    tolerance_ms: int,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    n = len(ts)
    tol = max(0, int(tolerance_ms))

    for delta_ms in delta_list:
        for h_ms in h_list:
            stats = OnlineStats()
            for i in range(n):
                t_cur = ts[i]
                s_cur = spread[i]

                target_prev = t_cur - delta_ms
                j = bisect.bisect_right(ts, target_prev, 0, i + 1) - 1
                if j < 0:
                    continue

                target_fwd = t_cur + h_ms
                k = bisect.bisect_left(ts, target_fwd, i, n)
                if k >= n:
                    continue

                if tol > 0:
                    if abs(ts[j] - target_prev) > tol:
                        continue
                    if abs(ts[k] - target_fwd) > tol:
                        continue

                s_prev = spread[j]
                s_fwd = spread[k]
                past_change = s_cur - s_prev
                fwd_change = s_fwd - s_cur
                stats.add(past_change * fwd_change)

            event_count, mean_product, t_stat = stats.final()
            rows.append(
                {
                    "exchange": exchange.lower(),
                    "symbol": symbol,
                    "date": day,
                    "stream": stream,
                    "delta_ms": int(delta_ms),
                    "h_ms": int(h_ms),
                    "event_count": int(event_count),
                    "mean_product": float(mean_product),
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
    candidates = [r for r in rows if int(r["event_count"]) > 0]
    if not candidates:
        return None
    candidates.sort(
        key=lambda r: (
            float(r["mean_product"]),
            float(r["t_stat"]),
            -int(r["event_count"]),
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
    tolerance_ms: int,
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
            "event_count": int(selected["event_count"]),
            "mean_product": float(f"{float(selected['mean_product']):.15f}"),
            "t_stat": float(f"{float(selected['t_stat']):.15f}"),
        }
        pass_signal = (
            int(selected["event_count"]) >= 200
            and float(selected["mean_product"]) < 0.0
            and float(selected["t_stat"]) <= -2.0
        )

    payload = {
        "family_id": "spread_reversion_v1",
        "status": status,
        "exchange": args.exchange,
        "symbol": args.symbol,
        "stream": args.stream.lower(),
        "window": f"{args.start}..{args.end}",
        "params": {
            "delta_ms_list": parse_csv_ints(args.delta_ms_list),
            "h_ms_list": parse_csv_ints(args.h_ms_list),
            "tolerance_ms": int(max(0, tolerance_ms)),
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
    tolerance_ms = max(0, int(args.tolerance_ms))
    days = ymd_days(args.start, args.end)
    symbol_slug = normalize_symbol(args.symbol)

    out_results = Path(args.results_out)
    out_summary = Path(args.summary_out)
    out_report = Path(args.report_out) if args.report_out else None

    if stream != "bbo":
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
                tolerance_ms=tolerance_ms,
            )
        print(f"RESULTS_OUT={out_results}")
        print(f"SUMMARY_OUT={out_summary}")
        if out_report is not None:
            print(f"REPORT_OUT={out_report}")
        return 0

    all_rows: List[Dict[str, object]] = []
    parquet_relpaths: List[str] = []
    for day in days:
        ts, spread, relpath = load_day_spread_events(
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
            spread=spread,
            tolerance_ms=tolerance_ms,
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
            tolerance_ms=tolerance_ms,
        )

    print(f"RESULTS_OUT={out_results}")
    print(f"SUMMARY_OUT={out_summary}")
    if out_report is not None:
        print(f"REPORT_OUT={out_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
