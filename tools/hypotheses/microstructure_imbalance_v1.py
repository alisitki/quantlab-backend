#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyarrow.parquet as pq


FAMILY_ID = "microstructure_imbalance_v1"

TSV_HEADER = [
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

SUPPORTED_STREAMS = {"trade", "bbo"}


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
    p = argparse.ArgumentParser(description="microstructure_imbalance_v1 family runner")
    p.add_argument("--exchange", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--stream", required=True, help="trade|bbo")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--miDeltaMsList", "--mi-delta-ms-list", "--delta-ms-list", dest="delta_ms_list", required=True)
    p.add_argument("--miHMsList", "--mi-h-ms-list", "--h-ms-list", dest="h_ms_list", required=True)
    p.add_argument(
        "--miPressureThresholdList",
        "--mi-pressure-threshold-list",
        "--pressure-threshold-list",
        dest="pressure_threshold_list",
        default="0.1,0.2",
    )
    p.add_argument("--miMinSupport", "--mi-min-support", dest="min_support", type=int, default=200)
    p.add_argument("--miMinEdgeBps", "--mi-min-edge-bps", dest="min_edge_bps", type=float, default=0.2)
    p.add_argument("--miMinTStat", "--mi-min-t-stat", dest="min_t_stat", type=float, default=2.0)
    p.add_argument("--results-out", "--resultsOut", dest="results_out", required=True)
    p.add_argument("--summary-out", "--summaryOut", dest="summary_out", required=True)
    p.add_argument("--report-out", "--reportOut", dest="report_out", default="")
    p.add_argument("--label-report-out", "--labelReportOut", dest="label_report_out", default="")
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
        if tok:
            out.append(int(tok))
    if not out:
        raise ValueError("empty integer list")
    return sorted(set(out))


def parse_csv_floats(raw: str) -> List[float]:
    out: List[float] = []
    for tok in str(raw).split(","):
        tok = tok.strip()
        if tok:
            out.append(float(tok))
    if not out:
        raise ValueError("empty float list")
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
                    str(r["feature"]),
                    int(r["delta_ms"]),
                    int(r["h_ms"]),
                    f"{float(r['pressure_threshold']):.6f}",
                    int(r["event_count"]),
                    f"{float(r['mean_abs_pressure']):.15f}",
                    f"{float(r['mean_signed_fwd_return_bps']):.15f}",
                    f"{float(r['t_stat']):.15f}",
                    str(r["label"]),
                ]
            )


def parquet_path_for(repo: Path, exchange: str, stream: str, symbol_slug: str, day: str) -> Path:
    return (
        repo
        / "data"
        / "curated"
        / f"exchange={exchange.lower()}"
        / f"stream={stream}"
        / f"symbol={symbol_slug}"
        / f"date={day}"
        / "data.parquet"
    )


def load_trade_events(
    repo: Path,
    exchange: str,
    symbol_slug: str,
    day: str,
) -> Tuple[List[int], List[float], List[float], List[float], Optional[str], str]:
    parquet_path = parquet_path_for(repo, exchange, "trade", symbol_slug, day)
    if not parquet_path.is_file():
        return [], [], [], [], str(parquet_path.relative_to(repo)).replace("\\", "/"), "missing_parquet"

    ts_raw: List[int] = []
    seq_raw: List[int] = []
    px_raw: List[float] = []
    pressure_value_raw: List[float] = []
    pressure_weight_raw: List[float] = []

    required = ["ts_event", "seq", "price", "qty", "side"]
    pf = pq.ParquetFile(parquet_path)
    names = set(pf.schema_arrow.names)
    missing = [name for name in required if name not in names]
    if missing:
        return [], [], [], [], str(parquet_path.relative_to(repo)).replace("\\", "/"), f"missing_columns:{','.join(missing)}"

    for batch in pf.iter_batches(columns=required, batch_size=131072):
        cols = batch.to_pydict()
        for i in range(len(cols["ts_event"])):
            ts = cols["ts_event"][i]
            seq = cols["seq"][i]
            px = cols["price"][i]
            qty = cols["qty"][i]
            side = cols["side"][i]
            if ts is None or seq is None or px is None or qty is None or side is None:
                continue
            px_f = float(px)
            qty_f = float(qty)
            side_f = float(side)
            if px_f <= 0.0 or qty_f <= 0.0 or side_f == 0.0:
                continue
            ts_raw.append(int(ts))
            seq_raw.append(int(seq))
            px_raw.append(px_f)
            pressure_value_raw.append(1.0 if side_f > 0.0 else -1.0)
            pressure_weight_raw.append(qty_f)

    idx = list(range(len(ts_raw)))
    idx.sort(key=lambda i: (ts_raw[i], seq_raw[i], i))
    return (
        [ts_raw[i] for i in idx],
        [px_raw[i] for i in idx],
        [pressure_value_raw[i] for i in idx],
        [pressure_weight_raw[i] for i in idx],
        str(parquet_path.relative_to(repo)).replace("\\", "/"),
        "ok",
    )


def load_bbo_events(
    repo: Path,
    exchange: str,
    symbol_slug: str,
    day: str,
) -> Tuple[List[int], List[float], List[float], List[float], Optional[str], str]:
    parquet_path = parquet_path_for(repo, exchange, "bbo", symbol_slug, day)
    if not parquet_path.is_file():
        return [], [], [], [], str(parquet_path.relative_to(repo)).replace("\\", "/"), "missing_parquet"

    ts_raw: List[int] = []
    seq_raw: List[int] = []
    mid_raw: List[float] = []
    pressure_value_raw: List[float] = []
    pressure_weight_raw: List[float] = []

    required = ["ts_event", "seq", "bid_price", "bid_qty", "ask_price", "ask_qty"]
    pf = pq.ParquetFile(parquet_path)
    names = set(pf.schema_arrow.names)
    missing = [name for name in required if name not in names]
    if missing:
        return [], [], [], [], str(parquet_path.relative_to(repo)).replace("\\", "/"), f"missing_columns:{','.join(missing)}"

    for batch in pf.iter_batches(columns=required, batch_size=131072):
        cols = batch.to_pydict()
        for i in range(len(cols["ts_event"])):
            ts = cols["ts_event"][i]
            seq = cols["seq"][i]
            bid = cols["bid_price"][i]
            ask = cols["ask_price"][i]
            bid_qty = cols["bid_qty"][i]
            ask_qty = cols["ask_qty"][i]
            if ts is None or seq is None or bid is None or ask is None or bid_qty is None or ask_qty is None:
                continue
            bid_f = float(bid)
            ask_f = float(ask)
            bid_qty_f = float(bid_qty)
            ask_qty_f = float(ask_qty)
            total_qty = bid_qty_f + ask_qty_f
            if bid_f <= 0.0 or ask_f <= 0.0 or ask_f < bid_f or total_qty <= 0.0:
                continue
            mid = (bid_f + ask_f) / 2.0
            if mid <= 0.0:
                continue
            imbalance = (bid_qty_f - ask_qty_f) / total_qty
            ts_raw.append(int(ts))
            seq_raw.append(int(seq))
            mid_raw.append(mid)
            pressure_value_raw.append(imbalance)
            pressure_weight_raw.append(1.0)

    idx = list(range(len(ts_raw)))
    idx.sort(key=lambda i: (ts_raw[i], seq_raw[i], i))
    return (
        [ts_raw[i] for i in idx],
        [mid_raw[i] for i in idx],
        [pressure_value_raw[i] for i in idx],
        [pressure_weight_raw[i] for i in idx],
        str(parquet_path.relative_to(repo)).replace("\\", "/"),
        "ok",
    )


def label_for(
    event_count: int,
    mean_signed_fwd_return_bps: float,
    t_stat: float,
    min_support: int,
    min_edge_bps: float,
    min_t_stat: float,
) -> str:
    if event_count < min_support:
        return "INSUFFICIENT_SUPPORT"
    if mean_signed_fwd_return_bps >= min_edge_bps and t_stat >= min_t_stat:
        return "DIRECTIONAL"
    if mean_signed_fwd_return_bps <= -min_edge_bps and t_stat <= -min_t_stat:
        return "ANTI_EDGE"
    return "NO_EDGE"


def compute_feature_rows(
    exchange: str,
    symbol: str,
    stream: str,
    feature: str,
    day: str,
    delta_list: List[int],
    h_list: List[int],
    pressure_threshold_list: List[float],
    ts: List[int],
    px: List[float],
    pressure_value: List[float],
    pressure_weight: List[float],
    tolerance_ms: int,
    min_support: int,
    min_edge_bps: float,
    min_t_stat: float,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    n = len(ts)
    tol = max(0, int(tolerance_ms))

    for delta_ms in delta_list:
        rolling_weighted_pressure = 0.0
        rolling_weight = 0.0
        left = 0
        rolling_pressure_by_index: List[float] = []

        for i in range(n):
            w = pressure_weight[i]
            rolling_weighted_pressure += pressure_value[i] * w
            rolling_weight += w
            min_ts = ts[i] - int(delta_ms)
            while left <= i and ts[left] < min_ts:
                old_w = pressure_weight[left]
                rolling_weighted_pressure -= pressure_value[left] * old_w
                rolling_weight -= old_w
                left += 1
            pressure = rolling_weighted_pressure / rolling_weight if rolling_weight > 0.0 else 0.0
            rolling_pressure_by_index.append(pressure)

        for h_ms in h_list:
            for threshold in pressure_threshold_list:
                signed_fwd_stats = OnlineStats()
                abs_pressure_stats = OnlineStats()
                threshold_abs = abs(float(threshold))

                for i in range(n):
                    p_cur = px[i]
                    pressure = rolling_pressure_by_index[i]
                    if p_cur <= 0.0 or abs(pressure) < threshold_abs or pressure == 0.0:
                        continue
                    target_fwd = ts[i] + int(h_ms)
                    k = bisect.bisect_left(ts, target_fwd, i + 1, n)
                    if k >= n:
                        continue
                    if tol > 0 and abs(ts[k] - target_fwd) > tol:
                        continue
                    p_fwd = px[k]
                    if p_fwd <= 0.0:
                        continue
                    fwd_return_bps = 10000.0 * (p_fwd - p_cur) / p_cur
                    signed_fwd_return_bps = (1.0 if pressure > 0.0 else -1.0) * fwd_return_bps
                    signed_fwd_stats.add(signed_fwd_return_bps)
                    abs_pressure_stats.add(abs(pressure))

                event_count, mean_signed, t_stat = signed_fwd_stats.final()
                _, mean_abs_pressure, _ = abs_pressure_stats.final()
                label = label_for(
                    event_count=event_count,
                    mean_signed_fwd_return_bps=mean_signed,
                    t_stat=t_stat,
                    min_support=min_support,
                    min_edge_bps=min_edge_bps,
                    min_t_stat=min_t_stat,
                )
                rows.append(
                    {
                        "exchange": exchange.lower(),
                        "symbol": symbol,
                        "date": day,
                        "stream": stream,
                        "feature": feature,
                        "delta_ms": int(delta_ms),
                        "h_ms": int(h_ms),
                        "pressure_threshold": float(threshold_abs),
                        "event_count": int(event_count),
                        "mean_abs_pressure": float(mean_abs_pressure),
                        "mean_signed_fwd_return_bps": float(mean_signed),
                        "t_stat": float(t_stat),
                        "label": label,
                    }
                )

    rows.sort(
        key=lambda r: (
            str(r["exchange"]),
            str(r["symbol"]),
            str(r["date"]),
            str(r["stream"]),
            str(r["feature"]),
            int(r["delta_ms"]),
            int(r["h_ms"]),
            float(r["pressure_threshold"]),
        )
    )
    return rows


def pick_selected_row(rows: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
    candidates = [r for r in rows if int(r["event_count"]) > 0]
    if not candidates:
        return None
    label_rank = {"DIRECTIONAL": 0, "ANTI_EDGE": 1, "NO_EDGE": 2, "INSUFFICIENT_SUPPORT": 3}

    def metric_key(row: Dict[str, object]) -> Tuple[float, float]:
        label = str(row["label"])
        mean = float(row["mean_signed_fwd_return_bps"])
        t_stat = float(row["t_stat"])
        if label == "DIRECTIONAL":
            return -mean, -t_stat
        if label == "ANTI_EDGE":
            return mean, t_stat
        if label == "NO_EDGE":
            return -abs(mean), -abs(t_stat)
        return 0.0, 0.0

    candidates.sort(
        key=lambda r: (
            label_rank.get(str(r["label"]), 9),
            *metric_key(r),
            -int(r["event_count"]),
            str(r["date"]),
            str(r["feature"]),
            int(r["delta_ms"]),
            int(r["h_ms"]),
            float(r["pressure_threshold"]),
        )
    )
    return candidates[0]


def selected_payload(row: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
    if row is None:
        return None
    return {
        "exchange": str(row["exchange"]),
        "symbol": str(row["symbol"]),
        "date": str(row["date"]),
        "stream": str(row["stream"]),
        "feature": str(row["feature"]),
        "delta_ms": int(row["delta_ms"]),
        "h_ms": int(row["h_ms"]),
        "pressure_threshold": float(f"{float(row['pressure_threshold']):.6f}"),
        "event_count": int(row["event_count"]),
        "mean_abs_pressure": float(f"{float(row['mean_abs_pressure']):.15f}"),
        "mean_signed_fwd_return_bps": float(f"{float(row['mean_signed_fwd_return_bps']):.15f}"),
        "t_stat": float(f"{float(row['t_stat']):.15f}"),
        "label": str(row["label"]),
    }


def write_reports(
    *,
    report_path: Optional[Path],
    label_report_path: Optional[Path],
    args: argparse.Namespace,
    rows: List[Dict[str, object]],
    selected: Optional[Dict[str, object]],
    status: str,
    parquet_relpaths: List[str],
    day_status_counts: Dict[str, int],
) -> None:
    selected_cell = selected_payload(selected)
    label_counts = dict(sorted(Counter(str(r["label"]) for r in rows).items()))
    pass_signal = bool(selected_cell and selected_cell.get("label") == "DIRECTIONAL")
    params = {
        "delta_ms_list": parse_csv_ints(args.delta_ms_list),
        "h_ms_list": parse_csv_ints(args.h_ms_list),
        "pressure_threshold_list": parse_csv_floats(args.pressure_threshold_list),
        "min_support": int(args.min_support),
        "min_edge_bps": float(args.min_edge_bps),
        "min_t_stat": float(args.min_t_stat),
        "tolerance_ms": int(max(0, args.tolerance_ms)),
    }
    report_payload = {
        "family_id": FAMILY_ID,
        "status": status,
        "exchange": args.exchange,
        "symbol": args.symbol,
        "stream": args.stream.lower(),
        "window": f"{args.start}..{args.end}",
        "params": params,
        "inputs": {
            "parquet_relpaths": parquet_relpaths,
            "day_status_counts": dict(sorted(day_status_counts.items())),
        },
        "mechanism": {
            "trade": "signed short-window aggressive volume pressure followed by signed forward return",
            "bbo": "top-of-book size imbalance pressure followed by signed forward mid return",
        },
        "result": {
            "rows_produced": len(rows),
            "selected_cell": selected_cell,
            "label_counts": label_counts,
            "pass_signal": pass_signal,
        },
    }
    label_payload = {
        "family_id": FAMILY_ID,
        "status": status,
        "label_counts": label_counts,
        "pass_signal": pass_signal,
        "criteria": {
            "DIRECTIONAL": "event_count >= min_support and mean_signed_fwd_return_bps >= min_edge_bps and t_stat >= min_t_stat",
            "ANTI_EDGE": "event_count >= min_support and mean_signed_fwd_return_bps <= -min_edge_bps and t_stat <= -min_t_stat",
            "NO_EDGE": "event_count >= min_support but no directional or anti-edge pass",
            "INSUFFICIENT_SUPPORT": "event_count < min_support",
        },
        "selected_label": selected_cell.get("label") if selected_cell else None,
    }
    if label_report_path is not None:
        ensure_parent(label_report_path)
        label_report_path.write_text(json.dumps(label_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        report_payload["files"] = {
            "label_report_relpath": str(label_report_path),
        }
    if report_path is not None:
        ensure_parent(report_path)
        report_path.write_text(json.dumps(report_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[2]
    stream = args.stream.strip().lower()
    delta_list = parse_csv_ints(args.delta_ms_list)
    h_list = parse_csv_ints(args.h_ms_list)
    pressure_threshold_list = parse_csv_floats(args.pressure_threshold_list)
    days = ymd_days(args.start, args.end)
    symbol_slug = normalize_symbol(args.symbol)

    out_results = Path(args.results_out)
    out_summary = Path(args.summary_out)
    out_report = Path(args.report_out) if args.report_out else None
    out_label_report = Path(args.label_report_out) if args.label_report_out else (
        out_report.with_name("family_microstructure_imbalance_label_report.json") if out_report is not None else None
    )

    if stream not in SUPPORTED_STREAMS:
        write_rows_tsv(out_results, [])
        write_rows_tsv(out_summary, [])
        write_reports(
            report_path=out_report,
            label_report_path=out_label_report,
            args=args,
            rows=[],
            selected=None,
            status="unsupported_stream",
            parquet_relpaths=[],
            day_status_counts={"unsupported_stream": 1},
        )
        print(f"RESULTS_OUT={out_results}")
        print(f"SUMMARY_OUT={out_summary}")
        if out_report is not None:
            print(f"REPORT_OUT={out_report}")
        if out_label_report is not None:
            print(f"LABEL_REPORT_OUT={out_label_report}")
        return 0

    all_rows: List[Dict[str, object]] = []
    parquet_relpaths: List[str] = []
    day_status_counts: Counter[str] = Counter()
    for day in days:
        if stream == "trade":
            ts, px, pressure_value, pressure_weight, relpath, load_status = load_trade_events(
                repo=repo,
                exchange=args.exchange,
                symbol_slug=symbol_slug,
                day=day,
            )
            feature = "signed_trade_volume_imbalance"
        else:
            ts, px, pressure_value, pressure_weight, relpath, load_status = load_bbo_events(
                repo=repo,
                exchange=args.exchange,
                symbol_slug=symbol_slug,
                day=day,
            )
            feature = "top_book_size_imbalance"
        if relpath:
            parquet_relpaths.append(relpath)
        day_status_counts[load_status] += 1
        if load_status != "ok":
            continue
        all_rows.extend(
            compute_feature_rows(
                exchange=args.exchange,
                symbol=args.symbol,
                stream=stream,
                feature=feature,
                day=day,
                delta_list=delta_list,
                h_list=h_list,
                pressure_threshold_list=pressure_threshold_list,
                ts=ts,
                px=px,
                pressure_value=pressure_value,
                pressure_weight=pressure_weight,
                tolerance_ms=max(0, int(args.tolerance_ms)),
                min_support=max(0, int(args.min_support)),
                min_edge_bps=max(0.0, float(args.min_edge_bps)),
                min_t_stat=max(0.0, float(args.min_t_stat)),
            )
        )

    all_rows.sort(
        key=lambda r: (
            str(r["exchange"]),
            str(r["symbol"]),
            str(r["date"]),
            str(r["stream"]),
            str(r["feature"]),
            int(r["delta_ms"]),
            int(r["h_ms"]),
            float(r["pressure_threshold"]),
        )
    )
    write_rows_tsv(out_results, all_rows)
    write_rows_tsv(out_summary, all_rows)

    selected = pick_selected_row(all_rows)
    status = "ok" if day_status_counts.get("ok", 0) > 0 else "no_supported_input"
    write_reports(
        report_path=out_report,
        label_report_path=out_label_report,
        args=args,
        rows=all_rows,
        selected=selected,
        status=status,
        parquet_relpaths=parquet_relpaths,
        day_status_counts=dict(day_status_counts),
    )

    print(f"RESULTS_OUT={out_results}")
    print(f"SUMMARY_OUT={out_summary}")
    if out_report is not None:
        print(f"REPORT_OUT={out_report}")
    if out_label_report is not None:
        print(f"LABEL_REPORT_OUT={out_label_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
