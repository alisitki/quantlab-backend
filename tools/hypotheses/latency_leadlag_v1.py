#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import itertools
import json
import math
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Dict, List, Set, Tuple

import boto3
import pyarrow.parquet as pq


@dataclass(frozen=True)
class GridMetric:
    pair: str
    delta_t_ms: int
    h_ms: int
    event_count: int
    mean_forward_return_bps: float
    t_stat: float
    metric_kind: str
    value_def: str


@dataclass(frozen=True)
class PairSupport:
    pair: str
    event_count_pair: int


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
        t_stat = self.mean / (std / math.sqrt(self.n))
        return self.n, self.mean, t_stat


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="latency_leadlag_v1 diagnostic runner")
    p.add_argument("--object-keys-tsv", required=True)
    p.add_argument("--downloads-dir", required=True)
    p.add_argument("--exchange-order", required=True, help="csv, e.g. binance,bybit,okx")
    p.add_argument("--symbol", required=True)
    p.add_argument("--stream", default="bbo")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--tolerance-ms", type=int, default=20)
    p.add_argument("--pair-mode", choices=["triad", "all6"], default="triad")
    p.add_argument("--cells_file", default="", help="Targeted mode TSV path")
    p.add_argument("--delta-ms-list", required=True, help="csv ints")
    p.add_argument("--h-ms-list", required=True, help="csv ints")
    p.add_argument("--results-out", required=True)
    p.add_argument("--pair-support-out", required=True)
    p.add_argument("--summary-out", required=True)
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
    return out


def normalize_symbol(sym: str) -> str:
    return str(sym).replace("/", "").replace("-", "").strip().lower()


def metric_contract(stream: str) -> Tuple[str, str]:
    st = stream.strip().lower()
    if st == "bbo":
        return "log_return", "mid"
    if st == "trade":
        return "log_return", "last"
    if st == "mark_price":
        return "log_return", "mark"
    if st == "funding":
        return "diff_bps", "funding_rate"
    raise ValueError(f"unsupported stream for latency_leadlag_v1: {stream}")


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


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k and k not in os.environ:
            os.environ[k] = v


def make_s3_client() -> boto3.client:
    repo = Path(__file__).resolve().parents[2]
    load_dotenv(repo / ".env")
    kwargs = {
        "endpoint_url": os.getenv("S3_COMPACT_ENDPOINT") or None,
        "region_name": os.getenv("S3_COMPACT_REGION") or "us-east-1",
    }
    ak = os.getenv("S3_COMPACT_ACCESS_KEY")
    sk = os.getenv("S3_COMPACT_SECRET_KEY")
    if ak and sk:
        kwargs["aws_access_key_id"] = ak
        kwargs["aws_secret_access_key"] = sk
    return boto3.client("s3", **kwargs)


def parse_tsv(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rows.append({k: (v.strip() if isinstance(v, str) else "") for k, v in row.items()})
    return rows


def parse_cells_file(
    path: Path,
    stream: str,
    symbol: str,
    expected_metric_kind: str,
    expected_value_def: str,
) -> Tuple[Dict[str, Set[Tuple[int, int]]], int]:
    rows = parse_tsv(path)
    stream_norm = stream.strip().lower()
    symbol_norm = normalize_symbol(symbol)

    out: Dict[str, Set[Tuple[int, int]]] = {}
    tol_vals: Set[int] = set()
    for r in rows:
        row_stream = r.get("stream", "").strip().lower()
        row_symbol = normalize_symbol(r.get("symbol", ""))
        if row_stream != stream_norm or row_symbol != symbol_norm:
            continue

        pair = r.get("pair", "").strip().lower()
        if not pair or "->" not in pair:
            raise ValueError(f"invalid pair in cells_file row: {r}")
        dt_ms = int(r.get("dt_ms", ""))
        h_ms = int(r.get("h_ms", ""))
        tol = int(r.get("tolerance_ms", ""))
        metric_kind = r.get("metric_kind", "").strip()
        value_def = r.get("value_def", "").strip()

        if metric_kind != expected_metric_kind or value_def != expected_value_def:
            raise ValueError(
                "cells_file metric contract mismatch "
                f"(expected {expected_metric_kind}/{expected_value_def}, got {metric_kind}/{value_def})"
            )

        out.setdefault(pair, set()).add((dt_ms, h_ms))
        tol_vals.add(tol)

    if not out:
        raise ValueError(f"cells_file has no rows for stream={stream_norm} symbol={symbol_norm}")
    if len(tol_vals) != 1:
        raise ValueError("cells_file must contain exactly one tolerance_ms value per run job")
    return out, list(tol_vals)[0]


def parse_object_keys(rows: List[Dict[str, str]]) -> Dict[Tuple[str, str], Tuple[str, str]]:
    out: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for r in rows:
        ex = r.get("exchange", "")
        date = r.get("date", "")
        data_key = r.get("data_key", "")
        bucket = r.get("bucket", "") or "quantlab-compact"

        if not ex and r.get("partition_key"):
            parts = r["partition_key"].split("/")
            if len(parts) == 4:
                ex = parts[0]
        if not date and data_key:
            for piece in data_key.split("/"):
                if piece.startswith("date="):
                    date = piece.split("=", 1)[1]
                    break

        if not ex or not date or not data_key:
            continue
        out[(ex.lower(), date)] = (bucket, data_key)
    return out


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def download_inputs(
    mapping: Dict[Tuple[str, str], Tuple[str, str]],
    exchanges: List[str],
    days: List[str],
    downloads_dir: Path,
) -> Dict[str, List[Path]]:
    s3 = make_s3_client()
    out: Dict[str, List[Path]] = {ex: [] for ex in exchanges}
    for ex in exchanges:
        for day in days:
            key = (ex, day)
            if key not in mapping:
                raise RuntimeError(f"missing_object_key exchange={ex} date={day}")
            bucket, s3_key = mapping[key]
            local = downloads_dir / f"exchange={ex}" / f"date={day}" / "data.parquet"
            ensure_parent(local)
            s3.download_file(bucket, s3_key, str(local))
            if not local.exists() or local.stat().st_size <= 0:
                raise RuntimeError(
                    f"download_failed_or_empty exchange={ex} date={day} bucket={bucket} key={s3_key}"
                )
            out[ex].append(local)
    return out


def load_exchange_events(paths: List[Path]) -> Tuple[List[int], List[int], List[float]]:
    raise RuntimeError("load_exchange_events(paths) signature changed; call with stream and value_def")


def load_exchange_events_by_stream(
    paths: List[Path],
    stream: str,
    value_def: str,
) -> Tuple[List[int], List[int], List[float]]:
    ts_out: List[int] = []
    seq_out: List[int] = []
    val_out: List[float] = []

    st = stream.strip().lower()
    if st == "bbo":
        columns = ["ts_event", "seq", "bid_price", "ask_price"]
    elif st == "trade":
        columns = ["ts_event", "seq", "price"]
    elif st == "mark_price":
        columns = ["ts_event", "seq", "mark_price"]
    elif st == "funding":
        columns = ["ts_event", "seq", "funding_rate"]
    else:
        raise ValueError(f"unsupported stream: {stream}")

    for p in paths:
        pf = pq.ParquetFile(p)
        for batch in pf.iter_batches(columns=columns, batch_size=131072):
            cols = batch.to_pydict()
            ts_col = cols["ts_event"]
            seq_col = cols["seq"]

            for i in range(len(ts_col)):
                ts = ts_col[i]
                seq = seq_col[i]
                if ts is None or seq is None:
                    continue

                val = None
                if st == "bbo":
                    bid = cols["bid_price"][i]
                    ask = cols["ask_price"][i]
                    if bid is None or ask is None:
                        continue
                    bid_f = float(bid)
                    ask_f = float(ask)
                    if bid_f <= 0.0 or ask_f <= 0.0:
                        continue
                    val = (bid_f + ask_f) / 2.0
                elif st == "trade":
                    price = cols["price"][i]
                    if price is None:
                        continue
                    val = float(price)
                elif st == "mark_price":
                    mark = cols["mark_price"][i]
                    if mark is None:
                        continue
                    val = float(mark)
                elif st == "funding":
                    fr = cols["funding_rate"][i]
                    if fr is None:
                        continue
                    val = float(fr)

                if val is None:
                    continue
                if value_def in {"mid", "last", "mark"} and val <= 0.0:
                    continue

                ts_out.append(int(ts))
                seq_out.append(int(seq))
                val_out.append(val)

    # Stable deterministic ordering by (ts, seq, idx)
    idx = list(range(len(ts_out)))
    idx.sort(key=lambda i: (ts_out[i], seq_out[i], i))
    ts_sorted = [ts_out[i] for i in idx]
    seq_sorted = [seq_out[i] for i in idx]
    val_sorted = [val_out[i] for i in idx]
    return ts_sorted, seq_sorted, val_sorted


def nearest_index_within_tol(
    target_ts: List[int],
    target_seq: List[int],
    query_ts: int,
    tol_ms: int,
) -> int:
    n = len(target_ts)
    if n == 0:
        return -1
    pos = bisect.bisect_left(target_ts, query_ts)
    left = pos - 1
    right = pos

    best = -1
    best_diff = None
    best_ts = None
    best_seq = None

    if left >= 0:
        diff = abs(query_ts - target_ts[left])
        if diff <= tol_ms:
            best = left
            best_diff = diff
            best_ts = target_ts[left]
            best_seq = target_seq[left]

    if right < n:
        diff = abs(target_ts[right] - query_ts)
        if diff <= tol_ms:
            if best == -1:
                best = right
                best_diff = diff
                best_ts = target_ts[right]
                best_seq = target_seq[right]
            else:
                cand = (diff, target_ts[right], target_seq[right], right)
                cur = (best_diff, best_ts, best_seq, best)
                if cand < cur:
                    best = right
                    best_diff = diff
                    best_ts = target_ts[right]
                    best_seq = target_seq[right]
    return best


def compute_pair_metrics(
    source_ts: List[int],
    source_val: List[float],
    target_ts: List[int],
    target_seq: List[int],
    target_val: List[float],
    pair_name: str,
    metric_kind: str,
    value_def: str,
    delta_list: List[int],
    h_list: List[int],
    tolerance_ms: int,
    targeted_cells: Set[Tuple[int, int]] | None = None,
) -> Tuple[List[GridMetric], PairSupport]:
    if targeted_cells is not None and len(targeted_cells) == 0:
        return [], PairSupport(pair=pair_name, event_count_pair=0)

    if targeted_cells is None:
        cell_keys = [(dt_ms, h_ms) for dt_ms in delta_list for h_ms in h_list]
    else:
        cell_keys = sorted(targeted_cells, key=lambda x: (x[0], x[1]))
    dt_to_hs: Dict[int, List[int]] = {}
    for dt_ms, h_ms in cell_keys:
        dt_to_hs.setdefault(dt_ms, []).append(h_ms)
    dt_iter = sorted(dt_to_hs.keys())

    if len(source_ts) < 2 or len(target_ts) < 2:
        metrics = [
            GridMetric(
                pair=pair_name,
                delta_t_ms=dt_ms,
                h_ms=h_ms,
                event_count=0,
                mean_forward_return_bps=0.0,
                t_stat=0.0,
                metric_kind=metric_kind,
                value_def=value_def,
            )
            for (dt_ms, h_ms) in cell_keys
        ]
        return metrics, PairSupport(pair=pair_name, event_count_pair=0)

    signal_ts: List[int] = []
    signal_sign: List[int] = []
    for i in range(1, len(source_ts)):
        prev_val = source_val[i - 1]
        cur_val = source_val[i]
        if metric_kind == "log_return":
            if prev_val <= 0.0 or cur_val <= 0.0:
                continue
            lr = math.log(cur_val / prev_val)
            if lr > 0.0:
                signal_ts.append(source_ts[i])
                signal_sign.append(1)
            elif lr < 0.0:
                signal_ts.append(source_ts[i])
                signal_sign.append(-1)
        elif metric_kind == "diff_bps":
            delta = cur_val - prev_val
            if delta > 0.0:
                signal_ts.append(source_ts[i])
                signal_sign.append(1)
            elif delta < 0.0:
                signal_ts.append(source_ts[i])
                signal_sign.append(-1)
        else:
            raise ValueError(f"unsupported metric_kind: {metric_kind}")

    if not signal_ts:
        metrics = [
            GridMetric(
                pair=pair_name,
                delta_t_ms=dt_ms,
                h_ms=h_ms,
                event_count=0,
                mean_forward_return_bps=0.0,
                t_stat=0.0,
                metric_kind=metric_kind,
                value_def=value_def,
            )
            for (dt_ms, h_ms) in cell_keys
        ]
        return metrics, PairSupport(pair=pair_name, event_count_pair=0)

    cell_stats: Dict[Tuple[int, int], OnlineStats] = {}
    for dt_ms, h_ms in cell_keys:
        cell_stats[(dt_ms, h_ms)] = OnlineStats()

    valid_src_any = [False] * len(signal_ts)

    for si, t0 in enumerate(signal_ts):
        sgn = signal_sign[si]
        src_valid = False
        for dt_ms in dt_iter:
            target_time = t0 + dt_ms
            j = nearest_index_within_tol(target_ts, target_seq, target_time, tolerance_ms)
            if j < 0:
                continue
            val1 = target_val[j]
            if metric_kind == "log_return" and val1 <= 0.0:
                continue

            t1 = target_ts[j]
            for h_ms in dt_to_hs[dt_ms]:
                k = bisect.bisect_left(target_ts, t1 + h_ms, lo=j)
                if k >= len(target_ts):
                    continue
                val2 = target_val[k]
                if metric_kind == "log_return" and val2 <= 0.0:
                    continue
                if metric_kind == "log_return":
                    rb = 10000.0 * math.log(val2 / val1)
                else:
                    rb = 10000.0 * (val2 - val1)
                rsigned = sgn * rb
                cell_stats[(dt_ms, h_ms)].add(rsigned)
                src_valid = True
        if src_valid:
            valid_src_any[si] = True

    out_metrics: List[GridMetric] = []
    for dt_ms, h_ms in cell_keys:
        n, mean, t = cell_stats[(dt_ms, h_ms)].final()
        out_metrics.append(
            GridMetric(
                pair=pair_name,
                delta_t_ms=dt_ms,
                h_ms=h_ms,
                event_count=n,
                mean_forward_return_bps=mean,
                t_stat=t,
                metric_kind=metric_kind,
                value_def=value_def,
            )
        )

    pair_support = sum(1 for x in valid_src_any if x)
    return out_metrics, PairSupport(pair=pair_name, event_count_pair=pair_support)


def write_results(path: Path, window: str, metrics: List[GridMetric], determinism_status: str = "PENDING") -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(
            [
                "window",
                "pair",
                "delta_t_ms",
                "h_ms",
                "event_count",
                "mean_forward_return_bps",
                "t_stat",
                "metric_kind",
                "value_def",
                "determinism_status",
            ]
        )
        for m in sorted(metrics, key=lambda x: (x.pair, x.delta_t_ms, x.h_ms)):
            w.writerow(
                [
                    window,
                    m.pair,
                    m.delta_t_ms,
                    m.h_ms,
                    m.event_count,
                    f"{m.mean_forward_return_bps:.15f}",
                    f"{m.t_stat:.15f}",
                    m.metric_kind,
                    m.value_def,
                    determinism_status,
                ]
            )


def write_pair_support(path: Path, window: str, supports: List[PairSupport]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["window", "pair", "event_count_pair"])
        for s in sorted(supports, key=lambda x: x.pair):
            w.writerow([window, s.pair, s.event_count_pair])


def metrics_hash(metrics: List[GridMetric]) -> str:
    rows = [
        {
            "pair": m.pair,
            "delta_t_ms": int(m.delta_t_ms),
            "h_ms": int(m.h_ms),
            "event_count": int(m.event_count),
            "mean_forward_return_bps": f"{m.mean_forward_return_bps:.15f}",
            "t_stat": f"{m.t_stat:.15f}",
            "metric_kind": m.metric_kind,
            "value_def": m.value_def,
        }
        for m in sorted(metrics, key=lambda x: (x.pair, x.delta_t_ms, x.h_ms))
    ]
    blob = json.dumps(rows, separators=(",", ":"), ensure_ascii=True)
    return sha256(blob.encode("utf-8")).hexdigest()


def main() -> int:
    args = parse_args()

    exchange_order = [x.strip().lower() for x in args.exchange_order.split(",") if x.strip()]
    if len(exchange_order) != 3:
        raise SystemExit("exchange-order must contain exactly 3 exchanges")

    stream = args.stream.strip().lower()
    metric_kind, value_def = metric_contract(stream)
    delta_list = parse_csv_ints(args.delta_ms_list)
    h_list = parse_csv_ints(args.h_ms_list)
    days = ymd_days(args.start, args.end)
    if len(days) not in {1, 2}:
        raise SystemExit("this diagnostic runner expects 1 or 2 days")

    rows = parse_tsv(Path(args.object_keys_tsv))
    mapping = parse_object_keys(rows)

    downloads_dir = Path(args.downloads_dir)
    resolved = download_inputs(mapping, exchange_order, days, downloads_dir)

    targeted_cells_by_pair: Dict[str, Set[Tuple[int, int]]] | None = None
    if args.cells_file:
        targeted_cells_by_pair, tol_from_cells = parse_cells_file(
            path=Path(args.cells_file),
            stream=stream,
            symbol=args.symbol,
            expected_metric_kind=metric_kind,
            expected_value_def=value_def,
        )
        if int(tol_from_cells) != int(args.tolerance_ms):
            raise SystemExit(
                f"cells_file tolerance mismatch: file={tol_from_cells} cli={int(args.tolerance_ms)}"
            )

    events: Dict[str, Tuple[List[int], List[int], List[float]]] = {}
    for ex in exchange_order:
        events[ex] = load_exchange_events_by_stream(
            paths=resolved[ex],
            stream=stream,
            value_def=value_def,
        )

    if args.pair_mode == "triad":
        pairs = [
            (exchange_order[0], exchange_order[1]),
            (exchange_order[0], exchange_order[2]),
            (exchange_order[1], exchange_order[2]),
        ]
    else:
        pairs = [(src, dst) for src, dst in itertools.permutations(exchange_order, 2)]

    all_metrics: List[GridMetric] = []
    all_support: List[PairSupport] = []
    for src, dst in pairs:
        src_ts, _src_seq, src_vals = events[src]
        dst_ts, dst_seq, dst_vals = events[dst]
        pair_name = f"{src}->{dst}"
        targeted_for_pair = None
        if targeted_cells_by_pair is not None:
            targeted_for_pair = targeted_cells_by_pair.get(pair_name)
            if not targeted_for_pair:
                continue
        metrics, support = compute_pair_metrics(
            source_ts=src_ts,
            source_val=src_vals,
            target_ts=dst_ts,
            target_seq=dst_seq,
            target_val=dst_vals,
            pair_name=pair_name,
            metric_kind=metric_kind,
            value_def=value_def,
            delta_list=delta_list,
            h_list=h_list,
            tolerance_ms=args.tolerance_ms,
            targeted_cells=targeted_for_pair,
        )
        all_metrics.extend(metrics)
        all_support.append(support)

    window = f"{args.start}..{args.end}"
    write_results(Path(args.results_out), window, all_metrics, determinism_status="PENDING")
    write_pair_support(Path(args.pair_support_out), window, all_support)

    summary = {
        "family_id": "latency_leadlag_v1",
        "window": window,
        "symbol": args.symbol,
        "stream": stream,
        "exchanges": exchange_order,
        "params": {
            "tolerance_ms": int(args.tolerance_ms),
            "delta_t_ms": delta_list,
            "h_ms": h_list,
            "pair_mode": args.pair_mode,
            "targeted_mode": bool(args.cells_file),
            "metric_kind": metric_kind,
            "value_def": value_def,
        },
        "inputs": {
            "object_keys_tsv": str(Path(args.object_keys_tsv)),
            "downloads_dir": str(downloads_dir),
            "rows_loaded_by_exchange": {ex: len(events[ex][0]) for ex in exchange_order},
            "parquet_paths": {ex: [str(p) for p in resolved[ex]] for ex in exchange_order},
            "cells_file": str(Path(args.cells_file)) if args.cells_file else "",
        },
        "pair_support": [
            {"pair": s.pair, "event_count_pair": int(s.event_count_pair)}
            for s in sorted(all_support, key=lambda x: x.pair)
        ],
        "primary_hash": metrics_hash(all_metrics),
        "compare_basis": "pair,delta_t_ms,h_ms,event_count,mean_forward_return_bps,t_stat,metric_kind,value_def",
    }

    summary_path = Path(args.summary_out)
    ensure_parent(summary_path)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    print(f"RESULTS_OUT={Path(args.results_out)}")
    print(f"PAIR_SUPPORT_OUT={Path(args.pair_support_out)}")
    print(f"SUMMARY_OUT={summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
