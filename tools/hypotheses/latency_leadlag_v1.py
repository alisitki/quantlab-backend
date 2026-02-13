#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Dict, List, Tuple

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
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--tolerance-ms", type=int, default=20)
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
    ts_out: List[int] = []
    seq_out: List[int] = []
    mid_out: List[float] = []

    for p in paths:
        pf = pq.ParquetFile(p)
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
                if bid_f <= 0.0 or ask_f <= 0.0:
                    continue
                ts_out.append(int(ts))
                seq_out.append(int(seq))
                mid_out.append((bid_f + ask_f) / 2.0)

    # Input order is deterministic in parquet. Stable sort by (ts, seq, idx) to guarantee tie-break order.
    idx = list(range(len(ts_out)))
    idx.sort(key=lambda i: (ts_out[i], seq_out[i], i))
    ts_sorted = [ts_out[i] for i in idx]
    seq_sorted = [seq_out[i] for i in idx]
    mid_sorted = [mid_out[i] for i in idx]
    return ts_sorted, seq_sorted, mid_sorted


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
    source_mid: List[float],
    target_ts: List[int],
    target_seq: List[int],
    target_mid: List[float],
    pair_name: str,
    delta_list: List[int],
    h_list: List[int],
    tolerance_ms: int,
) -> Tuple[List[GridMetric], PairSupport]:
    if len(source_ts) < 2 or len(target_ts) < 2:
        metrics = [
            GridMetric(pair=pair_name, delta_t_ms=dt_ms, h_ms=h_ms, event_count=0, mean_forward_return_bps=0.0, t_stat=0.0)
            for dt_ms in delta_list
            for h_ms in h_list
        ]
        return metrics, PairSupport(pair=pair_name, event_count_pair=0)

    signal_ts: List[int] = []
    signal_sign: List[int] = []
    for i in range(1, len(source_ts)):
        prev_mid = source_mid[i - 1]
        cur_mid = source_mid[i]
        if prev_mid <= 0.0 or cur_mid <= 0.0:
            continue
        r = math.log(cur_mid / prev_mid)
        if r > 0.0:
            signal_ts.append(source_ts[i])
            signal_sign.append(1)
        elif r < 0.0:
            signal_ts.append(source_ts[i])
            signal_sign.append(-1)

    if not signal_ts:
        metrics = [
            GridMetric(pair=pair_name, delta_t_ms=dt_ms, h_ms=h_ms, event_count=0, mean_forward_return_bps=0.0, t_stat=0.0)
            for dt_ms in delta_list
            for h_ms in h_list
        ]
        return metrics, PairSupport(pair=pair_name, event_count_pair=0)

    cell_stats: Dict[Tuple[int, int], OnlineStats] = {}
    for dt_ms in delta_list:
        for h_ms in h_list:
            cell_stats[(dt_ms, h_ms)] = OnlineStats()

    valid_src_any = [False] * len(signal_ts)

    for si, t0 in enumerate(signal_ts):
        sgn = signal_sign[si]
        src_valid = False
        for dt_ms in delta_list:
            target_time = t0 + dt_ms
            j = nearest_index_within_tol(target_ts, target_seq, target_time, tolerance_ms)
            if j < 0:
                continue
            mid1 = target_mid[j]
            if mid1 <= 0.0:
                continue

            t1 = target_ts[j]
            for h_ms in h_list:
                k = bisect.bisect_left(target_ts, t1 + h_ms, lo=j)
                if k >= len(target_ts):
                    continue
                mid2 = target_mid[k]
                if mid2 <= 0.0:
                    continue
                rb = 10000.0 * math.log(mid2 / mid1)
                rsigned = sgn * rb
                cell_stats[(dt_ms, h_ms)].add(rsigned)
                src_valid = True
        if src_valid:
            valid_src_any[si] = True

    out_metrics: List[GridMetric] = []
    for dt_ms in delta_list:
        for h_ms in h_list:
            n, mean, t = cell_stats[(dt_ms, h_ms)].final()
            out_metrics.append(
                GridMetric(
                    pair=pair_name,
                    delta_t_ms=dt_ms,
                    h_ms=h_ms,
                    event_count=n,
                    mean_forward_return_bps=mean,
                    t_stat=t,
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

    delta_list = parse_csv_ints(args.delta_ms_list)
    h_list = parse_csv_ints(args.h_ms_list)
    days = ymd_days(args.start, args.end)
    if len(days) != 2:
        raise SystemExit("this diagnostic runner expects exactly 2 days")

    rows = parse_tsv(Path(args.object_keys_tsv))
    mapping = parse_object_keys(rows)

    downloads_dir = Path(args.downloads_dir)
    resolved = download_inputs(mapping, exchange_order, days, downloads_dir)

    events: Dict[str, Tuple[List[int], List[int], List[float]]] = {}
    for ex in exchange_order:
        events[ex] = load_exchange_events(resolved[ex])

    pairs = [
        (exchange_order[0], exchange_order[1]),
        (exchange_order[0], exchange_order[2]),
        (exchange_order[1], exchange_order[2]),
    ]

    all_metrics: List[GridMetric] = []
    all_support: List[PairSupport] = []
    for src, dst in pairs:
        src_ts, _src_seq, src_mid = events[src]
        dst_ts, dst_seq, dst_mid = events[dst]
        pair_name = f"{src}->{dst}"
        metrics, support = compute_pair_metrics(
            source_ts=src_ts,
            source_mid=src_mid,
            target_ts=dst_ts,
            target_seq=dst_seq,
            target_mid=dst_mid,
            pair_name=pair_name,
            delta_list=delta_list,
            h_list=h_list,
            tolerance_ms=args.tolerance_ms,
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
        "stream": "bbo",
        "exchanges": exchange_order,
        "params": {
            "tolerance_ms": int(args.tolerance_ms),
            "delta_t_ms": delta_list,
            "h_ms": h_list,
        },
        "inputs": {
            "object_keys_tsv": str(Path(args.object_keys_tsv)),
            "downloads_dir": str(downloads_dir),
            "rows_loaded_by_exchange": {ex: len(events[ex][0]) for ex in exchange_order},
            "parquet_paths": {ex: [str(p) for p in resolved[ex]] for ex in exchange_order},
        },
        "pair_support": [
            {"pair": s.pair, "event_count_pair": int(s.event_count_pair)}
            for s in sorted(all_support, key=lambda x: x.pair)
        ],
        "primary_hash": metrics_hash(all_metrics),
        "compare_basis": "pair,delta_t_ms,h_ms,event_count,mean_forward_return_bps,t_stat",
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
