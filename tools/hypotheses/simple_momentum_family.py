#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
from pathlib import Path

import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SimpleMomentum family runner")
    p.add_argument("--exchange", required=True)
    p.add_argument("--stream", required=True)
    p.add_argument("--symbol", required=True, help="e.g. LTC/USDT or ltcusdt")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--lookback-minutes", type=int, default=5)
    p.add_argument("--forward-minutes", type=int, default=5)
    p.add_argument("--signal-quantile", type=float, default=0.90)
    p.add_argument("--min-support", type=int, default=200)
    p.add_argument("--output", required=True)
    return p.parse_args()


def normalize_symbol(sym: str) -> str:
    return sym.replace("/", "").replace("-", "").lower()


def date_iter(start: str, end: str):
    d0 = dt.datetime.strptime(start, "%Y%m%d").date()
    d1 = dt.datetime.strptime(end, "%Y%m%d").date()
    cur = d0
    while cur <= d1:
        yield cur.strftime("%Y%m%d")
        cur += dt.timedelta(days=1)


def quantile(values, q: float):
    if not values:
        return float("nan")
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    idx = int((len(s) - 1) * q)
    return s[idx]


def mean_std(vals):
    if not vals:
        return 0.0, 0.0
    n = len(vals)
    m = sum(vals) / n
    if n == 1:
        return m, 0.0
    var = sum((x - m) ** 2 for x in vals) / (n - 1)
    return m, math.sqrt(max(var, 0.0))


def detect_ts_scale(max_ts: int):
    # Trade compact data ts_event is typically epoch-ms, but we keep this robust.
    if max_ts >= 10**17:
        return "ns", 1_000_000_000
    if max_ts >= 10**14:
        return "us", 1_000_000
    if max_ts >= 10**11:
        return "ms", 1_000
    return "s", 1


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[2]

    symbol_slug = normalize_symbol(args.symbol)
    parquet_paths = []
    timestamps = []
    prices = []

    for day in date_iter(args.start, args.end):
        p = (
            repo
            / "data"
            / "curated"
            / f"exchange={args.exchange.lower()}"
            / f"stream={args.stream.lower()}"
            / f"symbol={symbol_slug}"
            / f"date={day}"
            / "data.parquet"
        )
        if not p.is_file():
            continue

        pf = pq.ParquetFile(p)
        t = pf.read(columns=["ts_event", "price"])
        ts = t["ts_event"].to_pylist()
        px = t["price"].to_pylist()
        for i in range(len(ts)):
            v_ts = ts[i]
            v_px = px[i]
            if v_ts is None or v_px is None:
                continue
            timestamps.append(int(v_ts))
            prices.append(float(v_px))
        parquet_paths.append(str(p.relative_to(repo)).replace("\\", "/"))

    n = len(timestamps)
    report = {
        "family_id": "family_b_simple_momentum",
        "exchange": args.exchange,
        "stream": args.stream,
        "symbol": args.symbol,
        "symbol_slug": symbol_slug,
        "date_range": f"{args.start}..{args.end}",
        "inputs": {
            "parquet_relpaths": parquet_paths,
            "rows_loaded": n,
            "timestamp_unit": "unknown",
        },
        "params": {
            "lookback_minutes": args.lookback_minutes,
            "forward_minutes": args.forward_minutes,
            "signal_quantile": args.signal_quantile,
            "min_support": args.min_support,
        },
        "result": {
            "valid_pairs": 0,
            "signal_support": 0,
            "lookback_quantile_threshold": None,
            "mean_forward_return": 0.0,
            "t_stat": 0.0,
            "pass_signal": False,
        },
        "diagnosticNotes": [],
    }

    if n < 3:
        report["diagnosticNotes"].append("insufficient_rows")
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
        return 0

    pairs = sorted(zip(timestamps, prices), key=lambda x: x[0])
    ts = [x[0] for x in pairs]
    px = [x[1] for x in pairs]

    unit, per_second = detect_ts_scale(max(ts))
    report["inputs"]["timestamp_unit"] = unit

    lb_delta = int(args.lookback_minutes * 60 * per_second)
    fw_delta = int(args.forward_minutes * 60 * per_second)

    lb_ptr = -1
    fw_ptr = 0
    lb_returns = []
    fw_returns = []

    for i in range(len(ts)):
        t_cur = ts[i]

        target_lb = t_cur - lb_delta
        while lb_ptr + 1 < i and ts[lb_ptr + 1] <= target_lb:
            lb_ptr += 1
        if lb_ptr < 0:
            continue

        if fw_ptr < i:
            fw_ptr = i
        target_fw = t_cur + fw_delta
        while fw_ptr < len(ts) and ts[fw_ptr] < target_fw:
            fw_ptr += 1
        if fw_ptr >= len(ts):
            break

        p0 = px[lb_ptr]
        p1 = px[i]
        p2 = px[fw_ptr]
        if p0 == 0.0 or p1 == 0.0:
            continue

        r_lb = (p1 / p0) - 1.0
        r_fw = (p2 / p1) - 1.0
        lb_returns.append(r_lb)
        fw_returns.append(r_fw)

    report["result"]["valid_pairs"] = len(lb_returns)
    if len(lb_returns) == 0:
        report["diagnosticNotes"].append("no_valid_pairs")
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
        return 0

    q_thr = quantile(lb_returns, args.signal_quantile)
    selected_fw = [fw_returns[i] for i in range(len(lb_returns)) if lb_returns[i] >= q_thr]
    support = len(selected_fw)
    mean_fw, std_fw = mean_std(selected_fw)
    t_stat = 0.0
    if support >= 2 and std_fw > 0.0:
        t_stat = mean_fw / (std_fw / math.sqrt(support))

    pass_signal = support >= args.min_support and mean_fw > 0.0 and abs(t_stat) > 2.0

    report["result"].update(
        {
            "signal_support": support,
            "lookback_quantile_threshold": q_thr,
            "mean_forward_return": mean_fw,
            "t_stat": t_stat,
            "pass_signal": pass_signal,
        }
    )

    if support < args.min_support:
        report["diagnosticNotes"].append("support_below_min")

    Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
