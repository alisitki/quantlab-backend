#!/usr/bin/env python3
"""Phase7 cross-exchange / market context analysis for linkusdt microstructure rerun.

Build ex-ante-safe context features at trade entry timestamps using the rerun
trade attribution surface plus compacted market data from linkusdt and btcusdt
across bybit/binance/okx.
"""

from __future__ import annotations

import bisect
import json
import math
import statistics
import subprocess
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
ATTRIBUTION_V2_JSON = ROOT / "tools/phase7_trade_attribution_v2.json"
ATTRIBUTION_V2_REPORT = ROOT / "tools/phase7_trade_attribution_output/phase7_trade_attribution_report_v2.md"
CONTEXT_JSONL = ROOT / "tools/phase7_microstructure_observability_output/full_linkusdt_rerun/shadow_state/shadow_execution_events_v1.jsonl"
ECON_JSON = ROOT / "tools/phase7_microstructure_observability_output/full_linkusdt_rerun/shadow_state/shadow_futures_paper_ledger_v1.json"
BATCH_RESULT_JSON = ROOT / "tools/phase7_microstructure_observability_output/full_linkusdt_rerun/shadow_observation_batch_result_v0.json"
SUMMARY_JSON = ROOT / "tools/phase7_microstructure_observability_output/full_linkusdt_rerun/batch_out/rank01_microstructure_imbalance_v1_bybit_linkusdt_trade_d100_h500_pt020/summary.json"
INVENTORY_JSON = Path("/tmp/compacted__state.json")
S3_TOOL = Path("/tmp/s3_compact_tool.py")
OUTPUT_JSON = ROOT / "tools/phase7_cross_exchange_context_v0.json"
OUTPUT_DIR = ROOT / "tools/phase7_cross_exchange_context_output"
OUTPUT_REPORT_MD = OUTPUT_DIR / "phase7_cross_exchange_context_report_v0.md"
SCHEMA_VERSION = "phase7_cross_exchange_context_v0"
BUCKET = "quantlab-compact"
WINDOWS_MS = (100, 250, 500)
TARGET_STRATEGY_ID = "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"
LINK_VENUES = ("bybit", "binance", "okx")
BTC_BBO_VENUES = ("bybit", "okx")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open() as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def ns_to_day(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).strftime("%Y%m%d")


def normalize_ts_to_ns(raw_ts: int) -> int:
    """Normalize compacted parquet timestamps to nanoseconds.

    Historical compacted parquet surfaces use millisecond ts_event values,
    while shadow runtime surfaces use nanoseconds.
    """
    ts = int(raw_ts)
    if ts < 10**14:
        return ts * 1_000_000
    if ts < 10**17:
        return ts * 1_000
    return ts


def as_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def round_or_none(value: float | None, digits: int = 12) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    w = pos - lo
    return ordered[lo] * (1.0 - w) + ordered[hi] * w


def bucket_terciles(values: list[float], value: float | None) -> str:
    clean = sorted(v for v in values if v is not None and math.isfinite(v))
    if value is None or not clean:
        return "UNKNOWN"
    q1 = pct(clean, 1.0 / 3.0)
    q2 = pct(clean, 2.0 / 3.0)
    if q1 is None or q2 is None:
        return "UNKNOWN"
    if value <= q1:
        return "low"
    if value <= q2:
        return "medium"
    return "high"


def sign_relation(value: float | None, trade_sign: int, flat_eps: float = 1e-9) -> str:
    if value is None:
        return "no_coverage"
    if abs(value) <= flat_eps:
        return "flat"
    if value * trade_sign > 0:
        return "supportive"
    return "disagreement"


def bool_flag(value: bool | None) -> str:
    if value is None:
        return "UNKNOWN"
    return "true" if value else "false"


@dataclass(frozen=True)
class DataSourceSpec:
    exchange: str
    stream: str
    symbol: str

    @property
    def inventory_key(self) -> str:
        return f"{self.exchange}/{self.stream}/{self.symbol}"

    @property
    def s3_data_key(self) -> str:
        return f"exchange={self.exchange}/stream={self.stream}/symbol={self.symbol}/date={{day}}/data.parquet"


@dataclass
class TradeDataset:
    ts: list[int]
    price: list[float]
    cum_signed_qty: list[float]
    cum_total_qty: list[float]

    def window_metrics(self, entry_ts_ns: int, window_ms: int) -> dict[str, Any]:
        window_ns = int(window_ms * 1_000_000)
        lo = bisect.bisect_left(self.ts, entry_ts_ns - window_ns)
        hi = bisect.bisect_right(self.ts, entry_ts_ns)
        if hi <= lo:
            return {"pressure": None, "return_bps": None, "event_count": 0}
        total_qty = self.cum_total_qty[hi] - self.cum_total_qty[lo]
        signed_qty = self.cum_signed_qty[hi] - self.cum_signed_qty[lo]
        pressure = signed_qty / total_qty if total_qty > 0 else None
        return_bps = 0.0
        if hi - lo >= 2 and self.price[lo] > 0:
            return_bps = (self.price[hi - 1] / self.price[lo] - 1.0) * 10_000.0
        return {
            "pressure": round_or_none(pressure),
            "return_bps": round_or_none(return_bps),
            "event_count": hi - lo,
        }


@dataclass
class BboDataset:
    ts: list[int]
    mid: list[float]
    cum_imbalance: list[float]
    cum_spread_bps: list[float]

    def window_metrics(self, entry_ts_ns: int, window_ms: int) -> dict[str, Any]:
        window_ns = int(window_ms * 1_000_000)
        lo = bisect.bisect_left(self.ts, entry_ts_ns - window_ns)
        hi = bisect.bisect_right(self.ts, entry_ts_ns)
        if hi <= lo:
            return {"imbalance": None, "return_bps": None, "count": 0, "spread_bps": None}
        count = hi - lo
        imbalance = (self.cum_imbalance[hi] - self.cum_imbalance[lo]) / count
        spread_bps = (self.cum_spread_bps[hi] - self.cum_spread_bps[lo]) / count
        return_bps = 0.0
        if hi - lo >= 2 and self.mid[lo] > 0:
            return_bps = (self.mid[hi - 1] / self.mid[lo] - 1.0) * 10_000.0
        return {
            "imbalance": round_or_none(imbalance),
            "return_bps": round_or_none(return_bps),
            "count": count,
            "spread_bps": round_or_none(spread_bps),
        }


def load_inventory() -> dict[str, dict[str, Any]]:
    obj = read_json(INVENTORY_JSON)
    partitions = obj.get("partitions") or {}
    return partitions if isinstance(partitions, dict) else {}


def build_base_rows() -> list[dict[str, Any]]:
    doc = read_json(ATTRIBUTION_V2_JSON)
    rows = doc.get("joined_trade_rows") or []
    base = []
    for row in rows:
        trade_sequence_id = int(row["trade_sequence_id"])
        entry_ts = int(row["entry_timestamp"])
        entry_side = str(row["entry_side"]).upper()
        trade_sign = 1 if entry_side == "LONG" else -1
        base.append(
            {
                "trade_sequence_id": trade_sequence_id,
                "entry_timestamp_ns": entry_ts,
                "entry_day": ns_to_day(entry_ts),
                "entry_side": entry_side,
                "trade_sign": trade_sign,
                "gross_pnl": float(row["gross_pnl"]),
                "net_pnl": float(row["net_pnl"]),
                "fee_paid": float(row["fee_paid"]),
                "entry_abs_pressure_local": as_float(row.get("entry_abs_pressure")),
                "was_reversal_trade": bool(row.get("was_reversal_trade")),
            }
        )
    base.sort(key=lambda row: row["trade_sequence_id"])
    return base


def select_subsets(base_rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(base_rows, key=lambda row: row["gross_pnl"], reverse=True)
    total_gross = sum(row["gross_pnl"] for row in base_rows)
    running = 0.0
    good: list[dict[str, Any]] = []
    for row in ordered:
        good.append(row)
        running += row["gross_pnl"]
        if total_gross > 0 and running > 0.5 * total_gross:
            break
    bad = [row for row in base_rows if row["gross_pnl"] < 0]
    return {
        "good_rule": "minimum subset by descending gross_pnl producing >50% of total gross pnl",
        "bad_rule": "all trades with gross_pnl < 0",
        "good_trade_sequence_ids": {row["trade_sequence_id"] for row in good},
        "bad_trade_sequence_ids": {row["trade_sequence_id"] for row in bad},
        "good_count": len(good),
        "bad_count": len(bad),
        "full_count": len(base_rows),
    }


def resolve_local_or_s3(
    *,
    spec: DataSourceSpec,
    day: str,
    inventory: dict[str, dict[str, Any]],
    temp_root: Path,
) -> tuple[Path | None, dict[str, Any]]:
    partition_key = f"{spec.exchange}/{spec.stream}/{spec.symbol}/{day}"
    payload = inventory.get(partition_key)
    status = str(payload.get("status", "")).strip().lower() if isinstance(payload, dict) else ""
    quality = str(payload.get("day_quality_post", "")).strip().upper() if isinstance(payload, dict) else ""
    rows = int(payload.get("rows") or 0) if isinstance(payload, dict) else 0
    curated = (
        ROOT
        / "data/curated"
        / f"exchange={spec.exchange}"
        / f"stream={spec.stream}"
        / f"symbol={spec.symbol}"
        / f"date={day}"
        / "data.parquet"
    )
    meta = {
        "partition_key": partition_key,
        "status": status,
        "day_quality_post": quality,
        "rows": rows,
        "bucket": BUCKET,
        "data_key": spec.s3_data_key.format(day=day),
        "source": None,
        "path": None,
    }
    if curated.is_file():
        meta["source"] = "curated"
        meta["path"] = str(curated)
        return curated, meta
    if status != "success" or quality not in {"GOOD", "DEGRADED"} or rows <= 0:
        meta["source"] = "skipped_inventory_status"
        return None, meta
    if not S3_TOOL.exists():
        meta["source"] = "missing_s3_tool"
        return None, meta
    out = temp_root / f"exchange={spec.exchange}" / f"stream={spec.stream}" / f"symbol={spec.symbol}" / f"date={day}" / "data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        ["python3", str(S3_TOOL), "get", BUCKET, spec.s3_data_key.format(day=day), str(out)],
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0 and out.is_file() and out.stat().st_size > 0:
        meta["source"] = "s3"
        meta["path"] = str(out)
        return out, meta
    meta["source"] = "s3_fetch_failed"
    meta["stderr"] = proc.stderr[-4000:] if proc.stderr else ""
    meta["stdout"] = proc.stdout[-4000:] if proc.stdout else ""
    return None, meta


def load_trade_dataset(path: Path) -> TradeDataset:
    ts: list[int] = []
    seq: list[int] = []
    price: list[float] = []
    signed_qty: list[float] = []
    total_qty: list[float] = []
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(columns=["ts_event", "seq", "price", "qty", "side"], batch_size=131072):
        cols = batch.to_pydict()
        for i in range(len(cols["ts_event"])):
            ts_raw = cols["ts_event"][i]
            seq_raw = cols["seq"][i]
            px_raw = cols["price"][i]
            qty_raw = cols["qty"][i]
            side_raw = cols["side"][i]
            if ts_raw is None or seq_raw is None or px_raw is None or qty_raw is None or side_raw is None:
                continue
            px = float(px_raw)
            qty = float(qty_raw)
            side = float(side_raw)
            if px <= 0 or qty <= 0 or side == 0:
                continue
            ts.append(normalize_ts_to_ns(int(ts_raw)))
            seq.append(int(seq_raw))
            price.append(px)
            signed_qty.append(qty if side > 0 else -qty)
            total_qty.append(qty)
    order = sorted(range(len(ts)), key=lambda idx: (ts[idx], seq[idx], idx))
    ts = [ts[idx] for idx in order]
    price = [price[idx] for idx in order]
    signed_qty = [signed_qty[idx] for idx in order]
    total_qty = [total_qty[idx] for idx in order]
    cum_signed = [0.0]
    cum_total = [0.0]
    for value in signed_qty:
        cum_signed.append(cum_signed[-1] + value)
    for value in total_qty:
        cum_total.append(cum_total[-1] + value)
    return TradeDataset(ts=ts, price=price, cum_signed_qty=cum_signed, cum_total_qty=cum_total)


def load_bbo_dataset(path: Path) -> BboDataset:
    ts: list[int] = []
    seq: list[int] = []
    mid: list[float] = []
    imbalance: list[float] = []
    spread_bps: list[float] = []
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(columns=["ts_event", "seq", "bid_price", "bid_qty", "ask_price", "ask_qty"], batch_size=131072):
        cols = batch.to_pydict()
        for i in range(len(cols["ts_event"])):
            ts_raw = cols["ts_event"][i]
            seq_raw = cols["seq"][i]
            bid_p_raw = cols["bid_price"][i]
            bid_q_raw = cols["bid_qty"][i]
            ask_p_raw = cols["ask_price"][i]
            ask_q_raw = cols["ask_qty"][i]
            if None in (ts_raw, seq_raw, bid_p_raw, bid_q_raw, ask_p_raw, ask_q_raw):
                continue
            bid_p = float(bid_p_raw)
            bid_q = float(bid_q_raw)
            ask_p = float(ask_p_raw)
            ask_q = float(ask_q_raw)
            if bid_p <= 0 or ask_p <= 0 or bid_q < 0 or ask_q < 0 or ask_p < bid_p:
                continue
            denom = bid_q + ask_q
            if denom <= 0:
                continue
            mid_px = (bid_p + ask_p) / 2.0
            imb = (bid_q - ask_q) / denom
            spread = (ask_p - bid_p) / mid_px * 10_000.0 if mid_px > 0 else 0.0
            ts.append(normalize_ts_to_ns(int(ts_raw)))
            seq.append(int(seq_raw))
            mid.append(mid_px)
            imbalance.append(imb)
            spread_bps.append(spread)
    order = sorted(range(len(ts)), key=lambda idx: (ts[idx], seq[idx], idx))
    ts = [ts[idx] for idx in order]
    mid = [mid[idx] for idx in order]
    imbalance = [imbalance[idx] for idx in order]
    spread_bps = [spread_bps[idx] for idx in order]
    cum_imb = [0.0]
    cum_spread = [0.0]
    for value in imbalance:
        cum_imb.append(cum_imb[-1] + value)
    for value in spread_bps:
        cum_spread.append(cum_spread[-1] + value)
    return BboDataset(ts=ts, mid=mid, cum_imbalance=cum_imb, cum_spread_bps=cum_spread)


def load_market_datasets(days: list[str]) -> tuple[dict[tuple[str, str, str], Any], dict[str, Any]]:
    inventory = load_inventory()
    specs = [
        DataSourceSpec(exchange=exchange, stream="trade", symbol="linkusdt")
        for exchange in LINK_VENUES
    ] + [
        DataSourceSpec(exchange=exchange, stream="bbo", symbol="linkusdt")
        for exchange in LINK_VENUES
    ] + [
        DataSourceSpec(exchange=exchange, stream="bbo", symbol="btcusdt")
        for exchange in BTC_BBO_VENUES
    ] + [
        DataSourceSpec(exchange=exchange, stream="trade", symbol="btcusdt")
        for exchange in LINK_VENUES
    ]

    datasets: dict[tuple[str, str, str], Any] = {}
    source_meta: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="phase7_cross_exchange_context_") as tmp:
        temp_root = Path(tmp)
        for spec in specs:
            per_day_paths: list[Path] = []
            per_day_meta: list[dict[str, Any]] = []
            if spec.symbol == "btcusdt" and spec.stream == "trade":
                for day in days:
                    partition_key = f"{spec.exchange}/{spec.stream}/{spec.symbol}/{day}"
                    payload = inventory.get(partition_key)
                    per_day_meta.append(
                        {
                            "partition_key": partition_key,
                            "status": str(payload.get("status", "")).strip().lower() if isinstance(payload, dict) else "",
                            "day_quality_post": str(payload.get("day_quality_post", "")).strip().upper() if isinstance(payload, dict) else "",
                            "rows": int(payload.get("rows") or 0) if isinstance(payload, dict) else 0,
                            "bucket": BUCKET,
                            "data_key": spec.s3_data_key.format(day=day),
                            "source": "skipped_btc_trade_unavailable_or_non_authoritative",
                            "path": None,
                        }
                    )
                source_meta[f"{spec.exchange}:{spec.stream}:{spec.symbol}"] = per_day_meta
                continue
            for day in days:
                path, meta = resolve_local_or_s3(spec=spec, day=day, inventory=inventory, temp_root=temp_root)
                per_day_meta.append(meta)
                if path is not None:
                    per_day_paths.append(path)
            source_meta[f"{spec.exchange}:{spec.stream}:{spec.symbol}"] = per_day_meta
            if not per_day_paths:
                continue
            # Current run spans one day, but loading multiple days by concatenation stays deterministic.
            if spec.stream == "trade":
                merged_ts: list[int] = []
                merged_price: list[float] = []
                merged_signed: list[float] = []
                merged_total: list[float] = []
                for path in per_day_paths:
                    ds = load_trade_dataset(path)
                    # unwrap cumulative arrays back to per-event values for concatenation
                    signed = [ds.cum_signed_qty[i + 1] - ds.cum_signed_qty[i] for i in range(len(ds.ts))]
                    total = [ds.cum_total_qty[i + 1] - ds.cum_total_qty[i] for i in range(len(ds.ts))]
                    merged_ts.extend(ds.ts)
                    merged_price.extend(ds.price)
                    merged_signed.extend(signed)
                    merged_total.extend(total)
                order = sorted(range(len(merged_ts)), key=lambda idx: (merged_ts[idx], idx))
                ts = [merged_ts[idx] for idx in order]
                price = [merged_price[idx] for idx in order]
                signed = [merged_signed[idx] for idx in order]
                total = [merged_total[idx] for idx in order]
                cum_signed = [0.0]
                cum_total = [0.0]
                for value in signed:
                    cum_signed.append(cum_signed[-1] + value)
                for value in total:
                    cum_total.append(cum_total[-1] + value)
                datasets[(spec.exchange, spec.stream, spec.symbol)] = TradeDataset(ts=ts, price=price, cum_signed_qty=cum_signed, cum_total_qty=cum_total)
            else:
                merged_ts: list[int] = []
                merged_mid: list[float] = []
                merged_imb: list[float] = []
                merged_spread: list[float] = []
                for path in per_day_paths:
                    ds = load_bbo_dataset(path)
                    imb = [ds.cum_imbalance[i + 1] - ds.cum_imbalance[i] for i in range(len(ds.ts))]
                    spread = [ds.cum_spread_bps[i + 1] - ds.cum_spread_bps[i] for i in range(len(ds.ts))]
                    merged_ts.extend(ds.ts)
                    merged_mid.extend(ds.mid)
                    merged_imb.extend(imb)
                    merged_spread.extend(spread)
                order = sorted(range(len(merged_ts)), key=lambda idx: (merged_ts[idx], idx))
                ts = [merged_ts[idx] for idx in order]
                mid = [merged_mid[idx] for idx in order]
                imb = [merged_imb[idx] for idx in order]
                spread = [merged_spread[idx] for idx in order]
                cum_imb = [0.0]
                cum_spread = [0.0]
                for value in imb:
                    cum_imb.append(cum_imb[-1] + value)
                for value in spread:
                    cum_spread.append(cum_spread[-1] + value)
                datasets[(spec.exchange, spec.stream, spec.symbol)] = BboDataset(ts=ts, mid=mid, cum_imbalance=cum_imb, cum_spread_bps=cum_spread)
        # datasets remain in memory after TemporaryDirectory cleanup.
    return datasets, source_meta


def build_feature_rows(base_rows: list[dict[str, Any]], datasets: dict[tuple[str, str, str], Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for base in base_rows:
        row = dict(base)
        entry_ts = base["entry_timestamp_ns"]
        trade_sign = base["trade_sign"]
        for window_ms in WINDOWS_MS:
            link_trade_pressures: list[float] = []
            link_bbo_imbalances: list[float] = []
            btc_returns: list[float] = []
            btc_imbalances: list[float] = []
            btc_counts: list[int] = []
            link_trade_available = 0
            link_bbo_available = 0
            btc_available = 0

            for venue in LINK_VENUES:
                trade_ds = datasets.get((venue, "trade", "linkusdt"))
                trade_metrics = trade_ds.window_metrics(entry_ts, window_ms) if trade_ds else {"pressure": None, "return_bps": None, "event_count": 0}
                row[f"{venue}_link_trade_pressure_{window_ms}ms"] = trade_metrics["pressure"]
                row[f"{venue}_link_trade_return_bps_{window_ms}ms"] = trade_metrics["return_bps"]
                row[f"{venue}_link_trade_event_count_{window_ms}ms"] = trade_metrics["event_count"]
                if trade_metrics["pressure"] is not None:
                    link_trade_pressures.append(float(trade_metrics["pressure"]))
                    link_trade_available += 1

                bbo_ds = datasets.get((venue, "bbo", "linkusdt"))
                bbo_metrics = bbo_ds.window_metrics(entry_ts, window_ms) if bbo_ds else {"imbalance": None, "return_bps": None, "count": 0, "spread_bps": None}
                row[f"{venue}_link_bbo_imbalance_{window_ms}ms"] = bbo_metrics["imbalance"]
                row[f"{venue}_link_bbo_return_bps_{window_ms}ms"] = bbo_metrics["return_bps"]
                row[f"{venue}_link_bbo_count_{window_ms}ms"] = bbo_metrics["count"]
                row[f"{venue}_link_bbo_spread_bps_{window_ms}ms"] = bbo_metrics["spread_bps"]
                if bbo_metrics["imbalance"] is not None:
                    link_bbo_imbalances.append(float(bbo_metrics["imbalance"]))
                    link_bbo_available += 1

            for venue in BTC_BBO_VENUES:
                btc_ds = datasets.get((venue, "bbo", "btcusdt"))
                btc_metrics = btc_ds.window_metrics(entry_ts, window_ms) if btc_ds else {"imbalance": None, "return_bps": None, "count": 0, "spread_bps": None}
                row[f"{venue}_btc_bbo_imbalance_{window_ms}ms"] = btc_metrics["imbalance"]
                row[f"{venue}_btc_bbo_return_bps_{window_ms}ms"] = btc_metrics["return_bps"]
                row[f"{venue}_btc_bbo_count_{window_ms}ms"] = btc_metrics["count"]
                if btc_metrics["imbalance"] is not None or btc_metrics["return_bps"] is not None:
                    btc_available += 1
                if btc_metrics["return_bps"] is not None:
                    btc_returns.append(float(btc_metrics["return_bps"]))
                if btc_metrics["imbalance"] is not None:
                    btc_imbalances.append(float(btc_metrics["imbalance"]))
                if btc_metrics["count"]:
                    btc_counts.append(int(btc_metrics["count"]))

            row[f"link_trade_alignment_count_{window_ms}ms"] = sum(1 for value in link_trade_pressures if value * trade_sign > 0)
            row[f"link_trade_available_count_{window_ms}ms"] = link_trade_available
            row[f"link_trade_venue_agreement_flag_{window_ms}ms"] = link_trade_available >= 2 and row[f"link_trade_alignment_count_{window_ms}ms"] == link_trade_available
            row[f"link_trade_pressure_spread_{window_ms}ms"] = round_or_none(max(link_trade_pressures) - min(link_trade_pressures)) if len(link_trade_pressures) >= 2 else None

            row[f"link_bbo_alignment_count_{window_ms}ms"] = sum(1 for value in link_bbo_imbalances if value * trade_sign > 0)
            row[f"link_bbo_available_count_{window_ms}ms"] = link_bbo_available
            row[f"link_bbo_venue_agreement_flag_{window_ms}ms"] = link_bbo_available >= 2 and row[f"link_bbo_alignment_count_{window_ms}ms"] == link_bbo_available
            row[f"link_bbo_imbalance_spread_{window_ms}ms"] = round_or_none(max(link_bbo_imbalances) - min(link_bbo_imbalances)) if len(link_bbo_imbalances) >= 2 else None

            avg_btc_return = safe_div(sum(btc_returns), float(len(btc_returns))) if btc_returns else None
            avg_btc_imbalance = safe_div(sum(btc_imbalances), float(len(btc_imbalances))) if btc_imbalances else None
            row[f"btc_bbo_available_count_{window_ms}ms"] = btc_available
            row[f"btc_bbo_return_avg_bps_{window_ms}ms"] = round_or_none(avg_btc_return)
            row[f"btc_bbo_imbalance_avg_{window_ms}ms"] = round_or_none(avg_btc_imbalance)
            row[f"btc_bbo_activity_count_{window_ms}ms"] = sum(btc_counts)
            row[f"btc_bbo_return_relation_{window_ms}ms"] = sign_relation(avg_btc_return, trade_sign, flat_eps=0.05)
            row[f"btc_bbo_pressure_relation_{window_ms}ms"] = sign_relation(avg_btc_imbalance, trade_sign, flat_eps=0.02)

        rows.append(row)

    for window_ms in WINDOWS_MS:
        trade_spreads = [row[f"link_trade_pressure_spread_{window_ms}ms"] for row in rows]
        bbo_spreads = [row[f"link_bbo_imbalance_spread_{window_ms}ms"] for row in rows]
        btc_activity = [float(row[f"btc_bbo_activity_count_{window_ms}ms"]) for row in rows if row[f"btc_bbo_available_count_{window_ms}ms"] > 0]
        for row in rows:
            row[f"link_trade_divergence_bucket_{window_ms}ms"] = bucket_terciles(trade_spreads, row[f"link_trade_pressure_spread_{window_ms}ms"])
            row[f"link_bbo_divergence_bucket_{window_ms}ms"] = bucket_terciles(bbo_spreads, row[f"link_bbo_imbalance_spread_{window_ms}ms"])
            count = row[f"btc_bbo_activity_count_{window_ms}ms"]
            med = statistics.median(btc_activity) if btc_activity else None
            if row[f"btc_bbo_available_count_{window_ms}ms"] == 0 or med is None:
                row[f"btc_bbo_activity_bucket_{window_ms}ms"] = "no_coverage"
            elif count <= med:
                row[f"btc_bbo_activity_bucket_{window_ms}ms"] = "low"
            else:
                row[f"btc_bbo_activity_bucket_{window_ms}ms"] = "high"
    return rows


def bucket_compare(rows: list[dict[str, Any]], subset_good: set[int], subset_bad: set[int], feature_key: str) -> dict[str, Any]:
    groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(feature_key, "UNKNOWN"))].append(row)
    out: dict[str, Any] = {}
    good_total = len(subset_good)
    bad_total = len(subset_bad)
    for bucket in sorted(groups):
        group = groups[bucket]
        gross = sum(row["gross_pnl"] for row in group)
        net = sum(row["net_pnl"] for row in group)
        good_count = sum(1 for row in group if row["trade_sequence_id"] in subset_good)
        bad_count = sum(1 for row in group if row["trade_sequence_id"] in subset_bad)
        share_good = good_count / good_total if good_total else None
        share_bad = bad_count / bad_total if bad_total else None
        lift = (share_good / share_bad) if share_good is not None and share_bad not in (None, 0) else None
        out[bucket] = {
            "count": len(group),
            "avg_gross_pnl": round_or_none(gross / len(group)),
            "avg_net_pnl": round_or_none(net / len(group)),
            "total_gross_pnl": round_or_none(gross),
            "total_net_pnl": round_or_none(net),
            "good_trade_count": good_count,
            "bad_trade_count": bad_count,
            "share_of_good_subset": round_or_none(share_good),
            "share_of_bad_subset": round_or_none(share_bad),
            "good_vs_bad_share_lift": round_or_none(lift),
        }
    return out


def rule_metrics(rows: list[dict[str, Any]], subset_good: set[int], subset_bad: set[int], name: str, predicate: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
    matched = [row for row in rows if predicate(row)]
    if not matched:
        return {
            "rule": name,
            "trade_count": 0,
            "trade_ratio": 0.0,
            "gross_pnl_total": 0.0,
            "net_pnl_total": 0.0,
            "avg_gross_pnl_per_trade": None,
            "avg_net_pnl_per_trade": None,
            "good_trade_count": 0,
            "bad_trade_count": 0,
            "good_share": 0.0,
            "bad_share": 0.0,
            "good_vs_bad_share_lift": None,
        }
    gross = sum(row["gross_pnl"] for row in matched)
    net = sum(row["net_pnl"] for row in matched)
    good_count = sum(1 for row in matched if row["trade_sequence_id"] in subset_good)
    bad_count = sum(1 for row in matched if row["trade_sequence_id"] in subset_bad)
    good_share = good_count / len(subset_good) if subset_good else None
    bad_share = bad_count / len(subset_bad) if subset_bad else None
    lift = (good_share / bad_share) if good_share is not None and bad_share not in (None, 0) else None
    return {
        "rule": name,
        "trade_count": len(matched),
        "trade_ratio": round_or_none(len(matched) / len(rows)),
        "gross_pnl_total": round_or_none(gross),
        "net_pnl_total": round_or_none(net),
        "avg_gross_pnl_per_trade": round_or_none(gross / len(matched)),
        "avg_net_pnl_per_trade": round_or_none(net / len(matched)),
        "good_trade_count": good_count,
        "bad_trade_count": bad_count,
        "good_share": round_or_none(good_share),
        "bad_share": round_or_none(bad_share),
        "good_vs_bad_share_lift": round_or_none(lift),
    }


def build_rule_candidates(rows: list[dict[str, Any]], subset_good: set[int], subset_bad: set[int]) -> list[dict[str, Any]]:
    rules: list[tuple[str, Callable[[dict[str, Any]], bool]]] = []
    for window_ms in WINDOWS_MS:
        rules.extend(
            [
                (
                    f"link_trade_alignment_count_{window_ms}ms >= 2",
                    lambda row, w=window_ms: row[f"link_trade_alignment_count_{w}ms"] >= 2,
                ),
                (
                    f"link_trade_alignment_count_{window_ms}ms == 3",
                    lambda row, w=window_ms: row[f"link_trade_alignment_count_{w}ms"] == 3,
                ),
                (
                    f"link_trade_alignment_count_{window_ms}ms == 3 AND link_trade_divergence_bucket_{window_ms}ms == low",
                    lambda row, w=window_ms: row[f"link_trade_alignment_count_{w}ms"] == 3 and row[f"link_trade_divergence_bucket_{w}ms"] == "low",
                ),
                (
                    f"link_bbo_alignment_count_{window_ms}ms >= 2",
                    lambda row, w=window_ms: row[f"link_bbo_alignment_count_{w}ms"] >= 2,
                ),
                (
                    f"btc_bbo_return_relation_{window_ms}ms == supportive",
                    lambda row, w=window_ms: row[f"btc_bbo_return_relation_{w}ms"] == "supportive",
                ),
                (
                    f"btc_bbo_return_relation_{window_ms}ms == disagreement",
                    lambda row, w=window_ms: row[f"btc_bbo_return_relation_{w}ms"] == "disagreement",
                ),
                (
                    f"btc_bbo_pressure_relation_{window_ms}ms == supportive",
                    lambda row, w=window_ms: row[f"btc_bbo_pressure_relation_{w}ms"] == "supportive",
                ),
                (
                    f"btc_bbo_pressure_relation_{window_ms}ms == disagreement",
                    lambda row, w=window_ms: row[f"btc_bbo_pressure_relation_{w}ms"] == "disagreement",
                ),
                (
                    f"link_trade_alignment_count_{window_ms}ms == 3 AND btc_bbo_return_relation_{window_ms}ms == supportive",
                    lambda row, w=window_ms: row[f"link_trade_alignment_count_{w}ms"] == 3 and row[f"btc_bbo_return_relation_{w}ms"] == "supportive",
                ),
                (
                    f"link_trade_alignment_count_{window_ms}ms == 3 AND btc_bbo_pressure_relation_{window_ms}ms == supportive",
                    lambda row, w=window_ms: row[f"link_trade_alignment_count_{w}ms"] == 3 and row[f"btc_bbo_pressure_relation_{w}ms"] == "supportive",
                ),
                (
                    f"link_trade_alignment_count_{window_ms}ms == 3 AND link_trade_divergence_bucket_{window_ms}ms == low AND btc_bbo_return_relation_{window_ms}ms == supportive",
                    lambda row, w=window_ms: row[f"link_trade_alignment_count_{w}ms"] == 3 and row[f"link_trade_divergence_bucket_{w}ms"] == "low" and row[f"btc_bbo_return_relation_{w}ms"] == "supportive",
                ),
            ]
        )
    evaluated = [rule_metrics(rows, subset_good, subset_bad, name, predicate) for name, predicate in rules]
    filtered = [row for row in evaluated if row["trade_count"] > 0]
    filtered.sort(
        key=lambda row: (
            row["avg_net_pnl_per_trade"] if row["avg_net_pnl_per_trade"] is not None else float("-inf"),
            row["avg_gross_pnl_per_trade"] if row["avg_gross_pnl_per_trade"] is not None else float("-inf"),
            row["good_vs_bad_share_lift"] if row["good_vs_bad_share_lift"] is not None else float("-inf"),
            row["trade_count"],
        ),
        reverse=True,
    )
    return filtered


def choose_final_decision(rows: list[dict[str, Any]], rule_candidates: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    baseline_avg_gross = sum(row["gross_pnl"] for row in rows) / len(rows)
    baseline_avg_net = sum(row["net_pnl"] for row in rows) / len(rows)
    best = rule_candidates[0] if rule_candidates else None
    if best is None:
        return "NO_CONTEXT_EDGE", {"final_next_step": "KILL", "reason": "no usable context rule candidates were found", "best_rule": None}

    gross_improves = (best["avg_gross_pnl_per_trade"] or float("-inf")) > baseline_avg_gross * 1.25
    net_improves = (best["avg_net_pnl_per_trade"] or float("-inf")) > baseline_avg_net * 1.10
    lift_good = (best["good_vs_bad_share_lift"] or 0.0) > 1.25
    coverage_ok = (best["trade_ratio"] or 0.0) >= 0.10

    if gross_improves and net_improves and lift_good and coverage_ok and (best["avg_net_pnl_per_trade"] or -1.0) >= -0.002:
        return "CLEAR_CONTEXT_CONFIRMATION", {
            "final_next_step": "ESCALATE_TO_V2",
            "reason": "simple external-context rule materially improves gross/net per trade with usable coverage",
            "best_rule": best,
        }
    if gross_improves and lift_good:
        return "NARROW_CONTEXT_SIGNAL", {
            "final_next_step": "RESHAPE_REQUIRED",
            "reason": "external context improves trade quality, but coverage and/or net profile remain too weak for a clean confirmation layer",
            "best_rule": best,
        }
    return "NO_CONTEXT_EDGE", {
        "final_next_step": "KILL",
        "reason": "external context does not materially improve separability over the local signal baseline",
        "best_rule": best,
    }


def build_report(doc: dict[str, Any]) -> str:
    lines = [
        "# Phase7 Cross-Exchange Context v0",
        "",
        "This analysis compares GOOD vs BAD `linkusdt` microstructure trades using only ex-ante context from same-symbol cross-exchange and BTC market surfaces.",
        "",
        f"- GOOD subset: {doc['subset_definitions']['good_rule']} (`{doc['subset_definitions']['good_count']}` trades)",
        f"- BAD subset: {doc['subset_definitions']['bad_rule']} (`{doc['subset_definitions']['bad_count']}` trades)",
        f"- Join / coverage quality: full attribution join already at `100%`; context build covered `{doc['coverage']['rows_with_any_same_symbol_context']}` trades with same-symbol context and `{doc['coverage']['rows_with_any_btc_context']}` trades with BTC context",
        "",
        "## Strongest Patterns",
        f"- Same-symbol best rule: `{doc['best_patterns']['same_symbol_best_rule']['rule']}`",
        f"- BTC / market best rule: `{doc['best_patterns']['btc_best_rule']['rule']}`",
        f"- Combined best rule: `{doc['best_patterns']['combined_best_rule']['rule']}`",
        "",
        "## Weakest / Useless Pattern",
        f"- Weakest candidate observed: `{doc['best_patterns']['weakest_rule']['rule']}`",
        "",
        f"Final context verdict: `{doc['final_decision']['context_decision']}`",
        f"Next step: `{doc['final_decision']['final_next_step']}`",
        f"Reason: {doc['final_decision']['reason']}",
        "",
    ]
    best = doc["final_decision"]["best_rule"]
    if best:
        lines.append(
            f"Best simple rule candidate: `{best['rule']}` "
            f"(trade_count={best['trade_count']}, avg_gross={best['avg_gross_pnl_per_trade']}, "
            f"avg_net={best['avg_net_pnl_per_trade']}, good/bad lift={best['good_vs_bad_share_lift']})"
        )
    lines.extend([
        "",
        f"FINAL DECISION: `{doc['final_decision']['final_next_step']}`",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    attribution_doc = read_json(ATTRIBUTION_V2_JSON)
    base_rows = build_base_rows()
    subset_def = select_subsets(base_rows)
    unique_days = sorted({row["entry_day"] for row in base_rows})
    datasets, source_meta = load_market_datasets(unique_days)
    feature_rows = build_feature_rows(base_rows, datasets)
    good_ids = subset_def["good_trade_sequence_ids"]
    bad_ids = subset_def["bad_trade_sequence_ids"]

    coverage = {
        "total_trades": len(feature_rows),
        "good_trades_analyzed": len(good_ids),
        "bad_trades_analyzed": len(bad_ids),
        "rows_with_any_same_symbol_context": sum(
            1
            for row in feature_rows
            if any(row[f"link_trade_available_count_{w}ms"] > 0 for w in WINDOWS_MS)
        ),
        "rows_with_all_three_trade_venues_500ms": sum(
            1 for row in feature_rows if row["link_trade_available_count_500ms"] == 3
        ),
        "rows_with_all_three_bbo_venues_500ms": sum(
            1 for row in feature_rows if row["link_bbo_available_count_500ms"] == 3
        ),
        "rows_with_any_btc_context": sum(
            1 for row in feature_rows if row["btc_bbo_available_count_500ms"] > 0
        ),
        "rows_with_full_btc_venue_coverage_500ms": sum(
            1 for row in feature_rows if row["btc_bbo_available_count_500ms"] == len(BTC_BBO_VENUES)
        ),
        "same_symbol_trade_venue_sources": {
            venue: source_meta.get(f"{venue}:trade:linkusdt", []) for venue in LINK_VENUES
        },
        "same_symbol_bbo_venue_sources": {
            venue: source_meta.get(f"{venue}:bbo:linkusdt", []) for venue in LINK_VENUES
        },
        "btc_bbo_venue_sources": {
            venue: source_meta.get(f"{venue}:bbo:btcusdt", []) for venue in BTC_BBO_VENUES
        },
        "btc_trade_sources": {
            venue: source_meta.get(f"{venue}:trade:btcusdt", []) for venue in LINK_VENUES
        },
    }

    feature_keys = []
    for window_ms in WINDOWS_MS:
        feature_keys.extend(
            [
                f"link_trade_alignment_count_{window_ms}ms",
                f"link_trade_divergence_bucket_{window_ms}ms",
                f"link_trade_venue_agreement_flag_{window_ms}ms",
                f"link_bbo_alignment_count_{window_ms}ms",
                f"link_bbo_divergence_bucket_{window_ms}ms",
                f"link_bbo_venue_agreement_flag_{window_ms}ms",
                f"btc_bbo_return_relation_{window_ms}ms",
                f"btc_bbo_pressure_relation_{window_ms}ms",
                f"btc_bbo_activity_bucket_{window_ms}ms",
            ]
        )
    feature_comparison = {
        key: bucket_compare(feature_rows, good_ids, bad_ids, key) for key in feature_keys
    }
    rule_candidates = build_rule_candidates(feature_rows, good_ids, bad_ids)
    context_decision, decision_meta = choose_final_decision(feature_rows, rule_candidates)

    same_symbol_rules = [row for row in rule_candidates if row["rule"].startswith("link_")]
    btc_rules = [row for row in rule_candidates if row["rule"].startswith("btc_")]
    combined_rules = [row for row in rule_candidates if "AND btc_" in row["rule"]]
    weakest_rule = (
        min(
            rule_candidates,
            key=lambda row: (
                row["avg_net_pnl_per_trade"] if row["avg_net_pnl_per_trade"] is not None else float("inf"),
                row["avg_gross_pnl_per_trade"] if row["avg_gross_pnl_per_trade"] is not None else float("inf"),
            ),
        )
        if rule_candidates
        else None
    )

    report_doc = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now(),
        "authoritative_sources": {
            "trade_attribution_v2_json": str(ATTRIBUTION_V2_JSON),
            "trade_attribution_v2_report": str(ATTRIBUTION_V2_REPORT),
            "context_jsonl": str(CONTEXT_JSONL),
            "economic_json": str(ECON_JSON),
            "batch_result_json": str(BATCH_RESULT_JSON),
            "summary_json": str(SUMMARY_JSON),
            "inventory_json": str(INVENTORY_JSON),
        },
        "subset_definitions": {
            "good_rule": subset_def["good_rule"],
            "bad_rule": subset_def["bad_rule"],
            "good_count": subset_def["good_count"],
            "bad_count": subset_def["bad_count"],
            "full_count": subset_def["full_count"],
        },
        "coverage": coverage,
        "feature_comparison": feature_comparison,
        "candidate_rules": rule_candidates[:24],
        "best_patterns": {
            "same_symbol_best_rule": same_symbol_rules[0] if same_symbol_rules else None,
            "btc_best_rule": btc_rules[0] if btc_rules else None,
            "combined_best_rule": combined_rules[0] if combined_rules else None,
            "weakest_rule": weakest_rule,
        },
        "final_decision": {
            "context_decision": context_decision,
            **decision_meta,
        },
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(report_doc, indent=2))
    OUTPUT_REPORT_MD.write_text(build_report(report_doc))

    print("PHASE7_CROSS_EXCHANGE_CONTEXT_COMPLETE")
    print(f"OUTPUT_JSON={OUTPUT_JSON}")
    print(f"REPORT_MD={OUTPUT_REPORT_MD}")
    print(f"CONTEXT_DECISION={context_decision}")
    print(f"FINAL_NEXT_STEP={decision_meta['final_next_step']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
