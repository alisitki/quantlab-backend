#!/usr/bin/env python3
"""Phase7 trade attribution v2 for linkusdt microstructure rerun.

Join microstructure trade context from execution events with trade-level
economics from the rerun paper ledger, then produce a compact salvage verdict.
"""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RUN_ROOT = ROOT / "tools/phase7_microstructure_observability_output/full_linkusdt_rerun"
CONTEXT_JSONL = RUN_ROOT / "shadow_state/shadow_execution_events_v1.jsonl"
ECON_JSON = RUN_ROOT / "shadow_state/shadow_futures_paper_ledger_v1.json"
BATCH_RESULT_JSON = RUN_ROOT / "shadow_observation_batch_result_v0.json"
SUMMARY_JSON = (
    RUN_ROOT
    / "batch_out/rank01_microstructure_imbalance_v1_bybit_linkusdt_trade_d100_h500_pt020/summary.json"
)
OUTPUT_JSON = ROOT / "tools/phase7_trade_attribution_v2.json"
OUTPUT_DIR = ROOT / "tools/phase7_trade_attribution_output"
OUTPUT_REPORT_MD = OUTPUT_DIR / "phase7_trade_attribution_report_v2.md"
SCHEMA_VERSION = "phase7_trade_attribution_v2"
TARGET_STRATEGY_ID = "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"


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


def as_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def round_or_none(value: float | None, digits: int = 12) -> float | None:
    if value is None:
        return None
    return round(value, digits)


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


def dist(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "p25": None, "median": None, "p75": None, "max": None}
    return {
        "min": round_or_none(min(values)),
        "p25": round_or_none(pct(values, 0.25)),
        "median": round_or_none(float(median(values))),
        "p75": round_or_none(pct(values, 0.75)),
        "max": round_or_none(max(values)),
    }


def duration_bucket(duration_ms: float | None) -> str:
    if duration_ms is None:
        return "UNKNOWN"
    if duration_ms < 1_000:
        return "<1s"
    if duration_ms < 5_000:
        return "1-5s"
    if duration_ms < 20_000:
        return "5-20s"
    return ">20s"


def pressure_bucket(entry_abs_pressure: float | None) -> str:
    if entry_abs_pressure is None:
        return "UNKNOWN"
    if entry_abs_pressure < 0.25:
        return "0.2-0.25"
    if entry_abs_pressure < 0.30:
        return "0.25-0.3"
    return "0.3+"


def decay_bucket(decay: float | None) -> str:
    if decay is None:
        return "UNKNOWN"
    if decay >= 0.5:
        return "high_decay"
    return "low_decay"


def context_primary_key(ctx: dict[str, Any]) -> int | None:
    seq = ctx.get("trade_sequence_id")
    try:
        return int(seq)
    except (TypeError, ValueError):
        return None


def load_context_map() -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    rows = read_jsonl(CONTEXT_JSONL)
    dedup: dict[int, dict[str, Any]] = {}
    duplicate_counter: Counter[int] = Counter()
    null_key_rows = 0
    for row in rows:
        if row.get("selected_pack_id") != TARGET_STRATEGY_ID:
            continue
        trade_context = row.get("trade_context")
        if not isinstance(trade_context, dict):
            continue
        closing = trade_context.get("closing_trade")
        if not isinstance(closing, dict):
            continue
        seq = context_primary_key(closing)
        if seq is None:
            null_key_rows += 1
            continue
        duplicate_counter[seq] += 1
        dedup.setdefault(seq, closing)
    meta = {
        "closing_trade_rows": sum(duplicate_counter.values()),
        "unique_trade_sequence_id_count": len(dedup),
        "duplicate_histogram": {str(k): v for k, v in sorted(Counter(duplicate_counter.values()).items())},
        "null_trade_sequence_id_rows": null_key_rows,
    }
    return dedup, meta


def load_economic_rows() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    doc = read_json(ECON_JSON)
    items = doc.get("items") or []
    if len(items) != 1:
        raise SystemExit(f"expected one paper-ledger item in {ECON_JSON}, found {len(items)}")
    item = items[0]
    episodes = [ep for ep in item.get("episodes") or [] if ep.get("status") == "CLOSED"]
    meta = {
        "paper_run_status": item.get("paper_run_status"),
        "fill_event_count": item.get("fill_event_count"),
        "decision_event_count": item.get("decision_event_count"),
        "replayed_realized_pnl_quote_gross": item.get("replayed_realized_pnl_quote_gross"),
        "replayed_realized_pnl_quote_net": item.get("replayed_realized_pnl_quote_net"),
        "total_fee_quote": item.get("total_fee_quote"),
        "episode_count": len(item.get("episodes") or []),
        "closed_episode_count": len(episodes),
    }
    return episodes, meta


def fallback_match(
    episode: dict[str, Any],
    unmatched_context: dict[int, dict[str, Any]],
) -> tuple[int | None, dict[str, Any] | None]:
    entry_ts = str(episode.get("entry_timestamp") or episode.get("opened_ts_event") or "")
    exit_ts = str(episode.get("exit_timestamp") or episode.get("closed_ts_event") or "")
    best_key = None
    best_ctx = None
    for seq, ctx in unmatched_context.items():
        if str(ctx.get("entry_timestamp") or "") == entry_ts and str(ctx.get("exit_timestamp") or "") == exit_ts:
            best_key = seq
            best_ctx = ctx
            break
    return best_key, best_ctx


def build_joined_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    context_map, context_meta = load_context_map()
    episodes, economic_meta = load_economic_rows()
    unmatched_context = dict(context_map)
    joined: list[dict[str, Any]] = []
    unjoined: list[dict[str, Any]] = []
    join_method_counts: Counter[str] = Counter()

    for ep in episodes:
        seq = ep.get("trade_sequence_id")
        ctx = context_map.get(seq)
        join_method = None
        resolved_seq = seq
        if ctx is not None:
            join_method = "trade_sequence_id"
            unmatched_context.pop(seq, None)
        else:
            resolved_seq, ctx = fallback_match(ep, unmatched_context)
            if ctx is not None:
                join_method = "timestamp_fallback"
                unmatched_context.pop(resolved_seq, None)

        if ctx is None:
            unjoined.append({
                "trade_sequence_id": seq,
                "entry_timestamp": ep.get("entry_timestamp") or ep.get("opened_ts_event"),
                "exit_timestamp": ep.get("exit_timestamp") or ep.get("closed_ts_event"),
                "episode_id": ep.get("episode_id"),
                "join_status": "UNJOINED",
            })
            continue

        join_method_counts[join_method] += 1
        gross_pnl = as_float(ep.get("gross_pnl"))
        if gross_pnl is None:
            gross_pnl = as_float(ep.get("realized_pnl_quote_gross"))
        fee_paid = as_float(ep.get("fee_paid"))
        if fee_paid is None:
            fee_paid = as_float(ep.get("fee_quote"))
        net_pnl = as_float(ep.get("net_pnl"))
        if net_pnl is None:
            net_pnl = as_float(ep.get("realized_pnl_quote_net"))

        row = {
            "trade_sequence_id": int(resolved_seq),
            "episode_id": ep.get("episode_id"),
            "join_status": "JOINED",
            "join_method": join_method,
            "entry_timestamp": str(ctx.get("entry_timestamp")) if ctx.get("entry_timestamp") is not None else None,
            "entry_pressure": as_float(ctx.get("entry_pressure")),
            "entry_abs_pressure": as_float(ctx.get("entry_abs_pressure")),
            "entry_threshold": as_float(ctx.get("entry_threshold")),
            "entry_side": ctx.get("entry_side"),
            "entry_signal_reason": ctx.get("entry_signal_reason"),
            "exit_timestamp": str(ctx.get("exit_timestamp")) if ctx.get("exit_timestamp") is not None else None,
            "exit_pressure": as_float(ctx.get("exit_pressure")),
            "exit_abs_pressure": as_float(ctx.get("exit_abs_pressure")),
            "exit_reason": ctx.get("exit_reason"),
            "hold_duration_ms": as_float(ctx.get("hold_duration_ms")),
            "was_reversal_trade": bool(ctx.get("was_reversal_trade")),
            "max_abs_pressure_seen_during_trade": as_float(ctx.get("max_abs_pressure_seen_during_trade")),
            "min_abs_pressure_seen_during_trade": as_float(ctx.get("min_abs_pressure_seen_during_trade")),
            "pressure_decay_at_exit": as_float(ctx.get("pressure_decay_at_exit")),
            "observation_count_during_trade": int(ctx.get("observation_count_during_trade") or 0),
            "gross_pnl": gross_pnl,
            "fee_paid": fee_paid,
            "net_pnl": net_pnl,
        }
        row["duration_bucket"] = duration_bucket(row["hold_duration_ms"])
        row["pressure_bucket"] = pressure_bucket(row["entry_abs_pressure"])
        row["decay_bucket"] = decay_bucket(row["pressure_decay_at_exit"])
        row["pnl_per_trade"] = gross_pnl
        row["pnl_per_ms"] = safe_div(gross_pnl, row["hold_duration_ms"])
        row["fee_ratio"] = safe_div(fee_paid, abs(gross_pnl)) if gross_pnl not in (None, 0.0) else None
        joined.append(row)

    join_meta = {
        "closed_trades_total": len(episodes),
        "joined_trades": len(joined),
        "unjoined_trades": len(unjoined),
        "join_coverage_ratio": round_or_none(safe_div(float(len(joined)), float(len(episodes)))),
        "join_method_counts": dict(sorted(join_method_counts.items())),
        "context_meta": context_meta,
        "economic_meta": economic_meta,
        "unmatched_context_rows": len(unmatched_context),
    }
    return joined, unjoined, join_meta


def summarize_groups(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    total_gross = sum((row["gross_pnl"] or 0.0) for row in rows)
    total_net = sum((row["net_pnl"] or 0.0) for row in rows)
    out: dict[str, Any] = {}
    groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row[key])].append(row)
    for group_key in sorted(groups):
        group = groups[group_key]
        gross = sum((row["gross_pnl"] or 0.0) for row in group)
        net = sum((row["net_pnl"] or 0.0) for row in group)
        fees = sum((row["fee_paid"] or 0.0) for row in group)
        out[group_key] = {
            "trade_count": len(group),
            "avg_gross_pnl": round_or_none(safe_div(gross, float(len(group)))),
            "avg_net_pnl": round_or_none(safe_div(net, float(len(group)))),
            "avg_fee_paid": round_or_none(safe_div(fees, float(len(group)))),
            "gross_pnl_total": round_or_none(gross),
            "net_pnl_total": round_or_none(net),
            "gross_contribution_share": round_or_none(safe_div(gross, total_gross)),
            "net_contribution_share": round_or_none(safe_div(net, total_net)) if total_net != 0 else None,
            "positive_gross_rate": round_or_none(safe_div(sum(1 for row in group if (row["gross_pnl"] or 0.0) > 0), float(len(group)))),
            "positive_net_rate": round_or_none(safe_div(sum(1 for row in group if (row["net_pnl"] or 0.0) > 0), float(len(group)))),
        }
    return out


def summarize_interactions(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_gross = sum((row["gross_pnl"] or 0.0) for row in rows)
    combos = {
        "high_pressure_plus_long_duration": [
            row for row in rows if row["entry_abs_pressure"] is not None and row["entry_abs_pressure"] >= 0.3 and (row["hold_duration_ms"] or 0.0) >= 20_000
        ],
        "high_pressure_plus_reversal": [
            row for row in rows if row["entry_abs_pressure"] is not None and row["entry_abs_pressure"] >= 0.3 and row["was_reversal_trade"]
        ],
        "long_duration_plus_reversal": [
            row for row in rows if (row["hold_duration_ms"] or 0.0) >= 20_000 and row["was_reversal_trade"]
        ],
    }
    out: dict[str, Any] = {}
    for name, group in combos.items():
        gross = sum((row["gross_pnl"] or 0.0) for row in group)
        net = sum((row["net_pnl"] or 0.0) for row in group)
        out[name] = {
            "trade_count": len(group),
            "avg_gross_pnl": round_or_none(safe_div(gross, float(len(group)))) if group else None,
            "avg_net_pnl": round_or_none(safe_div(net, float(len(group)))) if group else None,
            "gross_pnl_total": round_or_none(gross),
            "net_pnl_total": round_or_none(net),
            "gross_contribution_share": round_or_none(safe_div(gross, total_gross)),
        }
    return out


def profit_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_gross = sum((row["gross_pnl"] or 0.0) for row in rows)
    ordered = sorted(rows, key=lambda row: row["gross_pnl"] or 0.0, reverse=True)
    top_10_n = max(1, math.ceil(len(rows) * 0.10))
    top_20_n = max(1, math.ceil(len(rows) * 0.20))
    top_10 = ordered[:top_10_n]
    top_20 = ordered[:top_20_n]
    running = 0.0
    subset: list[dict[str, Any]] = []
    for row in ordered:
        subset.append(row)
        running += row["gross_pnl"] or 0.0
        if total_gross > 0 and running > 0.5 * total_gross:
            break
    return {
        "top_10pct_trade_count": len(top_10),
        "top_10pct_gross_share": round_or_none(safe_div(sum((row["gross_pnl"] or 0.0) for row in top_10), total_gross)),
        "top_20pct_trade_count": len(top_20),
        "top_20pct_gross_share": round_or_none(safe_div(sum((row["gross_pnl"] or 0.0) for row in top_20), total_gross)),
        "minimum_subset_trade_count_producing_gt_50pct_gross": len(subset),
        "minimum_subset_trade_ratio_producing_gt_50pct_gross": round_or_none(safe_div(float(len(subset)), float(len(rows)))),
        "minimum_subset_gross_pnl_total": round_or_none(sum((row["gross_pnl"] or 0.0) for row in subset)),
        "minimum_subset_net_pnl_total": round_or_none(sum((row["net_pnl"] or 0.0) for row in subset)),
        "minimum_subset_fee_total": round_or_none(sum((row["fee_paid"] or 0.0) for row in subset)),
    }


def top_rows(rows: list[dict[str, Any]], field: str, n: int, reverse: bool) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: row[field] if row[field] is not None else float("-inf" if reverse else "inf"), reverse=reverse)
    out = []
    for row in ordered[:n]:
        out.append({
            "trade_sequence_id": row["trade_sequence_id"],
            "entry_abs_pressure": row["entry_abs_pressure"],
            "hold_duration_ms": row["hold_duration_ms"],
            "duration_bucket": row["duration_bucket"],
            "pressure_bucket": row["pressure_bucket"],
            "was_reversal_trade": row["was_reversal_trade"],
            "exit_reason": row["exit_reason"],
            "gross_pnl": row["gross_pnl"],
            "fee_paid": row["fee_paid"],
            "net_pnl": row["net_pnl"],
        })
    return out


def basic_summary(rows: list[dict[str, Any]], batch_result: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    gross = [row["gross_pnl"] or 0.0 for row in rows]
    net = [row["net_pnl"] or 0.0 for row in rows]
    fees = [row["fee_paid"] or 0.0 for row in rows]
    durations = [row["hold_duration_ms"] for row in rows if row["hold_duration_ms"] is not None]
    return {
        "batch_result": {
            "attempted_count": batch_result.get("attempted_count"),
            "completed_count": batch_result.get("completed_count"),
            "run_exit_code": (batch_result.get("results") or [{}])[0].get("run_exit_code"),
            "verify_soft_live_pass": (batch_result.get("results") or [{}])[0].get("verify_soft_live_pass"),
        },
        "summary_surface": {
            "processed_event_count": summary.get("processed_event_count"),
            "stop_reason": summary.get("stop_reason"),
            "run_duration_sec": summary.get("run_duration_sec"),
            "verify_soft_live_pass": summary.get("verify_soft_live_pass"),
        },
        "trade_level_summary": {
            "closed_trade_count": len(rows),
            "gross_pnl_total": round_or_none(sum(gross)),
            "net_pnl_total": round_or_none(sum(net)),
            "fee_paid_total": round_or_none(sum(fees)),
            "avg_gross_pnl_per_trade": round_or_none(safe_div(sum(gross), float(len(rows)))),
            "avg_net_pnl_per_trade": round_or_none(safe_div(sum(net), float(len(rows)))),
            "avg_fee_paid_per_trade": round_or_none(safe_div(sum(fees), float(len(rows)))),
            "median_gross_pnl_per_trade": round_or_none(float(median(gross))),
            "median_net_pnl_per_trade": round_or_none(float(median(net))),
            "pnl_per_trade_distribution_gross": dist(gross),
            "pnl_per_trade_distribution_net": dist(net),
            "duration_distribution_ms": dist(durations),
            "positive_gross_trade_count": sum(1 for value in gross if value > 0),
            "negative_gross_trade_count": sum(1 for value in gross if value < 0),
            "positive_net_trade_count": sum(1 for value in net if value > 0),
            "negative_net_trade_count": sum(1 for value in net if value < 0),
        },
    }


def determine_patterns(rows: list[dict[str, Any]], group_duration: dict[str, Any], group_pressure: dict[str, Any], group_reversal: dict[str, Any], interactions: dict[str, Any]) -> dict[str, Any]:
    best_duration = max(group_duration.items(), key=lambda kv: kv[1]["avg_gross_pnl"] if kv[1]["avg_gross_pnl"] is not None else float("-inf"))[0]
    worst_duration = min(group_duration.items(), key=lambda kv: kv[1]["avg_net_pnl"] if kv[1]["avg_net_pnl"] is not None else float("inf"))[0]
    best_pressure = max(group_pressure.items(), key=lambda kv: kv[1]["avg_gross_pnl"] if kv[1]["avg_gross_pnl"] is not None else float("-inf"))[0]
    worst_pressure = min(group_pressure.items(), key=lambda kv: kv[1]["avg_net_pnl"] if kv[1]["avg_net_pnl"] is not None else float("inf"))[0]
    reversal_key = "True" if "True" in group_reversal else "true"
    non_reversal_key = "False" if "False" in group_reversal else "false"
    return {
        "best_duration_bucket_by_avg_gross": best_duration,
        "worst_duration_bucket_by_avg_net": worst_duration,
        "best_pressure_bucket_by_avg_gross": best_pressure,
        "worst_pressure_bucket_by_avg_net": worst_pressure,
        "reversal_vs_non_reversal": {
            "reversal": group_reversal.get(reversal_key),
            "non_reversal": group_reversal.get(non_reversal_key),
        },
        "best_interaction_by_avg_gross": max(
            interactions.items(),
            key=lambda kv: kv[1]["avg_gross_pnl"] if kv[1]["avg_gross_pnl"] is not None else float("-inf"),
        )[0],
    }


def derive_rule_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[tuple[str, list[dict[str, Any]]]] = [
        (
            "entry_abs_pressure >= 0.3",
            [row for row in rows if row["entry_abs_pressure"] is not None and row["entry_abs_pressure"] >= 0.3],
        ),
        (
            "hold_duration_ms >= 20000",
            [row for row in rows if (row["hold_duration_ms"] or 0.0) >= 20_000],
        ),
        (
            "was_reversal_trade == true",
            [row for row in rows if row["was_reversal_trade"]],
        ),
        (
            "entry_abs_pressure >= 0.3 AND hold_duration_ms >= 20000",
            [row for row in rows if row["entry_abs_pressure"] is not None and row["entry_abs_pressure"] >= 0.3 and (row["hold_duration_ms"] or 0.0) >= 20_000],
        ),
        (
            "entry_abs_pressure >= 0.3 AND was_reversal_trade == true",
            [row for row in rows if row["entry_abs_pressure"] is not None and row["entry_abs_pressure"] >= 0.3 and row["was_reversal_trade"]],
        ),
        (
            "hold_duration_ms >= 20000 AND was_reversal_trade == true",
            [row for row in rows if (row["hold_duration_ms"] or 0.0) >= 20_000 and row["was_reversal_trade"]],
        ),
        (
            "entry_abs_pressure >= 0.3 AND hold_duration_ms >= 20000 AND was_reversal_trade == true",
            [
                row for row in rows
                if row["entry_abs_pressure"] is not None
                and row["entry_abs_pressure"] >= 0.3
                and (row["hold_duration_ms"] or 0.0) >= 20_000
                and row["was_reversal_trade"]
            ],
        ),
    ]
    out: list[dict[str, Any]] = []
    total_gross = sum((row["gross_pnl"] or 0.0) for row in rows)
    for rule, group in candidates:
        if not group:
            continue
        gross = sum((row["gross_pnl"] or 0.0) for row in group)
        net = sum((row["net_pnl"] or 0.0) for row in group)
        fees = sum((row["fee_paid"] or 0.0) for row in group)
        out.append({
            "rule": rule,
            "trade_count": len(group),
            "trade_ratio": round_or_none(safe_div(float(len(group)), float(len(rows)))),
            "gross_pnl_total": round_or_none(gross),
            "net_pnl_total": round_or_none(net),
            "fee_paid_total": round_or_none(fees),
            "avg_gross_pnl_per_trade": round_or_none(safe_div(gross, float(len(group)))),
            "avg_net_pnl_per_trade": round_or_none(safe_div(net, float(len(group)))),
            "gross_share": round_or_none(safe_div(gross, total_gross)),
            "positive_gross_rate": round_or_none(safe_div(sum(1 for row in group if (row["gross_pnl"] or 0.0) > 0), float(len(group)))),
            "positive_net_rate": round_or_none(safe_div(sum(1 for row in group if (row["net_pnl"] or 0.0) > 0), float(len(group)))),
        })
    out.sort(key=lambda row: (
        row["avg_gross_pnl_per_trade"] if row["avg_gross_pnl_per_trade"] is not None else float("-inf"),
        row["gross_pnl_total"] if row["gross_pnl_total"] is not None else float("-inf"),
    ), reverse=True)
    return out


def salvage_decision(rows: list[dict[str, Any]], concentration: dict[str, Any], rule_candidates: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    total_gross = sum((row["gross_pnl"] or 0.0) for row in rows)
    subset_ratio = concentration["minimum_subset_trade_ratio_producing_gt_50pct_gross"] or 1.0
    subset_net = concentration["minimum_subset_net_pnl_total"] or 0.0
    subset_gross = concentration["minimum_subset_gross_pnl_total"] or 0.0
    near_zero_net = subset_gross > 0 and subset_net >= -0.10 * abs(subset_gross)
    best_rule = rule_candidates[0] if rule_candidates else None

    separable = False
    separability_reason = "no rule candidate was found"
    rule_hints: list[str] = []
    if best_rule is not None:
        trade_ratio = best_rule["trade_ratio"] or 1.0
        gross_share = best_rule["gross_share"] or 0.0
        avg_gross = best_rule["avg_gross_pnl_per_trade"] or 0.0
        avg_net = best_rule["avg_net_pnl_per_trade"] or 0.0
        if trade_ratio <= 0.30 and gross_share >= 0.50 and avg_gross > 0 and avg_net >= -0.001:
            separable = True
            separability_reason = "simple thresholds isolate a small subset with majority gross and near-flat net"
            rule_hints.append(best_rule["rule"])
        else:
            separability_reason = "simple thresholds improve average gross but do not isolate a clean small subset with majority gross"

    if total_gross <= 0:
        verdict = "NO_SALVAGE"
    elif separable:
        verdict = "CLEAR_SUBSET_EDGE"
    elif subset_ratio <= 0.30 and near_zero_net:
        verdict = "NARROW_SUBSET_SALVAGEABLE"
    else:
        verdict = "NO_SALVAGE"

    if verdict == "CLEAR_SUBSET_EDGE":
        final_decision = "ESCALATE_TO_V2"
    elif verdict == "NARROW_SUBSET_SALVAGEABLE":
        final_decision = "RESHAPE (WITH RULE HINTS)"
        if not rule_hints and best_rule is not None:
            rule_hints.append(best_rule["rule"])
    else:
        final_decision = "KILL"

    return verdict, {
        "separable_with_simple_features": separable,
        "separability_reason": separability_reason,
        "rule_hints": rule_hints,
        "best_rule_candidate": best_rule,
        "final_decision": final_decision,
    }


def build_report(doc: dict[str, Any]) -> str:
    join = doc["join_quality"]
    conc = doc["profit_concentration"]
    patterns = doc["pattern_summary"]
    separability = doc["feature_separability"]
    top_rule = separability.get("best_rule_candidate")
    lines = [
        "# Phase7 Trade Attribution v2",
        "",
        "This report joins full-rerun microstructure trade context with paper-ledger trade economics for `linkusdt` only.",
        "",
        f"- Join coverage: `{join['joined_trades']}/{join['closed_trades_total']}` ({(join['join_coverage_ratio'] or 0.0):.1%})",
        f"- Profit concentration: top 10% trades carry `{(conc['top_10pct_gross_share'] or 0.0):.1%}` of gross, top 20% carry `{(conc['top_20pct_gross_share'] or 0.0):.1%}`",
        f"- Minimum subset for >50% gross: `{conc['minimum_subset_trade_count_producing_gt_50pct_gross']}` trades ({(conc['minimum_subset_trade_ratio_producing_gt_50pct_gross'] or 0.0):.1%}), subset net `{conc['minimum_subset_net_pnl_total']}`",
        "",
        "## Best/Worst Patterns",
        f"- Best duration bucket by avg gross: `{patterns['best_duration_bucket_by_avg_gross']}`",
        f"- Worst duration bucket by avg net: `{patterns['worst_duration_bucket_by_avg_net']}`",
        f"- Best pressure bucket by avg gross: `{patterns['best_pressure_bucket_by_avg_gross']}`",
        f"- Worst pressure bucket by avg net: `{patterns['worst_pressure_bucket_by_avg_net']}`",
        f"- Best interaction by avg gross: `{patterns['best_interaction_by_avg_gross']}`",
        "",
        "## Separability",
        f"- Verdict: `{doc['salvage_verdict']}`",
        f"- Simple-feature separability: `{separability['separable_with_simple_features']}`",
        f"- Reason: {separability['separability_reason']}",
    ]
    if top_rule:
        lines.append(
            f"- Strongest simple rule candidate: `{top_rule['rule']}` "
            f"(trade_ratio={(top_rule['trade_ratio'] or 0.0):.1%}, gross_share={(top_rule['gross_share'] or 0.0):.1%}, "
            f"avg_gross={top_rule['avg_gross_pnl_per_trade']}, avg_net={top_rule['avg_net_pnl_per_trade']})"
        )
    if separability.get("rule_hints"):
        lines.append(f"- Rule hints: {', '.join(f'`{rule}`' for rule in separability['rule_hints'])}")
    lines.extend([
        "",
        f"FINAL DECISION: `{separability['final_decision']}`",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    joined, unjoined, join_meta = build_joined_rows()
    batch_result = read_json(BATCH_RESULT_JSON)
    summary = read_json(SUMMARY_JSON)
    basic = basic_summary(joined, batch_result, summary)
    concentration_stats = profit_concentration(joined)
    by_duration = summarize_groups(joined, "duration_bucket")
    by_pressure = summarize_groups(joined, "pressure_bucket")
    by_reversal = summarize_groups(joined, "was_reversal_trade")
    by_decay = summarize_groups(joined, "decay_bucket")
    interactions = summarize_interactions(joined)
    patterns = determine_patterns(joined, by_duration, by_pressure, by_reversal, interactions)
    rule_candidates = derive_rule_candidates(joined)
    verdict, separability = salvage_decision(joined, concentration_stats, rule_candidates)

    failure_patterns = {
        "worst_25_by_gross": top_rows(joined, "gross_pnl", max(1, math.ceil(len(joined) * 0.25)), reverse=False)[:10],
        "most_negative_net_trades": top_rows(joined, "net_pnl", 10, reverse=False),
        "common_exit_reasons_in_worst_25pct_gross": dict(
            sorted(
                Counter(row["exit_reason"] for row in sorted(joined, key=lambda row: row["gross_pnl"] or 0.0)[: max(1, math.ceil(len(joined) * 0.25))]).items()
            )
        ),
    }

    doc = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now(),
        "authoritative_sources": {
            "context_jsonl": str(CONTEXT_JSONL),
            "economic_json": str(ECON_JSON),
            "batch_result_json": str(BATCH_RESULT_JSON),
            "summary_json": str(SUMMARY_JSON),
        },
        "join_quality": join_meta,
        "basic_summary": basic,
        "profit_concentration": concentration_stats,
        "segment_analysis": {
            "duration_bucket": by_duration,
            "pressure_bucket": by_pressure,
            "reversal_bucket": by_reversal,
            "decay_bucket": by_decay,
        },
        "interaction_analysis": interactions,
        "pattern_summary": patterns,
        "failure_patterns": failure_patterns,
        "feature_separability": separability,
        "salvage_verdict": verdict,
        "joined_trade_rows": joined,
        "unjoined_rows": unjoined,
        "rule_candidates": rule_candidates[:10],
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(doc, indent=2))
    OUTPUT_REPORT_MD.write_text(build_report(doc))

    print("PHASE7_TRADE_ATTRIBUTION_V2_COMPLETE")
    print(f"OUTPUT_JSON={OUTPUT_JSON}")
    print(f"REPORT_MD={OUTPUT_REPORT_MD}")
    print(f"SALVAGE_VERDICT={verdict}")
    print(f"FINAL_DECISION={separability['final_decision']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
