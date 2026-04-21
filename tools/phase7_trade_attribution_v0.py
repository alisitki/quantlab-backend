#!/usr/bin/env python3
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


ROOT = Path("/home/deploy/quantlab-backend")
PHASE7_RESULT_JSON = ROOT / "tools/phase7_microstructure_shadow_result_v1.json"
OUTPUT_JSON = ROOT / "tools/phase7_trade_attribution_v0.json"
OUTPUT_DIR = ROOT / "tools/phase7_trade_attribution_output"
OUTPUT_REPORT_MD = OUTPUT_DIR / "phase7_trade_attribution_report_v0.md"
TARGET_STRATEGY_ID = "microstructure_imbalance_v1__bybit__linkusdt__trade__d100__h500__pt020"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
    return json.loads(path.read_text())


def finite_or_none(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def pct(numerator: float, denominator: float):
    if denominator == 0:
        return None
    return numerator / denominator


def quantile_rows(rows: list[dict], field: str, q: float):
    if not rows:
        return None
    ordered = sorted(float(row[field]) for row in rows)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def duration_bucket(duration_ms: float) -> str:
    if duration_ms < 1_000:
        return "<1s"
    if duration_ms < 5_000:
        return "1-5s"
    if duration_ms < 20_000:
        return "5-20s"
    return ">20s"


@dataclass
class ResolvedRun:
    result_row: dict
    futures_paper_ledger_json: Path
    execution_ledger_jsonl: Path
    execution_events_jsonl: Path
    trade_ledger_jsonl: Path


def resolve_run() -> ResolvedRun:
    doc = load_json(PHASE7_RESULT_JSON)
    for row in doc.get("results", []):
        if row.get("strategy_id") == TARGET_STRATEGY_ID:
            artifacts = row.get("artifacts", {})
            return ResolvedRun(
                result_row=row,
                futures_paper_ledger_json=Path(artifacts["futures_paper_ledger_json"]),
                execution_ledger_jsonl=Path(artifacts["execution_ledger_jsonl"]),
                execution_events_jsonl=Path(artifacts["execution_events_jsonl"]),
                trade_ledger_jsonl=Path(artifacts["trade_ledger_jsonl"]),
            )
    raise SystemExit(f"target strategy not found in {PHASE7_RESULT_JSON}")


def build_trade_rows(episodes: list[dict]) -> list[dict]:
    rows = []
    for episode in episodes:
        if episode.get("status") != "CLOSED":
            continue
        opened_ts = int(episode["opened_ts_event"])
        closed_ts = int(episode["closed_ts_event"])
        gross = float(episode["realized_pnl_quote_gross"])
        fees = float(episode["fee_quote"])
        net = float(episode["realized_pnl_quote_net"])
        dur_ms = (closed_ts - opened_ts) / 1_000_000.0
        rows.append({
            "episode_id": episode["episode_id"],
            "entry_timestamp": episode["opened_ts_event"],
            "exit_timestamp": episode["closed_ts_event"],
            "duration_ms": dur_ms,
            "duration_bucket": duration_bucket(dur_ms),
            "side": episode["direction"],
            "entry_pressure": None,
            "exit_pressure": None,
            "pressure_bucket": None,
            "gross_pnl": gross,
            "fees": fees,
            "net_pnl": net,
            "pnl_per_trade": gross,
            "net_pnl_per_trade": net,
            "pnl_per_ms": gross / dur_ms if dur_ms > 0 else None,
            "fee_ratio": (fees / abs(gross)) if gross != 0 else None,
            "is_reversal": "REVERSAL" in str(episode.get("close_action") or "") or "REVERSAL" in str(episode.get("open_action") or ""),
            "open_action": episode.get("open_action"),
            "close_action": episode.get("close_action"),
            "entry_price": finite_or_none(episode.get("entry_price")),
            "exit_price": finite_or_none(episode.get("exit_price")),
            "status": episode.get("status"),
        })
    return rows


def summarize_rows(rows: list[dict]) -> dict:
    gross_values = [row["gross_pnl"] for row in rows]
    net_values = [row["net_pnl"] for row in rows]
    fee_values = [row["fees"] for row in rows]
    fee_ratios = [row["fee_ratio"] for row in rows if row["fee_ratio"] is not None]
    duration_values = [row["duration_ms"] for row in rows]
    return {
        "trade_count": len(rows),
        "gross_pnl_total": sum(gross_values),
        "net_pnl_total": sum(net_values),
        "fee_total": sum(fee_values),
        "avg_gross_pnl_per_trade": sum(gross_values) / len(rows),
        "avg_net_pnl_per_trade": sum(net_values) / len(rows),
        "avg_fee_per_trade": sum(fee_values) / len(rows),
        "median_gross_pnl_per_trade": median(gross_values),
        "median_net_pnl_per_trade": median(net_values),
        "median_fee_ratio": median(fee_ratios) if fee_ratios else None,
        "positive_gross_trade_count": sum(1 for row in rows if row["gross_pnl"] > 0),
        "negative_gross_trade_count": sum(1 for row in rows if row["gross_pnl"] < 0),
        "zero_gross_trade_count": sum(1 for row in rows if row["gross_pnl"] == 0),
        "positive_net_trade_count": sum(1 for row in rows if row["net_pnl"] > 0),
        "negative_net_trade_count": sum(1 for row in rows if row["net_pnl"] < 0),
        "zero_net_trade_count": sum(1 for row in rows if row["net_pnl"] == 0),
        "avg_duration_ms": sum(duration_values) / len(duration_values),
        "median_duration_ms": median(duration_values),
        "duration_ms_p90": quantile_rows(rows, "duration_ms", 0.90),
        "gross_pnl_per_1k_events": None,
    }


def concentration(rows: list[dict], total_gross: float) -> dict:
    ordered = sorted(rows, key=lambda row: row["gross_pnl"], reverse=True)
    top_10_n = max(1, math.ceil(len(rows) * 0.10))
    top_20_n = max(1, math.ceil(len(rows) * 0.20))
    top_10 = ordered[:top_10_n]
    top_20 = ordered[:top_20_n]
    worst_50 = sorted(rows, key=lambda row: row["gross_pnl"])[: len(rows) // 2]

    running = 0.0
    subset = []
    for row in ordered:
        subset.append(row)
        running += row["gross_pnl"]
        if total_gross > 0 and running >= 0.5 * total_gross:
            break

    return {
        "top_10pct_trade_count": len(top_10),
        "top_10pct_gross_pnl_share": pct(sum(row["gross_pnl"] for row in top_10), total_gross),
        "top_10pct_net_pnl_total": sum(row["net_pnl"] for row in top_10),
        "top_20pct_trade_count": len(top_20),
        "top_20pct_gross_pnl_share": pct(sum(row["gross_pnl"] for row in top_20), total_gross),
        "top_20pct_net_pnl_total": sum(row["net_pnl"] for row in top_20),
        "worst_50pct_trade_count": len(worst_50),
        "worst_50pct_gross_pnl_total": sum(row["gross_pnl"] for row in worst_50),
        "worst_50pct_net_pnl_total": sum(row["net_pnl"] for row in worst_50),
        "majority_gross_subset_trade_count": len(subset),
        "majority_gross_subset_trade_ratio": len(subset) / len(rows),
        "majority_gross_subset_gross_pnl_total": sum(row["gross_pnl"] for row in subset),
        "majority_gross_subset_net_pnl_total": sum(row["net_pnl"] for row in subset),
        "majority_gross_subset_fee_total": sum(row["fees"] for row in subset),
    }


def group_summary(rows: list[dict], key: str, total_gross: float) -> dict:
    out = {}
    for value in sorted({row[key] for row in rows}):
        group = [row for row in rows if row[key] == value]
        gross = sum(row["gross_pnl"] for row in group)
        net = sum(row["net_pnl"] for row in group)
        fees = sum(row["fees"] for row in group)
        out[str(value)] = {
            "trade_count": len(group),
            "avg_gross_pnl": gross / len(group),
            "avg_net_pnl": net / len(group),
            "avg_fee": fees / len(group),
            "avg_pnl_per_ms": sum(row["pnl_per_ms"] for row in group if row["pnl_per_ms"] is not None) / len([row for row in group if row["pnl_per_ms"] is not None]),
            "avg_fee_ratio": sum(row["fee_ratio"] for row in group if row["fee_ratio"] is not None) / len([row for row in group if row["fee_ratio"] is not None]) if any(row["fee_ratio"] is not None for row in group) else None,
            "gross_pnl_total": gross,
            "net_pnl_total": net,
            "gross_pnl_share": pct(gross, total_gross),
            "positive_gross_trade_rate": sum(1 for row in group if row["gross_pnl"] > 0) / len(group),
            "positive_net_trade_rate": sum(1 for row in group if row["net_pnl"] > 0) / len(group),
        }
    return out


def reversal_summary(rows: list[dict], total_gross: float) -> dict:
    groups = {}
    for value in [True, False]:
        group = [row for row in rows if row["is_reversal"] is value]
        gross = sum(row["gross_pnl"] for row in group)
        net = sum(row["net_pnl"] for row in group)
        fees = sum(row["fees"] for row in group)
        groups[str(value).lower()] = {
            "trade_count": len(group),
            "gross_pnl_total": gross,
            "net_pnl_total": net,
            "fee_total": fees,
            "gross_pnl_share": pct(gross, total_gross),
            "avg_gross_pnl": gross / len(group) if group else None,
            "avg_net_pnl": net / len(group) if group else None,
            "avg_fee": fees / len(group) if group else None,
        }
    return groups


def pick_samples(rows: list[dict], reverse: bool) -> list[dict]:
    ordered = sorted(rows, key=lambda row: row["gross_pnl"], reverse=reverse)
    sample = []
    for row in ordered[:5]:
        sample.append({
            "episode_id": row["episode_id"],
            "duration_bucket": row["duration_bucket"],
            "duration_ms": row["duration_ms"],
            "side": row["side"],
            "gross_pnl": row["gross_pnl"],
            "fees": row["fees"],
            "net_pnl": row["net_pnl"],
            "is_reversal": row["is_reversal"],
        })
    return sample


def determine_verdict(total_gross: float, concentration_stats: dict) -> tuple[str, bool, str]:
    subset_ratio = concentration_stats["majority_gross_subset_trade_ratio"]
    subset_gross = concentration_stats["majority_gross_subset_gross_pnl_total"]
    subset_net = concentration_stats["majority_gross_subset_net_pnl_total"]
    near_zero_net = subset_net >= -0.10 * abs(subset_gross)
    salvageable = total_gross > 0 and subset_ratio <= 0.30 and (subset_net > 0 or near_zero_net)
    if not salvageable:
        return ("NO_SALVAGE", False, "gross profit is not concentrated in a small near-net-neutral subset")
    if subset_net > 0:
        return ("CLEAR_SUBSET_EDGE", True, "small subset carries majority of gross pnl and is already net positive")
    return ("NARROW_SUBSET_SALVAGEABLE", True, "small subset carries majority of gross pnl and is near net neutral, but not directly actionable from current artifacts")


def build_report(doc: dict) -> str:
    totals = doc["trade_level_summary"]
    concentration_stats = doc["profit_concentration"]
    duration_stats = doc["segment_analysis"]["duration_bucket"]
    reversal_stats = doc["segment_analysis"]["is_reversal"]
    verdict = doc["final_verdict"]

    best_duration = max(duration_stats.items(), key=lambda kv: kv[1]["avg_gross_pnl"])
    worst_duration = min(duration_stats.items(), key=lambda kv: kv[1]["avg_net_pnl"])

    return "\n".join([
        "# Phase7 Trade Attribution v0",
        "",
        f"- strategy: `{doc['strategy_id']}`",
        f"- verdict: `{verdict['verdict']}`",
        f"- reason: {verdict['reason']}",
        f"- closed trades analyzed: `{totals['trade_count']}`",
        f"- total gross pnl: `{totals['gross_pnl_total']}`",
        f"- total net pnl: `{totals['net_pnl_total']}`",
        f"- total fees: `{totals['fee_total']}`",
        f"- top 10% trades gross share: `{concentration_stats['top_10pct_gross_pnl_share']}`",
        f"- top 20% trades gross share: `{concentration_stats['top_20pct_gross_pnl_share']}`",
        f"- majority-gross subset ratio: `{concentration_stats['majority_gross_subset_trade_ratio']}`",
        f"- majority-gross subset net pnl: `{concentration_stats['majority_gross_subset_net_pnl_total']}`",
        f"- best duration bucket by avg gross/trade: `{best_duration[0]}`",
        f"- worst duration bucket by avg net/trade: `{worst_duration[0]}`",
        f"- reversal trade share of gross: `{reversal_stats['true']['gross_pnl_share']}`",
        "",
        "Pressure buckets are not available in the current artifact surface because entry/exit pressure is not persisted in the Phase7 ledgers.",
        "",
        "Kill vs reshape:",
        f"- `{verdict['verdict']}`",
    ]) + "\n"


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    resolved = resolve_run()
    phase7_doc = load_json(PHASE7_RESULT_JSON)
    paper_doc = load_json(resolved.futures_paper_ledger_json)
    item = paper_doc["items"][0]
    trade_rows = build_trade_rows(item.get("episodes") or [])
    if not trade_rows:
        raise SystemExit("no closed trade episodes found")

    totals = summarize_rows(trade_rows)
    totals["gross_pnl_per_1k_events"] = totals["gross_pnl_total"] / (resolved.result_row["metrics"]["processed_event_count"] / 1000.0)
    totals["net_pnl_per_1k_events"] = totals["net_pnl_total"] / (resolved.result_row["metrics"]["processed_event_count"] / 1000.0)

    concentration_stats = concentration(trade_rows, totals["gross_pnl_total"])
    duration_stats = group_summary(trade_rows, "duration_bucket", totals["gross_pnl_total"])
    reversal_stats = reversal_summary(trade_rows, totals["gross_pnl_total"])
    verdict_value, salvageable, verdict_reason = determine_verdict(totals["gross_pnl_total"], concentration_stats)

    doc = {
        "schema_version": "phase7_trade_attribution_v0",
        "generated_ts_utc": iso_utc_now(),
        "authoritative_sources": {
            "canonical_truth_registry_json": str(ROOT / "tools/system_state/canonical_truth_registry_v0.json"),
            "phase7_microstructure_shadow_result_json": str(PHASE7_RESULT_JSON),
            "futures_paper_ledger_json": str(resolved.futures_paper_ledger_json),
            "execution_ledger_jsonl": str(resolved.execution_ledger_jsonl),
            "execution_events_jsonl": str(resolved.execution_events_jsonl),
            "trade_ledger_jsonl": str(resolved.trade_ledger_jsonl),
        },
        "strategy_id": TARGET_STRATEGY_ID,
        "exchange": resolved.result_row["exchange"],
        "symbol": resolved.result_row["symbol"],
        "family_id": resolved.result_row["family_id"],
        "data_availability": {
            "entry_pressure_available": False,
            "exit_pressure_available": False,
            "pressure_bucket_analysis_available": False,
            "reason": "entry/exit pressure is not persisted in the current Phase7 artifact surface",
        },
        "trade_level_summary": totals,
        "profit_concentration": concentration_stats,
        "segment_analysis": {
            "duration_bucket": duration_stats,
            "pressure_bucket": {
                "status": "NOT_AVAILABLE_IN_CURRENT_ARTIFACT_SURFACE",
            },
            "is_reversal": reversal_stats,
        },
        "patterns": {
            "best_duration_bucket_by_avg_gross_pnl": max(duration_stats.items(), key=lambda kv: kv[1]["avg_gross_pnl"])[0],
            "worst_duration_bucket_by_avg_net_pnl": min(duration_stats.items(), key=lambda kv: kv[1]["avg_net_pnl"])[0],
            "short_duration_trades_mostly_negative_net": duration_stats["<1s"]["positive_net_trade_rate"] < 0.5,
            "high_pressure_trades_consistently_better": None,
            "high_pressure_reason": "NOT_AVAILABLE_IN_CURRENT_ARTIFACT_SURFACE",
        },
        "salvage_test": {
            "salvageable": salvageable,
            "rule": "subset carrying majority gross pnl must be <=30% of trades and net positive or near-neutral",
            "majority_gross_subset_trade_ratio": concentration_stats["majority_gross_subset_trade_ratio"],
            "majority_gross_subset_net_pnl_total": concentration_stats["majority_gross_subset_net_pnl_total"],
        },
        "top_trade_samples": pick_samples(trade_rows, reverse=True),
        "worst_trade_samples": pick_samples(trade_rows, reverse=False),
        "final_verdict": {
            "verdict": verdict_value,
            "reason": verdict_reason,
            "kill_vs_reshape": "RESHAPE" if salvageable else "KILL",
        },
        "source_phase7_metrics": resolved.result_row["metrics"],
        "source_shadow_paper_summary": {
            "replayed_realized_pnl_quote_gross": item["replayed_realized_pnl_quote_gross"],
            "replayed_realized_pnl_quote_net": item["replayed_realized_pnl_quote_net"],
            "total_fee_quote": item["total_fee_quote"],
            "funding_cost_quote": item["funding_cost_quote"],
            "profitability_status": item["profitability_status"],
            "cost_accounting_status": item["cost_accounting_status"],
        },
    }

    OUTPUT_JSON.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    OUTPUT_REPORT_MD.write_text(build_report(doc))
    print(f"wrote_json={OUTPUT_JSON}")
    print(f"wrote_report={OUTPUT_REPORT_MD}")
    print(f"verdict={verdict_value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
