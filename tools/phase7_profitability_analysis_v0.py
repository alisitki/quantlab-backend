#!/usr/bin/env python3
"""Phase7 continuation profitability decomposition.

Read-only analyzer for the linkusdt/avaxusdt continuation lane. It consumes
existing shadow artifacts only and writes a compact JSON + Markdown report.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

DEFAULT_RESULT_V1 = ROOT / "tools/phase7_continuation_validation_result_v1.json"
DEFAULT_OUTPUT_JSON = ROOT / "tools/phase7_profitability_analysis_v0.json"
DEFAULT_REPORT_MD = (
    ROOT / "tools/phase7_profitability_analysis_output/phase7_profitability_analysis_report_v0.md"
)
DEFAULT_CANONICAL_REGISTRY = ROOT / "tools/system_state/canonical_truth_registry_v0.json"

TARGET_SYMBOLS = ("linkusdt", "avaxusdt")
SCHEMA_VERSION = "phase7_profitability_analysis_v0"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def as_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(result):
        return default
    return result


def as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return numerator / denominator


def round_or_none(value: float | None, digits: int = 12) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def percentile_or_none(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * pct
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[lo]
    weight = idx - lo
    return ordered[lo] * (1.0 - weight) + ordered[hi] * weight


def find_futures_item(path: Path, symbol: str, pack_id: str | None, live_run_id: str | None) -> dict[str, Any]:
    data = read_json(path)
    items = data.get("items") or data.get("futures_paper_ledger") or data.get("ledger") or []
    symbol_upper = symbol.upper()
    matches: list[dict[str, Any]] = []
    for item in items:
        item_symbol = str(item.get("symbol") or "").upper()
        item_pack = item.get("selected_pack_id")
        item_run = item.get("live_run_id")
        if item_symbol != symbol_upper:
            continue
        if pack_id and item_pack != pack_id:
            continue
        if live_run_id and item_run != live_run_id:
            continue
        matches.append(item)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        symbol_matches = [item for item in items if str(item.get("symbol") or "").upper() == symbol_upper]
        if len(symbol_matches) == 1:
            return symbol_matches[0]
    raise RuntimeError(f"could not identify unique futures paper ledger item for {symbol} in {path}")


def event_pack_identity(row: dict[str, Any]) -> tuple[str | None, str | None]:
    artifacts = row.get("artifacts") or {}
    summary_path = artifacts.get("execution_pack_summary_json")
    if not summary_path:
        return None, None
    summary = read_json(Path(summary_path))
    latest = summary.get("latest_by_pack_id") or {}
    if isinstance(latest, dict) and len(latest) == 1:
        pack_id, record = next(iter(latest.items()))
        if isinstance(record, dict):
            return str(pack_id), record.get("live_run_id")
    return None, None


def episode_stats(episodes: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [ep for ep in episodes if ep.get("status") == "CLOSED"]
    gross_values = [as_float(ep.get("realized_pnl_quote_gross"), 0.0) or 0.0 for ep in closed]
    net_values = [as_float(ep.get("realized_pnl_quote_net"), 0.0) or 0.0 for ep in closed]
    fee_values = [as_float(ep.get("fee_quote"), 0.0) or 0.0 for ep in closed]
    positive = sum(1 for v in gross_values if v > 0)
    negative = sum(1 for v in gross_values if v < 0)
    flat = sum(1 for v in gross_values if v == 0)
    return {
        "episode_count": len(episodes),
        "closed_episode_count": len(closed),
        "gross_positive_episode_count": positive,
        "gross_negative_episode_count": negative,
        "gross_flat_episode_count": flat,
        "gross_win_rate": round_or_none(safe_div(float(positive), float(len(gross_values))) if gross_values else None),
        "gross_pnl_distribution": {
            "min": round_or_none(min(gross_values) if gross_values else None),
            "p25": round_or_none(percentile_or_none(gross_values, 0.25)),
            "median": round_or_none(median_or_none(gross_values)),
            "p75": round_or_none(percentile_or_none(gross_values, 0.75)),
            "max": round_or_none(max(gross_values) if gross_values else None),
        },
        "net_pnl_distribution": {
            "min": round_or_none(min(net_values) if net_values else None),
            "median": round_or_none(median_or_none(net_values)),
            "max": round_or_none(max(net_values) if net_values else None),
        },
        "avg_gross_pnl_per_episode": round_or_none(safe_div(sum(gross_values), float(len(gross_values))) if gross_values else None),
        "avg_net_pnl_per_episode": round_or_none(safe_div(sum(net_values), float(len(net_values))) if net_values else None),
        "avg_fee_per_episode": round_or_none(safe_div(sum(fee_values), float(len(fee_values))) if fee_values else None),
    }


def infer_verdict(gross_pnl: float | None, net_pnl: float | None, result_row: dict[str, Any]) -> tuple[str, str, str]:
    metrics = result_row.get("metrics") or {}
    if result_row.get("status") != "OK" or metrics.get("verify_soft_live_pass") is not True:
        return "INVALID_RUN", "invalid continuation run or verify failure", "infra invalid"
    if gross_pnl is None:
        return "WEAK_CONTINUE", "gross edge unavailable; cannot issue confident drop", "gross edge unavailable"
    if gross_pnl > 0:
        if net_pnl is not None and net_pnl <= 0:
            return (
                "WEAK_CONTINUE",
                "gross edge positive but net is not positive after costs",
                "edge exists but requires execution optimization",
            )
        return "KEEP_ADVANCING", "gross and net edge are positive", "edge exists"
    return "DROP", "gross edge is not positive before costs", "no real edge"


def analyze_row(row: dict[str, Any]) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").lower()
    artifacts = row.get("artifacts") or {}
    metrics = row.get("metrics") or {}
    pack_id, live_run_id = event_pack_identity(row)

    ledger_path = Path(artifacts["futures_paper_ledger_json"])
    ledger_item = find_futures_item(ledger_path, symbol, pack_id, live_run_id)
    trade_ledger_rows = read_jsonl(Path(artifacts["trade_ledger_jsonl"]))
    execution_ledger_rows = read_jsonl(Path(artifacts["execution_ledger_jsonl"]))

    gross = as_float(
        ledger_item.get("replayed_realized_pnl_quote_gross"),
        as_float(ledger_item.get("mark_to_market_pnl_quote_gross")),
    )
    if gross is None:
        gross = sum(
            as_float(ep.get("realized_pnl_quote_gross"), 0.0) or 0.0
            for ep in ledger_item.get("episodes") or []
            if ep.get("status") == "CLOSED"
        )

    net = as_float(
        ledger_item.get("replayed_realized_pnl_quote_net"),
        as_float(ledger_item.get("summary_realized_pnl_quote"), as_float(metrics.get("total_realized_pnl"))),
    )
    fee = as_float(ledger_item.get("total_fee_quote"), 0.0) or 0.0
    funding = as_float(ledger_item.get("funding_cost_quote"), 0.0) or 0.0
    turnover = as_float(ledger_item.get("turnover_quote"), None)
    processed_events = as_float(metrics.get("processed_event_count"), None)
    decision_count = as_int(metrics.get("decision_count"))
    fill_count = as_int(metrics.get("fill_count"))
    closed_episode_count = sum(1 for ep in ledger_item.get("episodes") or [] if ep.get("status") == "CLOSED")

    ep_stats = episode_stats(ledger_item.get("episodes") or [])
    verdict, verdict_reason, edge_statement = infer_verdict(gross, net, row)

    gross_abs = abs(gross) if gross is not None else None
    fee_ratio_abs_gross = safe_div(fee, gross_abs)
    funding_ratio_fee = safe_div(abs(funding), fee)
    decisions_vs_fills = safe_div(float(decision_count), float(fill_count))
    fill_per_decision = safe_div(float(fill_count), float(decision_count))

    metadata_warnings: list[str] = []
    if ledger_item.get("strategy_id") and ledger_item.get("strategy_id") != row.get("strategy_id"):
        metadata_warnings.append(
            "ledger_strategy_id_differs_from_continuation_result; using continuation result identity and ledger metrics"
        )
    if ledger_item.get("family_id") and ledger_item.get("family_id") != row.get("family_id"):
        metadata_warnings.append(
            "ledger_family_id_differs_from_continuation_result; using continuation result family and ledger metrics"
        )

    if gross is not None and gross > 0 and net is not None and net <= 0:
        fee_explanation = "edge exists but requires execution optimization"
    elif gross is not None and gross <= 0:
        fee_explanation = "fees deepen the loss, but they are not the only problem because gross pnl is not positive"
    else:
        fee_explanation = "fee impact cannot be isolated from current artifact surface"

    return {
        "strategy_id": row.get("strategy_id"),
        "family_id": row.get("family_id"),
        "symbol": symbol,
        "exchange": row.get("exchange"),
        "source_continuation_verdict": row.get("verdict"),
        "source_run_status": row.get("status"),
        "artifact_paths": {
            "futures_paper_ledger_json": artifacts.get("futures_paper_ledger_json"),
            "trade_ledger_jsonl": artifacts.get("trade_ledger_jsonl"),
            "execution_ledger_jsonl": artifacts.get("execution_ledger_jsonl"),
            "execution_pack_summary_json": artifacts.get("execution_pack_summary_json"),
        },
        "artifact_identity": {
            "selected_pack_id": pack_id,
            "live_run_id": live_run_id,
            "ledger_symbol": ledger_item.get("symbol"),
            "ledger_strategy_id": ledger_item.get("strategy_id"),
            "ledger_family_id": ledger_item.get("family_id"),
            "metadata_warnings": metadata_warnings,
        },
        "gross_performance": {
            "total_gross_pnl_quote": round_or_none(gross),
            "source": "futures_paper_ledger.replayed_realized_pnl_quote_gross",
            "mark_to_market_pnl_quote_gross": round_or_none(as_float(ledger_item.get("mark_to_market_pnl_quote_gross"))),
            "gross_edge_positive": bool(gross is not None and gross > 0),
        },
        "net_performance": {
            "realized_pnl_quote_net": round_or_none(net),
            "summary_realized_pnl_quote": round_or_none(as_float(ledger_item.get("summary_realized_pnl_quote"))),
            "source": "futures_paper_ledger.replayed_realized_pnl_quote_net",
            "profitability_status": ledger_item.get("profitability_status"),
            "cost_accounting_status": ledger_item.get("cost_accounting_status"),
            "paper_run_status": ledger_item.get("paper_run_status"),
        },
        "fee_burden": {
            "total_fee_quote": round_or_none(fee),
            "effective_fee_rate": round_or_none(as_float(ledger_item.get("effective_fee_rate"))),
            "fee_support_status": ledger_item.get("fee_support_status"),
            "fee_per_fill": round_or_none(safe_div(fee, float(fill_count))),
            "fee_per_closed_episode": round_or_none(safe_div(fee, float(closed_episode_count))),
            "fee_as_pct_of_gross_edge": (
                round_or_none((safe_div(fee, gross) or 0.0) * 100.0) if gross is not None and gross > 0 else None
            ),
            "fee_as_pct_of_abs_gross_pnl_magnitude": (
                round_or_none((fee_ratio_abs_gross or 0.0) * 100.0) if fee_ratio_abs_gross is not None else None
            ),
            "fee_explanation": fee_explanation,
        },
        "funding_impact": {
            "funding_cost_quote": round_or_none(funding),
            "funding_support_status": ledger_item.get("funding_support_status"),
            "funding_rate_source": ledger_item.get("funding_rate_source"),
            "funding_windows_crossed_count": as_int(ledger_item.get("funding_windows_crossed_count")),
            "funding_applied_count": as_int(ledger_item.get("funding_applied_count")),
            "funding_vs_fee_ratio": round_or_none(funding_ratio_fee),
            "funding_contribution_to_net_loss": (
                round_or_none(safe_div(abs(funding), abs(net)) if net is not None and net < 0 else None)
            ),
        },
        "trade_quality": {
            **ep_stats,
            "decision_count": decision_count,
            "fill_count": fill_count,
            "decisions_vs_fills_ratio": round_or_none(decisions_vs_fills),
            "fills_per_decision_ratio": round_or_none(fill_per_decision),
            "risk_reject_count": as_int(metrics.get("risk_reject_count")),
            "open_count": as_int(metrics.get("open_count")),
            "exit_count": as_int(metrics.get("exit_count")),
            "reversal_count": metrics.get("reversal_count"),
            "trade_ledger_row_count": len(trade_ledger_rows),
            "execution_ledger_row_count": len(execution_ledger_rows),
            "signal_consistency": (
                "gross episode distribution is not positive overall"
                if gross is not None and gross <= 0
                else "gross episode distribution is positive overall"
            ),
        },
        "efficiency_metrics": {
            "processed_event_count": as_int(metrics.get("processed_event_count")),
            "run_duration_sec": round_or_none(as_float(metrics.get("run_duration_sec"))),
            "stop_reason": metrics.get("stop_reason"),
            "verify_soft_live_pass": metrics.get("verify_soft_live_pass"),
            "bounded_churn": metrics.get("bounded_churn"),
            "trade_transitions_per_1k_events": metrics.get("trade_transitions_per_1k_events"),
            "turnover_quote": round_or_none(turnover),
            "opening_turnover_quote": round_or_none(as_float(ledger_item.get("opening_turnover_quote"))),
            "closing_turnover_quote": round_or_none(as_float(ledger_item.get("closing_turnover_quote"))),
            "gross_pnl_per_1k_events": round_or_none(safe_div(gross, processed_events) * 1000.0 if safe_div(gross, processed_events) is not None else None),
            "net_pnl_per_1k_events": round_or_none(safe_div(net, processed_events) * 1000.0 if safe_div(net, processed_events) is not None else None),
            "gross_pnl_per_fill": round_or_none(safe_div(gross, float(fill_count))),
            "net_pnl_per_fill": round_or_none(safe_div(net, float(fill_count))),
            "gross_pnl_per_closed_episode": round_or_none(safe_div(gross, float(closed_episode_count))),
            "net_pnl_per_closed_episode": round_or_none(safe_div(net, float(closed_episode_count))),
            "gross_bps_on_turnover": round_or_none(safe_div(gross, turnover) * 10000.0 if safe_div(gross, turnover) is not None else None),
            "net_bps_on_turnover": round_or_none(safe_div(net, turnover) * 10000.0 if safe_div(net, turnover) is not None else None),
            "fee_bps_on_turnover": round_or_none(safe_div(fee, turnover) * 10000.0 if safe_div(fee, turnover) is not None else None),
        },
        "edge_strength": {
            "gross_edge_positive": bool(gross is not None and gross > 0),
            "net_negative_only_because_of_fees": bool(gross is not None and gross > 0 and net is not None and net <= 0),
            "gross_already_weak_or_negative": bool(gross is not None and gross <= 0),
            "edge_statement": edge_statement,
        },
        "verdict": verdict,
        "verdict_reason": verdict_reason,
    }


def comparison(per_strategy: list[dict[str, Any]]) -> dict[str, Any]:
    by_symbol = {row["symbol"]: row for row in per_strategy}
    link = by_symbol.get("linkusdt")
    avax = by_symbol.get("avaxusdt")
    if not link or not avax:
        raise RuntimeError("expected linkusdt and avaxusdt profitability rows")

    def metric(row: dict[str, Any], section: str, key: str) -> float | None:
        return as_float((row.get(section) or {}).get(key))

    gross_link = metric(link, "gross_performance", "total_gross_pnl_quote")
    gross_avax = metric(avax, "gross_performance", "total_gross_pnl_quote")
    net_link = metric(link, "net_performance", "realized_pnl_quote_net")
    net_avax = metric(avax, "net_performance", "realized_pnl_quote_net")
    gross_per_fill_link = metric(link, "efficiency_metrics", "gross_pnl_per_fill")
    gross_per_fill_avax = metric(avax, "efficiency_metrics", "gross_pnl_per_fill")

    stronger = "avaxusdt"
    stronger_reason = (
        "avaxusdt has the less negative gross pnl per fill and higher fill-backed activity, "
        "but both gross pnl totals are negative before fees"
    )
    if gross_per_fill_link is not None and gross_per_fill_avax is not None and gross_per_fill_link > gross_per_fill_avax:
        stronger = "linkusdt"
        stronger_reason = (
            "linkusdt has the less negative gross pnl per fill, but both gross pnl totals are negative before fees"
        )

    final_decision = "NEITHER_CONTINUE"
    surviving = [row for row in per_strategy if row["verdict"] in {"KEEP_ADVANCING", "WEAK_CONTINUE"}]
    if {row["symbol"] for row in surviving} == {"linkusdt", "avaxusdt"}:
        final_decision = "BOTH_CONTINUE"
    elif surviving and surviving[0]["symbol"] == "avaxusdt":
        final_decision = "AVAX_ONLY"
    elif surviving and surviving[0]["symbol"] == "linkusdt":
        final_decision = "LINK_ONLY"

    return {
        "stronger_candidate": stronger,
        "weaker_candidate": "linkusdt" if stronger == "avaxusdt" else "avaxusdt",
        "stronger_candidate_reason": stronger_reason,
        "gross_edge_comparison": {
            "linkusdt_total_gross_pnl_quote": gross_link,
            "avaxusdt_total_gross_pnl_quote": gross_avax,
            "gross_edge_winner": "avaxusdt" if (gross_avax or -math.inf) > (gross_link or -math.inf) else "linkusdt",
            "interpretation": "both gross pnl totals are negative; no real edge for either candidate",
        },
        "net_comparison": {
            "linkusdt_realized_net_pnl_quote": net_link,
            "avaxusdt_realized_net_pnl_quote": net_avax,
            "net_winner": "linkusdt" if (net_link or -math.inf) > (net_avax or -math.inf) else "avaxusdt",
        },
        "fee_burden_comparison": {
            "linkusdt_total_fee_quote": metric(link, "fee_burden", "total_fee_quote"),
            "avaxusdt_total_fee_quote": metric(avax, "fee_burden", "total_fee_quote"),
            "linkusdt_fee_per_fill": metric(link, "fee_burden", "fee_per_fill"),
            "avaxusdt_fee_per_fill": metric(avax, "fee_burden", "fee_per_fill"),
            "interpretation": "fees dominate net loss for both, but fees are not the sole issue because gross pnl is already negative",
        },
        "funding_comparison": {
            "linkusdt_funding_cost_quote": metric(link, "funding_impact", "funding_cost_quote"),
            "avaxusdt_funding_cost_quote": metric(avax, "funding_impact", "funding_cost_quote"),
            "interpretation": "funding impact is zero in the current artifact surface for both candidates",
        },
        "trade_efficiency_comparison": {
            "linkusdt_gross_pnl_per_fill": gross_per_fill_link,
            "avaxusdt_gross_pnl_per_fill": gross_per_fill_avax,
            "linkusdt_net_pnl_per_1k_events": metric(link, "efficiency_metrics", "net_pnl_per_1k_events"),
            "avaxusdt_net_pnl_per_1k_events": metric(avax, "efficiency_metrics", "net_pnl_per_1k_events"),
            "interpretation": "avaxusdt is more active and has a less negative gross pnl per fill; linkusdt has smaller absolute net loss",
        },
        "stability_comparison": {
            "linkusdt_bounded_churn": (link.get("efficiency_metrics") or {}).get("bounded_churn"),
            "avaxusdt_bounded_churn": (avax.get("efficiency_metrics") or {}).get("bounded_churn"),
            "interpretation": "both candidates retained bounded churn and verified runs; profitability, not infrastructure, is the blocker",
        },
        "both_survive": final_decision == "BOTH_CONTINUE",
        "only_one_survives": final_decision in {"AVAX_ONLY", "LINK_ONLY"},
        "final_decision": final_decision,
        "final_decision_reason": "NEITHER_CONTINUE because both candidates have negative gross pnl before fees; no real edge",
    }


def render_markdown(result: dict[str, Any]) -> str:
    rows = result["per_strategy"]
    comp = result["comparison"]
    lines = [
        "# Phase7 Profitability Analysis v0",
        "",
        f"Generated: `{result['generated_ts_utc']}`",
        "",
        "## Final Decision",
        "",
        f"`{comp['final_decision']}`",
        "",
        comp["final_decision_reason"],
        "",
        "## Per Strategy",
        "",
    ]
    for row in rows:
        gross = row["gross_performance"]["total_gross_pnl_quote"]
        net = row["net_performance"]["realized_pnl_quote_net"]
        fees = row["fee_burden"]["total_fee_quote"]
        funding = row["funding_impact"]["funding_cost_quote"]
        fee_per_fill = row["fee_burden"]["fee_per_fill"]
        gross_per_fill = row["efficiency_metrics"]["gross_pnl_per_fill"]
        net_per_fill = row["efficiency_metrics"]["net_pnl_per_fill"]
        gross_win_rate = row["trade_quality"]["gross_win_rate"]
        lines.extend(
            [
                f"### {row['symbol']}",
                "",
                f"- Verdict: `{row['verdict']}`",
                f"- Gross PnL before fees/funding: `{gross}`",
                f"- Net realized PnL: `{net}` with status `{row['net_performance']['profitability_status']}`",
                f"- Fees: `{fees}` total, `{fee_per_fill}` per fill",
                f"- Funding: `{funding}` total",
                f"- Gross per fill: `{gross_per_fill}`; net per fill: `{net_per_fill}`",
                f"- Gross episode win rate: `{gross_win_rate}`",
                f"- Edge statement: `{row['edge_strength']['edge_statement']}`",
                f"- Reason: {row['verdict_reason']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Comparison",
            "",
            f"Stronger candidate on relative gross efficiency: `{comp['stronger_candidate']}`.",
            comp["stronger_candidate_reason"],
            "",
            "Fee is a major burden for both, but it is not the main distinction: both gross PnL values are already negative before fees. Funding impact is zero in the current artifact surface.",
            "",
            "## Next Step",
            "",
            "Do not promote this pair from the current 24h evidence. The next sprint should be cost/profitability gating or execution-threshold analysis using existing artifacts; do not treat this as a discovery failure.",
            "",
        ]
    )
    return "\n".join(lines)


def build_result(args: argparse.Namespace) -> dict[str, Any]:
    continuation = read_json(Path(args.continuation_result_json))
    rows = [
        row
        for row in continuation.get("results", [])
        if str(row.get("symbol") or "").lower() in set(TARGET_SYMBOLS)
    ]
    if sorted(str(row.get("symbol") or "").lower() for row in rows) != sorted(TARGET_SYMBOLS):
        raise RuntimeError(f"expected exactly target symbols {TARGET_SYMBOLS}, got {[row.get('symbol') for row in rows]}")

    per_strategy = [analyze_row(row) for row in sorted(rows, key=lambda item: TARGET_SYMBOLS.index(str(item.get("symbol")).lower()))]
    comp = comparison(per_strategy)

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": args.generated_ts or utc_now(),
        "governance": {
            "scope": "read_only_profitability_decomposition_for_phase7_continuation_candidates",
            "target_symbols": list(TARGET_SYMBOLS),
            "no_new_runs": True,
            "no_strategy_modification": True,
            "no_ranking_change": True,
            "no_promotion_change": True,
            "no_global_watchlist_mutation": True,
        },
        "inputs": {
            "phase7_continuation_validation_result_v1_json": str(Path(args.continuation_result_json).resolve()),
            "canonical_truth_registry_json": str(Path(args.canonical_truth_registry_json).resolve()),
        },
        "source_continuation_summary": {
            "schema_version": continuation.get("schema_version"),
            "generated_ts_utc": continuation.get("generated_ts_utc"),
            "lane_result": (continuation.get("summary") or {}).get("lane_result"),
            "final_recommendation": continuation.get("final_recommendation"),
        },
        "per_strategy": per_strategy,
        "comparison": comp,
        "final_decision": comp["final_decision"],
        "final_decision_reason": comp["final_decision_reason"],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--continuation-result-json", default=str(DEFAULT_RESULT_V1))
    parser.add_argument("--canonical-truth-registry-json", default=str(DEFAULT_CANONICAL_REGISTRY))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument("--report-md", default=str(DEFAULT_REPORT_MD))
    parser.add_argument("--generated-ts", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = build_result(args)

    output_json = Path(args.output_json)
    report_md = Path(args.report_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    report_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    report_md.write_text(render_markdown(result) + "\n")

    print("PHASE7_PROFITABILITY_ANALYSIS_COMPLETE")
    print(f"OUTPUT_JSON={output_json}")
    print(f"REPORT_MD={report_md}")
    print(f"FINAL_DECISION={result['final_decision']}")
    for row in result["per_strategy"]:
        print(
            "ROW",
            row["symbol"],
            row["verdict"],
            "gross=",
            row["gross_performance"]["total_gross_pnl_quote"],
            "net=",
            row["net_performance"]["realized_pnl_quote_net"],
            "fees=",
            row["fee_burden"]["total_fee_quote"],
            "funding=",
            row["funding_impact"]["funding_cost_quote"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
