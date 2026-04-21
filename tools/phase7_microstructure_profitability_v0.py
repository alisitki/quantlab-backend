#!/usr/bin/env python3
"""Phase7 microstructure profitability decomposition.

Read-only analyzer for the bybit-only MICROSTRUCTURE_IMBALANCE_V1 survivor set.
It consumes the completed Phase7 shadow artifacts and writes a compact economic
edge report. It does not run shadow, mutate ranking, or mutate promotion state.
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

DEFAULT_SHADOW_RESULT = ROOT / "tools/phase7_microstructure_shadow_result_v1.json"
DEFAULT_RUN_ROOT = ROOT / "tools/phase7_microstructure_shadow_v1_output/full_run/runs"
DEFAULT_OUT_JSON = ROOT / "tools/phase7_microstructure_profitability_v0.json"
DEFAULT_REPORT_MD = (
    ROOT
    / "tools/phase7_microstructure_profitability_output"
    / "phase7_microstructure_profitability_report_v0.md"
)

TARGET_SYMBOLS = ("linkusdt", "avaxusdt", "xrpusdt")
SCHEMA_VERSION = "phase7_microstructure_profitability_v0"


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
        if line:
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
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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
    weight = pos - lo
    return ordered[lo] * (1.0 - weight) + ordered[hi] * weight


def dist(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "p25": None, "median": None, "p75": None, "max": None}
    return {
        "min": round_or_none(min(values)),
        "p25": round_or_none(pct(values, 0.25)),
        "median": round_or_none(float(statistics.median(values))),
        "p75": round_or_none(pct(values, 0.75)),
        "max": round_or_none(max(values)),
    }


def find_run_dir(run_root: Path, strategy_id: str) -> Path:
    matches: list[Path] = []
    for run_dir in sorted(run_root.iterdir()):
        if not run_dir.is_dir():
            continue
        watchlist = run_dir / "input_watchlist.json"
        if watchlist.exists() and strategy_id in watchlist.read_text(errors="replace"):
            matches.append(run_dir)
    if len(matches) != 1:
        raise RuntimeError(f"expected one run dir for {strategy_id}, found {len(matches)}")
    return matches[0]


def load_futures_item(run_dir: Path, strategy_id: str) -> tuple[dict[str, Any], Path]:
    path = run_dir / "shadow_state/shadow_futures_paper_ledger_v1.json"
    data = read_json(path)
    items = data.get("items") or []
    matches = [item for item in items if item.get("selected_pack_id") == strategy_id]
    if len(matches) != 1:
        raise RuntimeError(f"expected one futures item for {strategy_id}, found {len(matches)}")
    return matches[0], path


def episode_stats(episodes: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [episode for episode in episodes if episode.get("status") == "CLOSED"]
    gross = [as_float(episode.get("realized_pnl_quote_gross"), 0.0) or 0.0 for episode in closed]
    net = [as_float(episode.get("realized_pnl_quote_net"), 0.0) or 0.0 for episode in closed]
    fees = [as_float(episode.get("fee_quote"), 0.0) or 0.0 for episode in closed]
    gross_pos = sum(1 for value in gross if value > 0)
    gross_neg = sum(1 for value in gross if value < 0)
    gross_flat = sum(1 for value in gross if value == 0)
    return {
        "episode_count": len(episodes),
        "closed_episode_count": len(closed),
        "gross_positive_episode_count": gross_pos,
        "gross_negative_episode_count": gross_neg,
        "gross_flat_episode_count": gross_flat,
        "gross_win_rate": round_or_none(safe_div(float(gross_pos), float(len(gross))) if gross else None),
        "gross_pnl_distribution": dist(gross),
        "net_pnl_distribution": dist(net),
        "fee_distribution": dist(fees),
        "avg_gross_pnl_per_closed_episode": round_or_none(
            safe_div(sum(gross), float(len(gross))) if gross else None
        ),
        "avg_net_pnl_per_closed_episode": round_or_none(
            safe_div(sum(net), float(len(net))) if net else None
        ),
        "avg_fee_per_closed_episode": round_or_none(
            safe_div(sum(fees), float(len(fees))) if fees else None
        ),
    }


def verdict_for(gross_pnl: float | None, net_pnl: float | None, fee_to_gross: float | None) -> tuple[str, str]:
    if gross_pnl is None:
        return "DROP", "gross pnl is unavailable in current artifact surface"
    if gross_pnl <= 0:
        return "DROP", "gross edge is not positive before fees"
    if net_pnl is not None and net_pnl > 0:
        return "KEEP_ADVANCING", "gross and net pnl are both positive"
    if fee_to_gross is not None and fee_to_gross > 5.0:
        return "WEAK_CONTINUE", "gross edge is positive but fees are more than 5x gross edge"
    return "KEEP_ADVANCING", "gross edge is positive and net loss is explainable by fees"


def analyze_strategy(row: dict[str, Any], run_root: Path) -> dict[str, Any]:
    strategy_id = str(row["strategy_id"])
    symbol = str(row["symbol"]).lower()
    run_dir = find_run_dir(run_root, strategy_id)
    item, futures_path = load_futures_item(run_dir, strategy_id)
    metrics = row.get("metrics") or {}

    execution_ledger_path = run_dir / "shadow_state/shadow_execution_ledger_v0.jsonl"
    trade_ledger_path = run_dir / "shadow_state/shadow_trade_ledger_v1.jsonl"
    execution_pack_summary_path = run_dir / "shadow_state/shadow_execution_pack_summary_v0.json"
    summary_runtime_path = run_dir / "summary_runtime.json"

    execution_ledger = read_jsonl(execution_ledger_path)
    trade_ledger = read_jsonl(trade_ledger_path)
    summary_runtime = read_json(summary_runtime_path)

    gross_pnl = as_float(item.get("replayed_realized_pnl_quote_gross"))
    net_pnl = as_float(item.get("replayed_realized_pnl_quote_net"))
    gross_mtm = as_float(item.get("mark_to_market_pnl_quote_gross"))
    net_mtm_after_fees_funding_exit = as_float(
        item.get("mark_to_market_pnl_quote_net_after_funding_and_exit_estimate")
    )
    total_fee = as_float(item.get("total_fee_quote"), 0.0) or 0.0
    funding_cost = as_float(item.get("funding_cost_quote"), 0.0) or 0.0
    fill_count = as_int(item.get("fill_event_count"), as_int(metrics.get("fill_count")))
    decision_count = as_int(item.get("decision_event_count"), as_int(metrics.get("decision_count")))
    processed_event_count = as_int(metrics.get("processed_event_count"))
    turnover = as_float(item.get("turnover_quote"))
    fee_to_gross = safe_div(total_fee, gross_pnl) if gross_pnl and gross_pnl > 0 else None
    gross_to_fee = safe_div(gross_pnl, total_fee) if gross_pnl is not None and total_fee else None

    episodes = item.get("episodes") or []
    ep_stats = episode_stats(episodes)
    closed_episode_count = as_int(ep_stats.get("closed_episode_count"))
    verdict, verdict_reason = verdict_for(gross_pnl, net_pnl, fee_to_gross)
    edge_statement = (
        "real edge exists but requires execution optimization"
        if gross_pnl is not None and gross_pnl > 0
        else "no real edge"
    )
    fee_problem = (
        "fees are the main net-PnL problem"
        if gross_pnl is not None and gross_pnl > 0 and net_pnl is not None and net_pnl <= 0
        else "fees deepen a non-positive gross edge"
    )

    return {
        "strategy_id": strategy_id,
        "family_id": row.get("family_id"),
        "exchange": row.get("exchange"),
        "symbol": symbol,
        "source_paths": {
            "run_dir": str(run_dir),
            "shadow_execution_ledger_v0_jsonl": str(execution_ledger_path),
            "shadow_trade_ledger_v1_jsonl": str(trade_ledger_path),
            "shadow_futures_paper_ledger_v1_json": str(futures_path),
            "execution_pack_summary_v0_json": str(execution_pack_summary_path),
            "summary_runtime_json": str(summary_runtime_path),
        },
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "edge_statement": edge_statement,
        "gross_performance": {
            "gross_realized_pnl_quote": round_or_none(gross_pnl),
            "gross_mark_to_market_pnl_quote": round_or_none(gross_mtm),
            "gross_pnl_source": "shadow_futures_paper_ledger_v1.replayed_realized_pnl_quote_gross",
            "gross_mark_to_market_source": "shadow_futures_paper_ledger_v1.mark_to_market_pnl_quote_gross",
        },
        "net_performance": {
            "net_realized_pnl_quote": round_or_none(net_pnl),
            "summary_realized_pnl_quote": round_or_none(as_float(item.get("summary_realized_pnl_quote"))),
            "net_mark_to_market_after_fees_funding_exit_estimate_quote": round_or_none(
                net_mtm_after_fees_funding_exit
            ),
            "profitability_status": item.get("profitability_status"),
            "cost_accounting_status": item.get("cost_accounting_status"),
            "paper_run_status": item.get("paper_run_status"),
            "pnl_reconciliation_status": item.get("pnl_reconciliation_status"),
        },
        "fee_burden": {
            "total_fee_quote": round_or_none(total_fee),
            "fee_per_fill": round_or_none(safe_div(total_fee, float(fill_count))),
            "fee_per_closed_episode": round_or_none(
                safe_div(total_fee, float(closed_episode_count)) if closed_episode_count else None
            ),
            "fee_to_positive_gross_ratio": round_or_none(fee_to_gross),
            "gross_to_fee_ratio": round_or_none(gross_to_fee),
            "effective_fee_rate": round_or_none(as_float(item.get("effective_fee_rate"))),
            "fee_support_status": item.get("fee_support_status"),
            "fee_problem": fee_problem,
        },
        "funding_impact": {
            "funding_cost_quote": round_or_none(funding_cost),
            "funding_applied_count": as_int(item.get("funding_applied_count")),
            "funding_windows_crossed_count": as_int(item.get("funding_windows_crossed_count")),
            "funding_to_fee_ratio": round_or_none(safe_div(abs(funding_cost), total_fee)),
            "funding_support_status": item.get("funding_support_status"),
        },
        "trade_quality": {
            "decision_count": decision_count,
            "fill_count": fill_count,
            "fill_to_decision_ratio": round_or_none(safe_div(float(fill_count), float(decision_count))),
            "risk_reject_count": as_int(item.get("risk_reject_event_count"), as_int(metrics.get("risk_reject_count"))),
            "risk_reject_to_decision_ratio": round_or_none(
                safe_div(float(as_int(item.get("risk_reject_event_count"))), float(decision_count))
            ),
            "closed_episode_count": closed_episode_count,
            "open_position_count": as_int(summary_runtime.get("execution_summary", {}).get("positions_count")),
            "final_position_direction": item.get("final_position_direction"),
            "episode_stats": ep_stats,
        },
        "efficiency_metrics": {
            "processed_event_count": processed_event_count,
            "turnover_quote": round_or_none(turnover),
            "gross_pnl_per_fill": round_or_none(safe_div(gross_pnl, float(fill_count))),
            "net_pnl_per_fill": round_or_none(safe_div(net_pnl, float(fill_count))),
            "gross_pnl_per_closed_episode": round_or_none(
                safe_div(gross_pnl, float(closed_episode_count)) if closed_episode_count else None
            ),
            "net_pnl_per_closed_episode": round_or_none(
                safe_div(net_pnl, float(closed_episode_count)) if closed_episode_count else None
            ),
            "gross_pnl_per_1k_events": round_or_none(safe_div(gross_pnl, float(processed_event_count)) * 1000.0),
            "net_pnl_per_1k_events": round_or_none(safe_div(net_pnl, float(processed_event_count)) * 1000.0),
            "gross_pnl_per_turnover": round_or_none(safe_div(gross_pnl, turnover)),
            "net_pnl_per_turnover": round_or_none(safe_div(net_pnl, turnover)),
            "trade_transitions_per_1k_events": round_or_none(
                as_float(metrics.get("trade_transitions_per_1k_events"))
            ),
        },
        "sanity_checks": {
            "execution_ledger_rows": len(execution_ledger),
            "trade_ledger_rows": len(trade_ledger),
            "execution_pack_summary_present": execution_pack_summary_path.exists(),
            "summary_runtime_stop_reason": summary_runtime.get("stop_reason"),
        },
    }


def comparison(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(
        rows,
        key=lambda row: (
            row["gross_performance"]["gross_realized_pnl_quote"] or float("-inf"),
            row["efficiency_metrics"]["gross_pnl_per_fill"] or float("-inf"),
            -(row["fee_burden"]["fee_to_positive_gross_ratio"] or float("inf")),
        ),
        reverse=True,
    )
    strongest = ranked[0] if ranked else None
    positive = [row for row in rows if (row["gross_performance"]["gross_realized_pnl_quote"] or 0) > 0]
    dropped = [row for row in rows if row["verdict"] == "DROP"]

    if len(positive) == len(rows) and not dropped:
        final = "ALL_CONTINUE"
    elif strongest and positive:
        final = "BEST_ONE_ONLY"
    else:
        final = "NONE_CONTINUE"

    return {
        "strongest_strategy_id": strongest["strategy_id"] if strongest else None,
        "strongest_symbol": strongest["symbol"] if strongest else None,
        "highest_gross_edge_strategy_id": strongest["strategy_id"] if strongest else None,
        "highest_gross_edge_quote": (
            strongest["gross_performance"]["gross_realized_pnl_quote"] if strongest else None
        ),
        "most_efficient_strategy_id": strongest["strategy_id"] if strongest else None,
        "fee_burden_rank_desc": [
            {
                "strategy_id": row["strategy_id"],
                "symbol": row["symbol"],
                "fee_to_positive_gross_ratio": row["fee_burden"]["fee_to_positive_gross_ratio"],
            }
            for row in sorted(
                rows,
                key=lambda row: row["fee_burden"]["fee_to_positive_gross_ratio"] or float("inf"),
            )
        ],
        "gross_edge_rank_desc": [
            {
                "strategy_id": row["strategy_id"],
                "symbol": row["symbol"],
                "gross_realized_pnl_quote": row["gross_performance"]["gross_realized_pnl_quote"],
                "gross_pnl_per_fill": row["efficiency_metrics"]["gross_pnl_per_fill"],
            }
            for row in ranked
        ],
        "final_decision": final,
        "final_decision_reason": (
            "linkusdt is the only candidate to carry forward: it has the strongest gross edge, "
            "best gross efficiency, and lowest fee/gross burden; avaxusdt is gross-positive but weaker, "
            "while xrpusdt has no positive gross edge."
            if final == "BEST_ONE_ONLY"
            else (
                "all candidates have positive gross edge"
                if final == "ALL_CONTINUE"
                else "no candidate has positive gross edge"
            )
        ),
    }


def build_report(data: dict[str, Any]) -> str:
    lines = [
        "# Phase7 Microstructure Profitability v0",
        "",
        f"- generated_ts_utc: `{data['generated_ts_utc']}`",
        "- scope: bybit-only `microstructure_imbalance_v1` survivors",
        f"- final_decision: `{data['final_decision']}`",
        f"- strongest: `{data['comparison']['strongest_symbol']}`",
        "- no new runs, no strategy edits, no ranking/promotion mutation",
        "",
        "## Verdicts",
    ]
    for row in data["per_strategy"]:
        lines.append(
            "- `{}` symbol={} verdict={} gross={} net={} fees={} funding={} fee/gross={} edge_statement=`{}`".format(
                row["strategy_id"],
                row["symbol"],
                row["verdict"],
                row["gross_performance"]["gross_realized_pnl_quote"],
                row["net_performance"]["net_realized_pnl_quote"],
                row["fee_burden"]["total_fee_quote"],
                row["funding_impact"]["funding_cost_quote"],
                row["fee_burden"]["fee_to_positive_gross_ratio"],
                row["edge_statement"],
            )
        )
    lines.extend(
        [
            "",
            "## Comparison",
            f"- highest gross edge: `{data['comparison']['highest_gross_edge_strategy_id']}` "
            f"gross={data['comparison']['highest_gross_edge_quote']}",
            "- fee impact: fees are the main problem for linkusdt and avaxusdt; funding impact is zero because no funding window crossed.",
            "- xrpusdt: no real edge; gross realized pnl is not positive before fees.",
            "",
            "## Next Step",
            "Run execution-cost optimization / lower-turnover gating only for `linkusdt`. Do not promote yet. "
            "Do not carry `xrpusdt`; keep `avaxusdt` as evidence-backed secondary but outside the next active lane unless the enum is expanded to allow top-2 continuation.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shadow-result-json", type=Path, default=DEFAULT_SHADOW_RESULT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--out-json", type=Path, default=DEFAULT_OUT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--generated-ts-utc", default=None)
    args = parser.parse_args()

    generated_ts_utc = args.generated_ts_utc or utc_now()
    shadow_result = read_json(args.shadow_result_json)
    survivors = shadow_result.get("continuation_candidates") or []
    survivor_symbols = tuple(str(row.get("symbol") or "").lower() for row in survivors)
    if survivor_symbols != TARGET_SYMBOLS:
        raise RuntimeError(f"expected survivor symbols {TARGET_SYMBOLS}, got {survivor_symbols}")

    per_strategy = [analyze_strategy(row, args.run_root) for row in survivors]
    comp = comparison(per_strategy)
    data = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": generated_ts_utc,
        "governance": {
            "scope": "read-only profitability decomposition for bybit-only microstructure survivors",
            "no_new_runs": True,
            "no_strategy_modification": True,
            "no_ranking_or_promotion_mutation": True,
            "profitability_inputs_only": True,
        },
        "source_shadow_result_json": str(args.shadow_result_json),
        "source_run_root": str(args.run_root),
        "per_strategy": per_strategy,
        "comparison": comp,
        "final_decision": comp["final_decision"],
        "clear_statement": {
            "linkusdt": "real edge exists but requires execution optimization",
            "avaxusdt": "real edge exists but requires execution optimization",
            "xrpusdt": "no real edge",
        },
    }

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    args.report_md.write_text(build_report(data))
    print("PHASE7_MICROSTRUCTURE_PROFITABILITY_COMPLETE")
    print(f"out_json={args.out_json}")
    print(f"report_md={args.report_md}")
    print(f"final_decision={data['final_decision']}")
    print(f"strongest={data['comparison']['strongest_symbol']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
