#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "tradability_triage_report_v0"
SHORTLIST_SCHEMA_VERSION = "shadow_shortlist_v0"
TRADABLE_DIRECTIONAL = "TRADABLE_DIRECTIONAL"
CONTEXT_GUARD = "CONTEXT_GUARD"
RESEARCH_ONLY = "RESEARCH_ONLY"
STATUS_OK = "OK"
STATUS_DISCOVERY_BIAS = "DISCOVERY_BIAS"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
PAPER_DIRECTIONAL_V1 = "PAPER_DIRECTIONAL_V1"
DIRECTIONAL = "DIRECTIONAL"


class TradabilityTriageError(RuntimeError):
    pass


@dataclass(frozen=True)
class Thresholds:
    min_abs_t_stat: float = 10.0
    min_event_count: int = 40000


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase6 tradability triage for newly bound non-momentum strategies")
    p.add_argument(
        "--canonical-truth-registry",
        default="tools/system_state/canonical_truth_registry_v0.json",
        help="Canonical truth registry",
    )
    p.add_argument(
        "--contract-json",
        default="tools/phase6_state/candidate_strategy_contract_v0.json",
        help="Authoritative candidate strategy contract artifact",
    )
    p.add_argument(
        "--binding-json",
        default="tools/phase6_state/candidate_strategy_runtime_binding_v0.json",
        help="Authoritative candidate strategy runtime binding artifact",
    )
    p.add_argument(
        "--family-role-json",
        default="tools/phase6_state/hypothesis_family_role_classification_v0.json",
        help="Derived family role classification surface",
    )
    p.add_argument(
        "--before-binding-json",
        default="tools/phase6_bridge_output/candidate_strategy_runtime_binding_v0.before.json",
        help="Historical runtime binding snapshot used only to detect newly bound rows",
    )
    p.add_argument(
        "--shadow-watchlist-json",
        default="tools/shadow_state/shadow_watchlist_v0.json",
        help="Authoritative active shadow subset",
    )
    p.add_argument(
        "--out-report",
        default="tools/phase6_state/tradability_triage_report_v0.json",
        help="Output classification report",
    )
    p.add_argument(
        "--out-shortlist",
        default="tools/phase6_state/shadow_shortlist_v0.json",
        help="Output shortlist artifact",
    )
    p.add_argument(
        "--max-shortlist",
        type=int,
        default=10,
        help="Maximum shortlist size",
    )
    return p.parse_args(argv)


def read_json(path: Path) -> Any:
    if not path.exists():
        raise TradabilityTriageError(f"missing required json: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def require_authoritative_path(
    registry: dict[str, Any],
    concept_name: str,
    expected_path: str,
) -> dict[str, Any]:
    for item in registry.get("concepts", []):
        if item.get("concept") != concept_name:
            continue
        authoritative_now = item.get("authoritative_now", [])
        for entry in authoritative_now:
            if entry.get("path") == expected_path:
                return item
        raise TradabilityTriageError(
            f"canonical registry concept {concept_name!r} does not point at expected path {expected_path}"
        )
    raise TradabilityTriageError(f"canonical registry missing concept: {concept_name}")


def build_contract_maps(contract_items: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_contract_row_id: dict[str, dict[str, Any]] = {}
    by_strategy_id: dict[str, dict[str, Any]] = {}
    for row in contract_items:
        contract_row_id = row.get("contract_row_id")
        if isinstance(contract_row_id, str):
            by_contract_row_id[contract_row_id] = row
        strategy_spec = row.get("strategy_spec")
        strategy_spec = strategy_spec if isinstance(strategy_spec, dict) else {}
        strategy_id = strategy_spec.get("strategy_id")
        if isinstance(strategy_id, str):
            by_strategy_id[strategy_id] = row
    return by_contract_row_id, by_strategy_id


def build_family_role_map(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in items:
        family_id = row.get("family_id")
        if isinstance(family_id, str):
            out[family_id] = row
    return out


def build_watchlist_strategy_ids(items: list[dict[str, Any]]) -> set[str]:
    strategy_ids: set[str] = set()
    for row in items:
        strategy_id = row.get("binding_strategy_id")
        if isinstance(strategy_id, str):
            strategy_ids.add(strategy_id)
    return strategy_ids


def normalize_selected_cell(cell: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(cell, dict):
        return {}
    keys = ("date", "delta_ms", "event_count", "exchange", "h_ms", "mean_product", "stream", "symbol", "t_stat")
    return {key: cell.get(key) for key in keys if key in cell}


def conservative_reason(reason_code: str, details: list[str]) -> dict[str, Any]:
    return {"reason_code": reason_code, "details": details}


def classify_row(
    binding_row: dict[str, Any],
    contract_row: dict[str, Any],
    family_role_row: dict[str, Any] | None,
    thresholds: Thresholds,
) -> tuple[str, dict[str, Any]]:
    family_id = binding_row.get("family_id")
    role = (family_role_row or {}).get("role", "UNKNOWN")
    role_rationale = (family_role_row or {}).get("rationale", "")
    strategy_spec = contract_row.get("strategy_spec")
    strategy_spec = strategy_spec if isinstance(strategy_spec, dict) else {}
    strategy_params = strategy_spec.get("strategy_params")
    strategy_params = strategy_params if isinstance(strategy_params, dict) else {}
    selected_cell = strategy_params.get("selected_cell", {})
    selected_cell = selected_cell if isinstance(selected_cell, dict) else {}
    binding_mode = binding_row.get("binding_mode")
    tradeability_class = binding_row.get("shadow_tradeability_class")
    stream = binding_row.get("stream")
    symbols = binding_row.get("symbols") or []
    mean_product = float(selected_cell.get("mean_product", 0.0) or 0.0)
    t_stat = float(selected_cell.get("t_stat", 0.0) or 0.0)
    event_count = int(selected_cell.get("event_count", 0) or 0)
    details = []
    if role_rationale:
        details.append(role_rationale)

    if role == CONTEXT_GUARD:
        details.append("Family role is support/context only in Phase6 governance.")
        return CONTEXT_GUARD, conservative_reason("FAMILY_ROLE_CONTEXT_GUARD", details)

    if family_id != "return_reversal_v1":
        details.append("No narrow directional triage rule exists for this non-momentum family.")
        return RESEARCH_ONLY, conservative_reason("NO_DIRECTIONAL_TRIAGE_RULE", details)

    if binding_row.get("runtime_binding_status") != BOUND_SHADOW_RUNNABLE:
        details.append("Runtime binding is not shadow runnable.")
        return RESEARCH_ONLY, conservative_reason("NOT_BOUND_SHADOW_RUNNABLE", details)

    if binding_mode != PAPER_DIRECTIONAL_V1 or tradeability_class != DIRECTIONAL:
        details.append("Runtime binding is not directional paper mode.")
        return RESEARCH_ONLY, conservative_reason("NON_DIRECTIONAL_RUNTIME_MODE", details)

    if stream != "trade":
        details.append("Only single-symbol trade-stream rows are allowed for directional shortlist consideration.")
        return RESEARCH_ONLY, conservative_reason("NON_TRADE_STREAM", details)

    if len(symbols) != 1:
        details.append("Only single-symbol rows are allowed for directional shortlist consideration.")
        return RESEARCH_ONLY, conservative_reason("NON_SINGLE_SYMBOL", details)

    if not selected_cell:
        details.append("Selected cell is missing from strategy params.")
        return RESEARCH_ONLY, conservative_reason("MISSING_SELECTED_CELL", details)

    if mean_product >= 0.0 or t_stat >= 0.0:
        details.append("Return-reversal cell does not preserve signed reversion semantics.")
        return RESEARCH_ONLY, conservative_reason("REVERSAL_SIGN_MISMATCH", details)

    if abs(t_stat) < thresholds.min_abs_t_stat:
        details.append(f"abs(t_stat)={abs(t_stat):.6f} is below conservative threshold {thresholds.min_abs_t_stat:.1f}.")
        return RESEARCH_ONLY, conservative_reason("WEAK_T_STAT", details)

    if event_count < thresholds.min_event_count:
        details.append(f"event_count={event_count} is below conservative threshold {thresholds.min_event_count}.")
        return RESEARCH_ONLY, conservative_reason("LOW_EVENT_COUNT", details)

    details.append(
        "Single-symbol trade-stream return-reversal row is directional, shadow runnable, and passes conservative cell thresholds."
    )
    return TRADABLE_DIRECTIONAL, conservative_reason("DIRECTIONAL_RUNTIME_AND_STRONG_REVERSAL_CELL", details)


def shortlist_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(row.get("rank", 10**9) or 10**9),
        row.get("family_id", ""),
        row.get("exchange", ""),
        row.get("stream", ""),
        row.get("selected_symbol", ""),
        row.get("strategy_id", ""),
    )


def build_report_row(
    binding_row: dict[str, Any],
    contract_row: dict[str, Any],
    previous_status: str | None,
    active_watchlist_ids: set[str],
    family_role_row: dict[str, Any] | None,
    classification: str,
    reasoning: dict[str, Any],
) -> dict[str, Any]:
    strategy_spec = contract_row.get("strategy_spec")
    strategy_spec = strategy_spec if isinstance(strategy_spec, dict) else {}
    strategy_params = strategy_spec.get("strategy_params")
    strategy_params = strategy_params if isinstance(strategy_params, dict) else {}
    selected_cell = strategy_params.get("selected_cell", {})
    selected_cell = normalize_selected_cell(selected_cell)
    return {
        "strategy_id": binding_row.get("strategy_id"),
        "contract_row_id": binding_row.get("contract_row_id"),
        "family_id": binding_row.get("family_id"),
        "family_role": (family_role_row or {}).get("role", "UNKNOWN"),
        "selected_symbol": binding_row.get("selected_symbol"),
        "exchange": binding_row.get("exchange"),
        "stream": binding_row.get("stream"),
        "symbols": binding_row.get("symbols"),
        "rank": binding_row.get("rank"),
        "decision_tier": binding_row.get("decision_tier"),
        "runtime_binding_status": binding_row.get("runtime_binding_status"),
        "binding_mode": binding_row.get("binding_mode"),
        "shadow_tradeability_class": binding_row.get("shadow_tradeability_class"),
        "translation_status": binding_row.get("translation_status"),
        "pack_id": binding_row.get("pack_id"),
        "pack_path": binding_row.get("pack_path"),
        "previous_runtime_binding_status": previous_status,
        "newly_bound_shadow_runnable": True,
        "currently_in_active_shadow_subset": binding_row.get("strategy_id") in active_watchlist_ids,
        "selected_cell": selected_cell,
        "classification": classification,
        "reasoning": reasoning,
    }


def build_shortlist(report_rows: list[dict[str, Any]], max_shortlist: int) -> list[dict[str, Any]]:
    tradable_rows = [row for row in report_rows if row.get("classification") == TRADABLE_DIRECTIONAL]
    tradable_rows.sort(key=shortlist_sort_key)
    deduped: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for row in tradable_rows:
        dedupe_key = (
            row.get("family_id"),
            row.get("exchange"),
            row.get("stream"),
            row.get("selected_symbol"),
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        shortlist_reason = (
            "Best-ranked newly bound directional instance for "
            f"{row.get('family_id')}::{row.get('selected_symbol')} after per-symbol dedupe."
        )
        deduped.append(
            {
                "strategy_id": row.get("strategy_id"),
                "contract_row_id": row.get("contract_row_id"),
                "family_id": row.get("family_id"),
                "selected_symbol": row.get("selected_symbol"),
                "exchange": row.get("exchange"),
                "stream": row.get("stream"),
                "rank": row.get("rank"),
                "binding_mode": row.get("binding_mode"),
                "shadow_tradeability_class": row.get("shadow_tradeability_class"),
                "selected_cell": row.get("selected_cell"),
                "reasoning": row.get("reasoning"),
                "shortlist_reason": shortlist_reason,
            }
        )
        if len(deduped) >= max_shortlist:
            break
    return deduped


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    thresholds = Thresholds()

    canonical_truth_registry_path = Path(args.canonical_truth_registry)
    contract_path = Path(args.contract_json)
    binding_path = Path(args.binding_json)
    family_role_path = Path(args.family_role_json)
    before_binding_path = Path(args.before_binding_json)
    shadow_watchlist_path = Path(args.shadow_watchlist_json)
    out_report_path = Path(args.out_report)
    out_shortlist_path = Path(args.out_shortlist)

    registry = read_json(canonical_truth_registry_path)
    contract_truth = require_authoritative_path(registry, "candidate_strategy_contract", args.contract_json)
    binding_truth = require_authoritative_path(registry, "runtime_binding", args.binding_json)
    shadow_truth = require_authoritative_path(registry, "active_shadow_subset", args.shadow_watchlist_json)
    family_role_truth = require_authoritative_path(registry, "family_role_classification", args.family_role_json)

    contract = read_json(contract_path)
    binding = read_json(binding_path)
    family_role = read_json(family_role_path)
    before_binding = read_json(before_binding_path)
    shadow_watchlist = read_json(shadow_watchlist_path)

    contract_items = contract.get("items", [])
    binding_items = binding.get("items", [])
    family_role_items = family_role.get("items", [])
    before_binding_items = before_binding.get("items", [])
    shadow_watchlist_items = shadow_watchlist.get("items", [])

    if not isinstance(contract_items, list) or not isinstance(binding_items, list):
        raise TradabilityTriageError("contract/binding artifacts do not have expected list items")

    contract_by_contract_row_id, contract_by_strategy_id = build_contract_maps(contract_items)
    family_role_by_id = build_family_role_map(family_role_items if isinstance(family_role_items, list) else [])
    active_watchlist_ids = build_watchlist_strategy_ids(shadow_watchlist_items if isinstance(shadow_watchlist_items, list) else [])
    previous_status_by_strategy_id = {
        row.get("strategy_id"): row.get("runtime_binding_status")
        for row in before_binding_items
        if isinstance(row, dict) and isinstance(row.get("strategy_id"), str)
    }

    report_rows: list[dict[str, Any]] = []
    for binding_row in binding_items:
        if not isinstance(binding_row, dict):
            continue
        family_id = binding_row.get("family_id")
        if family_id == "momentum_v1":
            continue
        if binding_row.get("runtime_binding_status") != BOUND_SHADOW_RUNNABLE:
            continue
        strategy_id = binding_row.get("strategy_id")
        previous_status = previous_status_by_strategy_id.get(strategy_id)
        if previous_status == BOUND_SHADOW_RUNNABLE:
            continue
        contract_row = contract_by_contract_row_id.get(binding_row.get("contract_row_id"))
        if contract_row is None and isinstance(strategy_id, str):
            contract_row = contract_by_strategy_id.get(strategy_id)
        if contract_row is None:
            raise TradabilityTriageError(f"missing contract row for strategy_id={strategy_id!r}")
        family_role_row = family_role_by_id.get(family_id)
        classification, reasoning = classify_row(binding_row, contract_row, family_role_row, thresholds)
        report_rows.append(
            build_report_row(
                binding_row=binding_row,
                contract_row=contract_row,
                previous_status=previous_status,
                active_watchlist_ids=active_watchlist_ids,
                family_role_row=family_role_row,
                classification=classification,
                reasoning=reasoning,
            )
        )

    report_rows.sort(key=shortlist_sort_key)
    classification_counts = {
        TRADABLE_DIRECTIONAL: sum(1 for row in report_rows if row["classification"] == TRADABLE_DIRECTIONAL),
        CONTEXT_GUARD: sum(1 for row in report_rows if row["classification"] == CONTEXT_GUARD),
        RESEARCH_ONLY: sum(1 for row in report_rows if row["classification"] == RESEARCH_ONLY),
    }
    shortlist_rows = build_shortlist(report_rows, args.max_shortlist)
    status = STATUS_OK if shortlist_rows else STATUS_DISCOVERY_BIAS

    out_report_path.parent.mkdir(parents=True, exist_ok=True)
    out_shortlist_path.parent.mkdir(parents=True, exist_ok=True)

    generated_ts_utc = utc_now()
    report_doc = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": generated_ts_utc,
        "status": status,
        "governance": {
            "authoritative_inputs": [
                {"concept": "candidate_strategy_contract", "path": args.contract_json, "registry_entry": contract_truth},
                {"concept": "runtime_binding", "path": args.binding_json, "registry_entry": binding_truth},
                {"concept": "active_shadow_subset", "path": args.shadow_watchlist_json, "registry_entry": shadow_truth},
            ],
            "derived_inputs": [
                {"concept": "family_role_classification", "path": args.family_role_json, "registry_entry": family_role_truth},
            ],
            "historical_inputs": [
                {"concept": "before_runtime_binding_snapshot", "path": args.before_binding_json},
            ],
            "notes": [
                "This report does not modify ranking or the active shadow subset.",
                "Family-role classification is used as a conservative guardrail, not as the primary runtime truth surface.",
            ],
        },
        "freshness": {
            "contract_generated_ts_utc": contract.get("generated_ts_utc"),
            "binding_generated_ts_utc": binding.get("generated_ts_utc"),
            "family_role_generated_ts_utc": family_role.get("generated_ts_utc"),
            "shadow_watchlist_generated_ts_utc": shadow_watchlist.get("generated_ts_utc"),
            "before_binding_generated_ts_utc": before_binding.get("generated_ts_utc"),
        },
        "triage_policy": {
            "focus": "newly_bound_non_momentum_bound_shadow_runnable_only",
            "directional_family_allowlist": ["return_reversal_v1"],
            "hard_research_only_behavior": "Anything ambiguous stays RESEARCH_ONLY.",
            "context_guard_behavior": "Families governed as CONTEXT_GUARD stay non-tradable even if runtime-bound.",
            "conservative_thresholds": {
                "abs_t_stat_gte": thresholds.min_abs_t_stat,
                "event_count_gte": thresholds.min_event_count,
                "mean_product_sign": "negative",
                "t_stat_sign": "negative",
                "stream": "trade",
                "single_symbol_required": True,
            },
        },
        "summary": {
            "newly_bound_non_momentum_count": len(report_rows),
            "classification_counts": classification_counts,
            "shortlist_count": len(shortlist_rows),
        },
        "report": report_rows,
    }

    shortlist_doc = {
        "schema_version": SHORTLIST_SCHEMA_VERSION,
        "generated_ts_utc": generated_ts_utc,
        "status": status,
        "source_tradability_report_json": args.out_report,
        "shortlist_policy": {
            "focus": "newly_bound_non_momentum_only",
            "include_only_classification": TRADABLE_DIRECTIONAL,
            "max_items": args.max_shortlist,
            "ordering": "rank asc, family_id asc, exchange asc, stream asc, selected_symbol asc, strategy_id asc",
            "dedupe_key": ["family_id", "exchange", "stream", "selected_symbol"],
            "ranking_logic_note": "Separate advisory shortlist only; does not modify active shadow subset ranking.",
        },
        "summary": {
            "newly_bound_non_momentum_count": len(report_rows),
            "tradable_directional_count": classification_counts[TRADABLE_DIRECTIONAL],
            "context_guard_count": classification_counts[CONTEXT_GUARD],
            "research_only_count": classification_counts[RESEARCH_ONLY],
        },
        "strategies": shortlist_rows,
    }

    out_report_path.write_text(json.dumps(report_doc, indent=2) + "\n", encoding="utf-8")
    out_shortlist_path.write_text(json.dumps(shortlist_doc, indent=2) + "\n", encoding="utf-8")

    print(status)
    print(f"NEW_NON_MOMENTUM={len(report_rows)}")
    print(f"TRADABLE_DIRECTIONAL={classification_counts[TRADABLE_DIRECTIONAL]}")
    print(f"CONTEXT_GUARD={classification_counts[CONTEXT_GUARD]}")
    print(f"RESEARCH_ONLY={classification_counts[RESEARCH_ONLY]}")
    print(f"SHORTLIST_COUNT={len(shortlist_rows)}")
    print(f"REPORT={out_report_path}")
    print(f"SHORTLIST={out_shortlist_path}")
    return 0 if status == STATUS_OK else 2


if __name__ == "__main__":
    raise SystemExit(main())
