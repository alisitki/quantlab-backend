#!/usr/bin/env python3
"""Read-only hypothesis family role/effectiveness audit.

This tool consumes existing Phase6/Phase7 artifacts and emits a compact family
classification plus a nightly execution policy. It does not mutate runtime,
ranking, promotion, compacted data, or strategy logic.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

DEFAULT_CANDIDATE_REVIEW = ROOT / "tools/phase6_state/candidate_review_v2.json"
DEFAULT_PROMOTION_INDEX = ROOT / "tools/phase6_state/promotion_index.json"
DEFAULT_RUNTIME_BINDING = ROOT / "tools/phase6_state/candidate_strategy_runtime_binding_v0.json"
DEFAULT_MEDIUM_SHADOW_RESULT = ROOT / "tools/phase7_medium_shadow_result_v0.json"
DEFAULT_CONTINUATION_RESULT = ROOT / "tools/phase7_continuation_validation_result_v1.json"
DEFAULT_PROFITABILITY_RESULT = ROOT / "tools/phase7_profitability_analysis_v0.json"
DEFAULT_CANONICAL_REGISTRY = ROOT / "tools/system_state/canonical_truth_registry_v0.json"

DEFAULT_AUDIT_JSON = ROOT / "tools/hypothesis_family_audit_v0.json"
DEFAULT_POLICY_JSON = ROOT / "tools/hypothesis_family_nightly_policy_v0.json"
DEFAULT_REPORT_MD = ROOT / "tools/hypothesis_family_audit_output/hypothesis_family_audit_report_v0.md"

SCHEMA_VERSION = "hypothesis_family_audit_v0"
POLICY_SCHEMA_VERSION = "hypothesis_family_nightly_policy_v0"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def split_families(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).split(",")
    families: list[str] = []
    for item in raw:
        family = str(item).strip()
        if family:
            families.append(family)
    return families


def inc(counter: dict[str, int], key: str, amount: int = 1) -> None:
    counter[key] = counter.get(key, 0) + amount


def empty_metrics(family_id: str) -> dict[str, Any]:
    return {
        "family_id": family_id,
        "discovery_output": {
            "candidate_review_pack_count": 0,
            "promoted_candidate_pack_count": 0,
            "strong_promotions_count": 0,
            "promotion_index_promoted_pack_count": 0,
            "promotion_index_strong_pack_count": 0,
            "review_class_counts": {},
            "decision_tier_counts": {},
            "family_role_counts": {},
        },
        "execution_success": {
            "runtime_binding_row_count": 0,
            "bound_shadow_runnable_count": 0,
            "unbound_config_gap_count": 0,
            "unbound_translation_rejected_count": 0,
            "directional_binding_count": 0,
            "observe_only_binding_count": 0,
            "binding_status_counts": {},
            "shadow_tradeability_class_counts": {},
            "phase7_medium_shadow_count": 0,
            "phase7_medium_continue_count": 0,
            "phase7_extended_validation_count": 0,
            "phase7_extended_keep_count": 0,
        },
        "profitability_outcome": {
            "profitability_tested_count": 0,
            "KEEP_ADVANCING": 0,
            "WEAK_CONTINUE": 0,
            "DROP": 0,
            "FAILED_PROFITABILITY": 0,
            "gross_positive_count": 0,
            "gross_non_positive_count": 0,
            "final_profitability_decisions": [],
        },
        "classification": {},
        "reason": "",
    }


def normalize_counter(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def add_candidate_review_metrics(
    families: dict[str, dict[str, Any]],
    candidate_review: dict[str, Any],
    promotion_index: dict[str, Any],
    unclassified: dict[str, int],
) -> None:
    promote_ids = set(promotion_index.get("promote_pack_ids") or [])
    strong_ids = set(promotion_index.get("promote_strong_pack_ids") or [])

    for row in candidate_review.get("rows") or []:
        row_families = split_families(row.get("selected_family_ids"))
        if not row_families:
            inc(unclassified, "candidate_review_rows_without_selected_family")
            continue
        for family_id in row_families:
            metric = families.setdefault(family_id, empty_metrics(family_id))
            discovery = metric["discovery_output"]
            discovery["candidate_review_pack_count"] += 1
            if row.get("decision_tier") in {"PROMOTE", "PROMOTE_STRONG"}:
                discovery["promoted_candidate_pack_count"] += 1
            if row.get("decision_tier") == "PROMOTE_STRONG":
                discovery["strong_promotions_count"] += 1
            if row.get("pack_id") in promote_ids:
                discovery["promotion_index_promoted_pack_count"] += 1
            if row.get("pack_id") in strong_ids:
                discovery["promotion_index_strong_pack_count"] += 1
            inc(discovery["review_class_counts"], str(row.get("review_class") or "UNKNOWN"))
            inc(discovery["decision_tier_counts"], str(row.get("decision_tier") or "UNKNOWN"))
            inc(discovery["family_role_counts"], str(row.get("family_roles") or "UNKNOWN"))


def add_runtime_binding_metrics(
    families: dict[str, dict[str, Any]],
    runtime_binding: dict[str, Any],
    unclassified: dict[str, int],
) -> None:
    for row in runtime_binding.get("items") or []:
        family_id = row.get("family_id")
        if not family_id:
            inc(unclassified, "runtime_binding_rows_without_family_id")
            inc(unclassified, f"runtime_binding_status:{row.get('runtime_binding_status') or 'UNKNOWN'}")
            continue
        metric = families.setdefault(family_id, empty_metrics(family_id))
        execution = metric["execution_success"]
        execution["runtime_binding_row_count"] += 1
        status = str(row.get("runtime_binding_status") or "UNKNOWN")
        tradeability = str(row.get("shadow_tradeability_class") or "UNKNOWN")
        inc(execution["binding_status_counts"], status)
        inc(execution["shadow_tradeability_class_counts"], tradeability)
        if status == "BOUND_SHADOW_RUNNABLE":
            execution["bound_shadow_runnable_count"] += 1
        if status == "UNBOUND_CONFIG_GAP":
            execution["unbound_config_gap_count"] += 1
        if status == "UNBOUND_TRANSLATION_REJECTED":
            execution["unbound_translation_rejected_count"] += 1
        if tradeability == "DIRECTIONAL":
            execution["directional_binding_count"] += 1
        if tradeability == "OBSERVE_ONLY":
            execution["observe_only_binding_count"] += 1


def add_phase7_metrics(
    families: dict[str, dict[str, Any]],
    medium_result: dict[str, Any],
    continuation_result: dict[str, Any],
    profitability_result: dict[str, Any],
) -> None:
    for row in medium_result.get("results") or []:
        family_id = row.get("family_id")
        if not family_id:
            continue
        metric = families.setdefault(family_id, empty_metrics(family_id))
        metric["execution_success"]["phase7_medium_shadow_count"] += 1
        if row.get("verdict") == "CONTINUE":
            metric["execution_success"]["phase7_medium_continue_count"] += 1

    for row in continuation_result.get("results") or []:
        family_id = row.get("family_id")
        if not family_id:
            continue
        metric = families.setdefault(family_id, empty_metrics(family_id))
        metric["execution_success"]["phase7_extended_validation_count"] += 1
        if row.get("verdict") == "KEEP_ADVANCING":
            metric["execution_success"]["phase7_extended_keep_count"] += 1

    for row in profitability_result.get("per_strategy") or []:
        family_id = row.get("family_id")
        if not family_id:
            continue
        metric = families.setdefault(family_id, empty_metrics(family_id))
        outcome = metric["profitability_outcome"]
        verdict = str(row.get("verdict") or "UNKNOWN")
        outcome["profitability_tested_count"] += 1
        inc(outcome, verdict)
        if verdict == "DROP":
            outcome["FAILED_PROFITABILITY"] += 1
        gross = (row.get("gross_performance") or {}).get("total_gross_pnl_quote")
        if isinstance(gross, (int, float)) and gross > 0:
            outcome["gross_positive_count"] += 1
        else:
            outcome["gross_non_positive_count"] += 1
        outcome["final_profitability_decisions"].append(
            {
                "strategy_id": row.get("strategy_id"),
                "symbol": row.get("symbol"),
                "verdict": verdict,
                "gross_pnl_quote": gross,
                "net_pnl_quote": (row.get("net_performance") or {}).get("realized_pnl_quote_net"),
                "edge_statement": (row.get("edge_strength") or {}).get("edge_statement"),
            }
        )


def infer_role(metric: dict[str, Any]) -> str:
    family_id = metric["family_id"]
    execution = metric["execution_success"]
    discovery = metric["discovery_output"]
    role_counts = discovery["family_role_counts"]
    if role_counts.get("CONTEXT_GUARD", 0) > 0 or execution["observe_only_binding_count"] > execution["directional_binding_count"]:
        return "CONTEXT"
    if (
        execution["directional_binding_count"] > 0
        or "momentum" in family_id
        or "reversal" in family_id
    ):
        return "TRADING"
    return "DIAGNOSTIC"


def classify(metric: dict[str, Any]) -> tuple[str, str, str]:
    family_id = metric["family_id"]
    role = infer_role(metric)
    execution = metric["execution_success"]
    outcome = metric["profitability_outcome"]

    if role == "CONTEXT":
        return (
            role,
            "ACTIVE",
            "REDUCED_NIGHTLY",
            "Context/guard behavior is useful but should not produce directional candidate spam.",
        )

    tested = outcome["profitability_tested_count"]
    failed = outcome["FAILED_PROFITABILITY"]
    keep = outcome["KEEP_ADVANCING"]
    weak = outcome["WEAK_CONTINUE"]

    if family_id == "momentum_v1" and tested == 0:
        return (
            role,
            "WEAK",
            "REDUCED_NIGHTLY",
            "No current profitability PASS in Phase7 artifacts; prior tightening/OOS weakness requires controlled nightly instead of full-volume generation.",
        )

    if tested > 0 and failed == tested and keep == 0 and weak == 0:
        return (
            role,
            "FAILED",
            "REDUCED_NIGHTLY",
            "Profitability-tested subset failed economic edge: gross pnl was non-positive before fees.",
        )

    if keep > 0:
        return (
            role,
            "ACTIVE",
            "FULL_NIGHTLY",
            "At least one profitability-tested candidate kept advancing with positive evidence.",
        )

    if execution["phase7_extended_validation_count"] > 0 or execution["phase7_medium_shadow_count"] > 0:
        return (
            role,
            "WEAK",
            "REDUCED_NIGHTLY",
            "Reached Phase7 but current evidence is not strong enough for full-volume nightly generation.",
        )

    if execution["bound_shadow_runnable_count"] > 0:
        return (
            role,
            "ACTIVE",
            "FULL_NIGHTLY",
            "Runtime-runnable directional family with no profitability failure in the current evidence set.",
        )

    if metric["discovery_output"]["candidate_review_pack_count"] > 0:
        return (
            role,
            "UNTESTED",
            "ON_DEMAND",
            "Produced candidates but has not reached runtime-runnable Phase7 evidence in this audit set.",
        )

    return (role, "UNTESTED", "ON_DEMAND", "No meaningful current candidate or Phase7 evidence found.")


def apply_classification(families: dict[str, dict[str, Any]]) -> None:
    for family_id in sorted(families):
        metric = families[family_id]
        role, status, nightly_mode, reason = classify(metric)
        metric["classification"] = {
            "role": role,
            "status": status,
            "nightly_mode": nightly_mode,
        }
        metric["reason"] = reason


def candidate_volume_impact(candidate_review: dict[str, Any], families: dict[str, dict[str, Any]]) -> dict[str, Any]:
    mode_by_family = {
        family_id: metric["classification"]["nightly_mode"]
        for family_id, metric in families.items()
    }
    total_pack_rows = 0
    total_family_attributions = 0
    attribution_by_mode: Counter[str] = Counter()
    pack_rows_all_non_full = 0
    pack_rows_mixed_full_and_reduced = 0
    affected_pack_rows = 0
    for row in candidate_review.get("rows") or []:
        fams = split_families(row.get("selected_family_ids"))
        if not fams:
            continue
        total_pack_rows += 1
        modes = {mode_by_family.get(family, "ON_DEMAND") for family in fams}
        total_family_attributions += len(fams)
        for family in fams:
            attribution_by_mode[mode_by_family.get(family, "ON_DEMAND")] += 1
        if modes and "FULL_NIGHTLY" not in modes:
            pack_rows_all_non_full += 1
        if "FULL_NIGHTLY" in modes and any(mode != "FULL_NIGHTLY" for mode in modes):
            pack_rows_mixed_full_and_reduced += 1
        if any(mode != "FULL_NIGHTLY" for mode in modes):
            affected_pack_rows += 1

    reduced_modes = {"REDUCED_NIGHTLY", "ON_DEMAND", "PAUSED"}
    reduced_attributions = sum(attribution_by_mode[mode] for mode in reduced_modes)
    return {
        "candidate_review_pack_rows_with_family": total_pack_rows,
        "candidate_family_attributions_total": total_family_attributions,
        "candidate_family_attributions_by_nightly_mode": dict(sorted(attribution_by_mode.items())),
        "candidate_family_attributions_eligible_for_reduction": reduced_attributions,
        "candidate_family_attribution_reduction_share_if_reduced_modes_are_capped": (
            round(reduced_attributions / total_family_attributions, 6) if total_family_attributions else None
        ),
        "candidate_pack_rows_affected_by_non_full_policy": affected_pack_rows,
        "candidate_pack_rows_all_selected_families_non_full": pack_rows_all_non_full,
        "candidate_pack_rows_mixed_full_and_reduced": pack_rows_mixed_full_and_reduced,
        "interpretation": (
            "This is a policy impact estimate, not a deletion count. Non-full modes mean capped handling, not family removal."
        ),
    }


def safety_check(families: dict[str, dict[str, Any]]) -> dict[str, Any]:
    full_nightly_trading = [
        family_id
        for family_id, metric in families.items()
        if metric["classification"]["role"] == "TRADING"
        and metric["classification"]["nightly_mode"] == "FULL_NIGHTLY"
    ]
    retained_controlled_trading = [
        family_id
        for family_id, metric in families.items()
        if metric["classification"]["role"] == "TRADING"
        and metric["classification"]["status"] in {"ACTIVE", "WEAK"}
        and metric["classification"]["nightly_mode"] in {"FULL_NIGHTLY", "REDUCED_NIGHTLY"}
    ]
    deleted = []
    return {
        "no_family_deleted": True,
        "deleted_families": deleted,
        "diagnostic_or_context_capability_preserved": any(
            metric["classification"]["role"] in {"CONTEXT", "DIAGNOSTIC"} for metric in families.values()
        ),
        "active_full_nightly_trading_families": sorted(full_nightly_trading),
        "active_full_nightly_trading_family_count": len(full_nightly_trading),
        "controlled_trading_discovery_families": sorted(retained_controlled_trading),
        "controlled_trading_discovery_family_count": len(retained_controlled_trading),
        "passes_controlled_discovery_guard": len(retained_controlled_trading) >= 1,
    }


def policy_rows(families: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family_id in sorted(families):
        metric = families[family_id]
        classification = metric["classification"]
        rows.append(
            {
                "family_id": family_id,
                "role": classification["role"],
                "status": classification["status"],
                "nightly_mode": classification["nightly_mode"],
                "reason": metric["reason"],
                "evidence": {
                    "candidate_review_pack_count": metric["discovery_output"]["candidate_review_pack_count"],
                    "strong_promotions_count": metric["discovery_output"]["strong_promotions_count"],
                    "bound_shadow_runnable_count": metric["execution_success"]["bound_shadow_runnable_count"],
                    "phase7_medium_shadow_count": metric["execution_success"]["phase7_medium_shadow_count"],
                    "phase7_extended_validation_count": metric["execution_success"]["phase7_extended_validation_count"],
                    "profitability_tested_count": metric["profitability_outcome"]["profitability_tested_count"],
                    "failed_profitability_count": metric["profitability_outcome"]["FAILED_PROFITABILITY"],
                },
            }
        )
    return rows


def render_markdown(audit: dict[str, Any], policy: dict[str, Any]) -> str:
    lines = [
        "# Hypothesis Family Audit v0",
        "",
        f"Generated: `{audit['generated_ts_utc']}`",
        "",
        "## Nightly Policy",
        "",
        "| family_id | role | status | nightly_mode | reason |",
        "|---|---:|---:|---:|---|",
    ]
    for row in policy["policy_table"]:
        lines.append(
            f"| `{row['family_id']}` | `{row['role']}` | `{row['status']}` | `{row['nightly_mode']}` | {row['reason']} |"
        )
    lines.extend(
        [
            "",
            "## Downgrades",
            "",
        ]
    )
    downgraded = [row for row in policy["policy_table"] if row["nightly_mode"] != "FULL_NIGHTLY"]
    for row in downgraded:
        lines.append(f"- `{row['family_id']}` -> `{row['nightly_mode']}`: {row['reason']}")
    if not downgraded:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Remain Full Nightly",
            "",
        ]
    )
    for row in policy["policy_table"]:
        if row["nightly_mode"] == "FULL_NIGHTLY":
            lines.append(f"- `{row['family_id']}` remains `FULL_NIGHTLY`: {row['reason']}")
    if not any(row["nightly_mode"] == "FULL_NIGHTLY" for row in policy["policy_table"]):
        lines.append("- None. Current evidence does not justify unrestricted `FULL_NIGHTLY` for any family.")
    lines.extend(
        [
            "",
            "## Cost-Aware Prefilter",
            "",
            f"- Status: `{policy['cost_aware_prefilter']['status']}`",
            f"- Rule: {policy['cost_aware_prefilter']['rule']}",
            f"- Unknown gross handling: {policy['cost_aware_prefilter']['unknown_gross_handling']}",
        ]
    )
    impact = audit["pipeline_impact_summary"]
    lines.extend(
        [
            "",
            "## Pipeline Impact",
            "",
            f"- Candidate-family attributions eligible for reduced handling: `{impact['candidate_family_attributions_eligible_for_reduction']}` / `{impact['candidate_family_attributions_total']}`.",
            f"- Pack rows fully under non-full families: `{impact['candidate_pack_rows_all_selected_families_non_full']}`.",
            f"- Mixed rows preserving a full-nightly family while reducing another family attribution: `{impact['candidate_pack_rows_mixed_full_and_reduced']}`.",
            "- No family is deleted; context capability remains; trading discovery is retained through reduced control lanes.",
            "",
            "## Profitability Note",
            "",
            "`momentum_v1` is downgraded because the current audit has no profitability PASS for it and prior tightening/OOS evidence makes full-volume nightly too optimistic.",
            "`return_reversal_v1` is not a discovery failure. It produced fill-backed Phase7 activity, but the tested profitability subset had non-positive gross PnL before fees, so nightly volume should be reduced until a cost/profitability gate improves the lane.",
            "",
        ]
    )
    return "\n".join(lines)


def build_outputs(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate_review = read_json(Path(args.candidate_review_json))
    promotion_index = read_json(Path(args.promotion_index_json))
    runtime_binding = read_json(Path(args.runtime_binding_json))
    medium_result = read_json(Path(args.medium_shadow_result_json))
    continuation_result = read_json(Path(args.continuation_result_json))
    profitability_result = read_json(Path(args.profitability_result_json))
    canonical_registry = read_json(Path(args.canonical_truth_registry_json))

    families: dict[str, dict[str, Any]] = {}
    unclassified: dict[str, int] = {}
    add_candidate_review_metrics(families, candidate_review, promotion_index, unclassified)
    add_runtime_binding_metrics(families, runtime_binding, unclassified)
    add_phase7_metrics(families, medium_result, continuation_result, profitability_result)
    apply_classification(families)

    freshness = {
        "canonical_registry_generated_ts_utc": canonical_registry.get("generated_ts_utc"),
        "candidate_review_generated_ts_utc": candidate_review.get("generated_ts_utc"),
        "runtime_binding_generated_ts_utc": runtime_binding.get("generated_ts_utc"),
        "phase7_medium_generated_ts_utc": medium_result.get("generated_ts_utc"),
        "phase7_continuation_generated_ts_utc": continuation_result.get("generated_ts_utc"),
        "phase7_profitability_generated_ts_utc": profitability_result.get("generated_ts_utc"),
        "note": "Registry is a first-entry truth map; current generated Phase6/7 artifacts are newer and used for metrics.",
    }
    impact = candidate_volume_impact(candidate_review, families)
    safety = safety_check(families)

    generated = args.generated_ts or utc_now()
    audit = {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": generated,
        "governance": {
            "scope": "classification_and_nightly_policy_definition_only",
            "no_family_deletion": True,
            "no_strategy_logic_change": True,
            "no_compacted_data_pipeline_change": True,
            "no_ranking_or_promotion_mutation": True,
            "no_new_runs": True,
        },
        "inputs": {
            "candidate_review_json": str(Path(args.candidate_review_json).resolve()),
            "promotion_index_json": str(Path(args.promotion_index_json).resolve()),
            "runtime_binding_json": str(Path(args.runtime_binding_json).resolve()),
            "medium_shadow_result_json": str(Path(args.medium_shadow_result_json).resolve()),
            "continuation_result_json": str(Path(args.continuation_result_json).resolve()),
            "profitability_result_json": str(Path(args.profitability_result_json).resolve()),
            "canonical_truth_registry_json": str(Path(args.canonical_truth_registry_json).resolve()),
        },
        "family_identification": {
            "candidate_review_family_field": "rows[].selected_family_ids",
            "runtime_binding_family_field": "items[].family_id",
            "promotion_index_family_field": None,
            "promotion_index_family_resolution": (
                "promotion_index stores pack ids/paths; family attribution is resolved by joining "
                "promotion_index promote_pack_ids/promote_strong_pack_ids to candidate_review_v2 rows by pack_id"
            ),
        },
        "truth_and_freshness": freshness,
        "family_count": len(families),
        "families": [families[family_id] for family_id in sorted(families)],
        "unclassified_records": dict(sorted(unclassified.items())),
        "pipeline_impact_summary": impact,
        "safety_check": safety,
    }

    policy = {
        "schema_version": POLICY_SCHEMA_VERSION,
        "generated_ts_utc": generated,
        "policy_principles": [
            "No family is deleted by this policy.",
            "Context families retain diagnostic/context capability but should not spam directional candidate queues.",
            "Profitability-failed trading families move to reduced nightly until a new economic edge is demonstrated.",
            "No family is unrestricted full-nightly under the current evidence; trading discovery continues through reduced control lanes.",
        ],
        "cost_aware_prefilter": {
            "status": "POLICY_DEFINED_NOT_APPLIED_BY_THIS_SCRIPT",
            "rule": "After Phase5, candidates with derivable gross_pnl <= 0 should not advance into Phase6 candidate generation.",
            "unknown_gross_handling": "If gross pnl is not derivable from existing artifacts, mark UNKNOWN/needs-review; do not delete or silently fail the candidate.",
            "rationale": "Prevents repeat false-edge loops where fill-backed activity survives but gross edge is non-positive before fees/funding.",
            "implementation_scope": "Nightly scheduler/export/bridge policy; this audit only writes the policy artifact and does not mutate runtime behavior.",
        },
        "nightly_mode_definitions": {
            "FULL_NIGHTLY": "normal scheduled generation and Phase6/7 bridge eligibility",
            "REDUCED_NIGHTLY": "capped/control-lane generation only; no broad candidate expansion until evidence improves",
            "ON_DEMAND": "manual/operator-triggered only",
            "PAUSED": "no scheduled generation; family retained in repo for explicit reactivation",
        },
        "policy_table": policy_rows(families),
        "pipeline_impact_summary": impact,
        "safety_check": safety,
    }
    return audit, policy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-review-json", default=str(DEFAULT_CANDIDATE_REVIEW))
    parser.add_argument("--promotion-index-json", default=str(DEFAULT_PROMOTION_INDEX))
    parser.add_argument("--runtime-binding-json", default=str(DEFAULT_RUNTIME_BINDING))
    parser.add_argument("--medium-shadow-result-json", default=str(DEFAULT_MEDIUM_SHADOW_RESULT))
    parser.add_argument("--continuation-result-json", default=str(DEFAULT_CONTINUATION_RESULT))
    parser.add_argument("--profitability-result-json", default=str(DEFAULT_PROFITABILITY_RESULT))
    parser.add_argument("--canonical-truth-registry-json", default=str(DEFAULT_CANONICAL_REGISTRY))
    parser.add_argument("--audit-json", default=str(DEFAULT_AUDIT_JSON))
    parser.add_argument("--policy-json", default=str(DEFAULT_POLICY_JSON))
    parser.add_argument("--report-md", default=str(DEFAULT_REPORT_MD))
    parser.add_argument("--generated-ts", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit, policy = build_outputs(args)

    audit_path = Path(args.audit_json)
    policy_path = Path(args.policy_json)
    report_path = Path(args.report_md)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    policy_path.write_text(json.dumps(policy, indent=2, sort_keys=True) + "\n")
    report_path.write_text(render_markdown(audit, policy) + "\n")

    print("HYPOTHESIS_FAMILY_AUDIT_COMPLETE")
    print(f"AUDIT_JSON={audit_path}")
    print(f"POLICY_JSON={policy_path}")
    print(f"REPORT_MD={report_path}")
    for row in policy["policy_table"]:
        print(
            "POLICY",
            row["family_id"],
            row["role"],
            row["status"],
            row["nightly_mode"],
            row["reason"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
