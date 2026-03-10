#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "primary_directional_family_selection_v0"
PRIMARY_DIRECTIONAL = "PRIMARY_DIRECTIONAL"
TRANSLATABLE = "TRANSLATABLE"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
REQUIRED_REVIEW_COLUMNS = {"rank", "decision_tier", "pack_id", "pack_path"}


class PrimaryDirectionalFamilySelectionError(RuntimeError):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Primary directional family selection v0 builder")
    p.add_argument(
        "--role-classification-json",
        default="tools/phase6_state/hypothesis_family_role_classification_v0.json",
        help="Hypothesis family role classification artifact",
    )
    p.add_argument(
        "--candidate-review-tsv",
        default="tools/phase6_state/candidate_review.tsv",
        help="Candidate review TSV",
    )
    p.add_argument(
        "--candidate-strategy-contract-json",
        default="tools/phase6_state/candidate_strategy_contract_v0.json",
        help="Candidate strategy contract artifact",
    )
    p.add_argument(
        "--candidate-strategy-runtime-binding-json",
        default="tools/phase6_state/candidate_strategy_runtime_binding_v0.json",
        help="Candidate strategy runtime binding artifact",
    )
    p.add_argument(
        "--out-json",
        default="tools/phase6_state/primary_directional_family_selection_v0.json",
        help="Output artifact path",
    )
    return p.parse_args(argv)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise PrimaryDirectionalFamilySelectionError(f"missing json artifact: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PrimaryDirectionalFamilySelectionError(f"invalid json artifact: {path}") from exc
    if not isinstance(payload, dict):
        raise PrimaryDirectionalFamilySelectionError(f"json artifact must be object: {path}")
    return payload


def load_role_items(path: Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path)
    if str(payload.get("schema_version", "")) != "hypothesis_family_role_classification_v0":
        raise PrimaryDirectionalFamilySelectionError(f"unexpected role classification schema: {path}")
    items = payload.get("items")
    if not isinstance(items, list):
        raise PrimaryDirectionalFamilySelectionError(f"role classification items missing: {path}")
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        family_id = str(item.get("family_id", "")).strip()
        if family_id:
            out[family_id] = item
    return out


def load_candidate_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise PrimaryDirectionalFamilySelectionError(f"missing candidate review tsv: {path}")
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        rows = list(reader)
        fieldnames = set(reader.fieldnames or [])
    if not REQUIRED_REVIEW_COLUMNS.issubset(fieldnames):
        raise PrimaryDirectionalFamilySelectionError(f"candidate review tsv missing required columns: {path}")
    return rows


def load_translation_counts(path: Path) -> Counter[str]:
    payload = load_json(path)
    if str(payload.get("schema_version", "")) != "candidate_strategy_contract_v0":
        raise PrimaryDirectionalFamilySelectionError(f"unexpected candidate strategy contract schema: {path}")
    counts: Counter[str] = Counter()
    for item in payload.get("items", []):
        if not isinstance(item, dict) or str(item.get("translation_status", "")) != TRANSLATABLE:
            continue
        spec = item.get("strategy_spec")
        family_id = str(spec.get("family_id", "")).strip() if isinstance(spec, dict) else ""
        if family_id:
            counts[family_id] += 1
    return counts


def load_binding_counts(path: Path) -> Counter[str]:
    payload = load_json(path)
    if str(payload.get("schema_version", "")) != "candidate_strategy_runtime_binding_v0":
        raise PrimaryDirectionalFamilySelectionError(f"unexpected runtime binding schema: {path}")
    counts: Counter[str] = Counter()
    for item in payload.get("items", []):
        if not isinstance(item, dict):
            continue
        family_id = str(item.get("family_id", "")).strip()
        status = str(item.get("runtime_binding_status", "")).strip()
        if family_id and status == BOUND_SHADOW_RUNNABLE:
            counts[family_id] += 1
    return counts


def extract_support_metric(report_obj: dict[str, Any]) -> tuple[str, float | None]:
    result = report_obj.get("result")
    selected = result.get("selected_cell") if isinstance(result, dict) else None
    if isinstance(selected, dict):
        for key in ("event_count", "jump_count", "sample_count", "signal_support"):
            if key in selected:
                try:
                    return key, float(selected[key])
                except (TypeError, ValueError):
                    return key, None
    if isinstance(result, dict) and "signal_support" in result:
        try:
            return "signal_support", float(result["signal_support"])
        except (TypeError, ValueError):
            return "signal_support", None
    return "unknown", None


def extract_t_stat(report_obj: dict[str, Any]) -> float | None:
    result = report_obj.get("result")
    selected = result.get("selected_cell") if isinstance(result, dict) else None
    if isinstance(selected, dict) and "t_stat" in selected:
        try:
            return abs(float(selected["t_stat"]))
        except (TypeError, ValueError):
            return None
    if isinstance(result, dict) and "t_stat" in result:
        try:
            return abs(float(result["t_stat"]))
        except (TypeError, ValueError):
            return None
    return None


def collect_candidate_family_metrics(
    rows: list[dict[str, str]],
    eligible_families: set[str],
) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "promote_strong_pack_ids": set(),
            "promote_pack_ids": set(),
            "all_pack_ids": set(),
            "single_symbol_pack_ids": set(),
            "support_values": [],
            "abs_t_stat_values": [],
            "support_metric_name_counts": Counter(),
            "sample_report_paths": [],
        }
    )
    for row in rows:
        pack_id = str(row.get("pack_id", "")).strip()
        pack_path = Path(str(row.get("pack_path", "")).strip())
        decision_tier = str(row.get("decision_tier", "")).strip()
        selected_symbols: list[str] = []
        plan_path = pack_path / "campaign_plan.json"
        if plan_path.exists():
            try:
                plan_obj = json.loads(plan_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                plan_obj = {}
            if isinstance(plan_obj, dict):
                selected_symbols = [
                    str(value).strip().lower()
                    for value in plan_obj.get("selected_symbols", [])
                    if str(value).strip()
                ]
        seen_in_pack: set[str] = set()
        for report_path in sorted(pack_path.glob("runs/*/artifacts/multi_hypothesis/family_*_report.json")):
            try:
                report_obj = json.loads(report_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            family_id = str(report_obj.get("family_id", "")).strip()
            if family_id not in eligible_families or family_id in seen_in_pack:
                continue
            seen_in_pack.add(family_id)
            entry = metrics[family_id]
            entry["all_pack_ids"].add(pack_id)
            if decision_tier == "PROMOTE_STRONG":
                entry["promote_strong_pack_ids"].add(pack_id)
            if decision_tier == "PROMOTE":
                entry["promote_pack_ids"].add(pack_id)
            if len(selected_symbols) == 1:
                entry["single_symbol_pack_ids"].add(pack_id)
            support_metric_name, support_value = extract_support_metric(report_obj)
            if support_metric_name:
                entry["support_metric_name_counts"][support_metric_name] += 1
            if support_value is not None:
                entry["support_values"].append(float(support_value))
            abs_t_stat = extract_t_stat(report_obj)
            if abs_t_stat is not None:
                entry["abs_t_stat_values"].append(float(abs_t_stat))
            sample_paths: list[str] = entry["sample_report_paths"]
            report_str = str(report_path)
            if report_str not in sample_paths and len(sample_paths) < 2:
                sample_paths.append(report_str)
    return metrics


def median_or_zero(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


def build_payload(
    role_classification_path: Path,
    candidate_review_tsv_path: Path,
    candidate_strategy_contract_path: Path,
    candidate_runtime_binding_path: Path,
) -> dict[str, Any]:
    role_items = load_role_items(role_classification_path)
    eligible_families = sorted(
        family_id for family_id, item in role_items.items() if str(item.get("role", "")) == PRIMARY_DIRECTIONAL
    )
    if not eligible_families:
        raise PrimaryDirectionalFamilySelectionError("no PRIMARY_DIRECTIONAL families in role classification")

    rows = load_candidate_rows(candidate_review_tsv_path)
    translated_counts = load_translation_counts(candidate_strategy_contract_path)
    bound_counts = load_binding_counts(candidate_runtime_binding_path)
    candidate_metrics = collect_candidate_family_metrics(rows, set(eligible_families))

    scorecard: list[dict[str, Any]] = []
    for family_id in eligible_families:
        role_item = role_items[family_id]
        metrics = candidate_metrics[family_id]
        support_metric_name = "unknown"
        if metrics["support_metric_name_counts"]:
            support_metric_name = sorted(
                metrics["support_metric_name_counts"].items(), key=lambda kv: (-kv[1], kv[0])
            )[0][0]
        scorecard.append(
            {
                "family_id": family_id,
                "role": PRIMARY_DIRECTIONAL,
                "promote_strong_pack_count": len(metrics["promote_strong_pack_ids"]),
                "promote_pack_count": len(metrics["promote_pack_ids"]),
                "total_candidate_pack_count": len(metrics["all_pack_ids"]),
                "single_symbol_candidate_pack_count": len(metrics["single_symbol_pack_ids"]),
                "strategy_translatable_now_count": int(translated_counts.get(family_id, 0)),
                "runtime_bindable_now_count": int(bound_counts.get(family_id, 0)),
                "paper_execution_ready_now_count": 1 if bool(role_item.get("paper_execution_ready_now")) else 0,
                "selected_support_metric_name": support_metric_name,
                "selected_support_median": median_or_zero(metrics["support_values"]),
                "selected_abs_t_stat_median": median_or_zero(metrics["abs_t_stat_values"]),
                "short_rationale": str(role_item.get("rationale", "")),
                "sample_report_paths": list(metrics["sample_report_paths"]),
                "next_path": str(role_item.get("next_path", "")),
            }
        )

    def score_key(item: dict[str, Any]) -> tuple[Any, ...]:
        return (
            -int(item["promote_strong_pack_count"]),
            -int(item["promote_pack_count"]),
            -int(item["total_candidate_pack_count"]),
            -int(item["single_symbol_candidate_pack_count"]),
            -float(item["selected_support_median"]),
            -float(item["selected_abs_t_stat_median"]),
            -int(item["strategy_translatable_now_count"]),
            -int(item["runtime_bindable_now_count"]),
            -int(item["paper_execution_ready_now_count"]),
            str(item["family_id"]),
        )

    scorecard.sort(key=score_key)
    for idx, item in enumerate(scorecard, start=1):
        item["selection_rank"] = idx

    winner = scorecard[0]
    selected_reason = (
        f"{winner['family_id']} won because it is PRIMARY_DIRECTIONAL and ranked first by "
        f"PROMOTE_STRONG ({winner['promote_strong_pack_count']}), PROMOTE ({winner['promote_pack_count']}), "
        f"candidate-pack coverage ({winner['total_candidate_pack_count']}), single-symbol coverage "
        f"({winner['single_symbol_candidate_pack_count']}), selected-support median ({winner['selected_support_median']}), "
        f"and |t_stat| median ({winner['selected_abs_t_stat_median']})."
    )

    selection_rule = [
        "eligible_families = role == PRIMARY_DIRECTIONAL only",
        "sort by promote_strong_pack_count descending",
        "then promote_pack_count descending",
        "then total_candidate_pack_count descending",
        "then single_symbol_candidate_pack_count descending",
        "then selected_support_median descending",
        "then selected_abs_t_stat_median descending",
        "then strategy_translatable_now_count descending",
        "then runtime_bindable_now_count descending",
        "then paper_execution_ready_now_count descending",
        "then family_id ascending",
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now(),
        "source_role_classification_json": str(role_classification_path),
        "source_candidate_review_tsv": str(candidate_review_tsv_path),
        "source_candidate_strategy_contract_json": str(candidate_strategy_contract_path),
        "source_candidate_strategy_runtime_binding_json": str(candidate_runtime_binding_path),
        "selection_rule": selection_rule,
        "eligible_families": eligible_families,
        "scorecard": scorecard,
        "selected_family_id": winner["family_id"],
        "selected_reason": selected_reason,
        "next_path": (
            f"{winner['family_id']} is the next target for candidate->strategy translation scope improvement, "
            f"runtime strategy implementation, runtime binding, and paper execution contract work."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_payload(
        role_classification_path=Path(args.role_classification_json),
        candidate_review_tsv_path=Path(args.candidate_review_tsv),
        candidate_strategy_contract_path=Path(args.candidate_strategy_contract_json),
        candidate_runtime_binding_path=Path(args.candidate_strategy_runtime_binding_json),
    )
    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(f"eligible_family_count={len(payload['eligible_families'])}")
    print(f"selected_family_id={payload['selected_family_id']}")
    print("selection_order=" + ",".join(item["family_id"] for item in payload["scorecard"]))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PrimaryDirectionalFamilySelectionError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
