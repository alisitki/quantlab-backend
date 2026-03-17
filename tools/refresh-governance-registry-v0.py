#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_JSON = ROOT / "tools" / "system_state" / "canonical_truth_registry_v0.json"
SCHEMA_VERSION = "canonical_truth_registry_v0"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh canonical truth governance registry v0")
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def surface_ref(relpath: str) -> dict[str, Any]:
    path = ROOT / relpath
    payload = load_json(path)
    result: dict[str, Any] = {
        "path": relpath,
        "exists": path.exists(),
    }
    if payload is not None:
        if str(payload.get("schema_version") or "").strip():
            result["schema_version"] = str(payload.get("schema_version"))
        for key in [
            "generated_ts_utc",
            "latest_export_ts_utc",
            "record_count",
            "selected_count",
            "sync_ok",
            "selected_family_id",
            "session_status",
        ]:
            if key in payload:
                result[key] = payload[key]
    return result


def concept(
    *,
    name: str,
    authoritative_now: list[dict[str, Any]],
    produced_by: list[str],
    consumed_by: list[str],
    stale_or_auxiliary_surfaces: list[dict[str, Any]],
    notes: list[str],
) -> dict[str, Any]:
    return {
        "concept": name,
        "authoritative_now": authoritative_now,
        "produced_by": produced_by,
        "consumed_by": consumed_by,
        "stale_or_auxiliary_surfaces": stale_or_auxiliary_surfaces,
        "notes": notes,
    }


def build_registry() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "reader_guide": [
            "Start here before reading summary artifacts.",
            "Code and generated state beat docs when they disagree.",
            "Tool path is the current operational critical path; service lanes exist in parallel.",
        ],
        "active_paths": {
            "current_critical_path": [
                "s3://quantlab-compact/compacted/_state.json",
                "tools/phase5_big_hunt_scheduler_v1.py",
                "pack/state_selection/object_keys_selected.tsv",
                "tools/phase5_big_hunt_v0.py or tools/run-multi-hypothesis.js",
                "tools/phase6_state/promotion_index.json",
                "tools/phase6_candidate_export_v0.py -> tools/phase6_state/candidate_index.json",
                "tools/phase6_candidate_review_v2.py -> tools/phase6_state/candidate_review_v2.tsv",
                "tools/shadow_candidate_bridge_v0.py -> tools/shadow_state/shadow_watchlist_v0.json",
                "tools/run-shadow-observation-batch-v0.py -> tools/shadow_state/shadow_observation_history_v0.jsonl",
                "tools/refresh-shadow-derived-surfaces-v0.py -> tools/shadow_state/shadow_execution_pack_summary_v0.json",
                "tools/run-shadow-watchlist-v0.js",
                "tools/run-soft-live.js",
                "core/strategy/live/LiveStrategyRunner.js",
            ],
            "active_tool_path": {
                "entrypoint": "tools/phase5_nightly_orchestrator_v0.py",
                "note": "Current default operator-facing path for phase5 -> phase6 -> shadow refresh.",
            },
            "active_service_path": {
                "entrypoints": [
                    "services/replayd/routes/stream.js",
                    "services/strategyd/routes/live.routes.js",
                    "core/scheduler/run_daily_prod.js",
                ],
                "status": "PRESENT_BUT_NOT_DEFAULT_CRITICAL_PATH",
                "note": "Service and ML lanes remain in-repo but are not the default hypothesis->shadow execution path.",
            },
            "not_in_current_critical_path": [
                {
                    "path": "tools/run-bound-shadow-launch-v0.py",
                    "reason": "One-shot bound launch helper, not the global shadow subset path.",
                },
                {
                    "path": "tools/run-momentum-continuous-shadow-session-v1.py",
                    "reason": "Family-specific continuous session lane.",
                },
                {
                    "path": "services/strategyd/scripts/runTwin.js",
                    "reason": "Replay parity/validation helper.",
                },
                {
                    "path": "services/strategyd/scripts/runTriad.js",
                    "reason": "Replay parity/validation helper.",
                },
                {
                    "path": "core/scheduler/run_daily_prod.js",
                    "reason": "Parallel ML production scheduler lane.",
                },
            ],
        },
        "concepts": [
            concept(
                name="family_role_classification",
                authoritative_now=[surface_ref("tools/phase6_state/hypothesis_family_role_classification_v0.json")],
                produced_by=["tools/phase6_hypothesis_family_role_classification_v0.py"],
                consumed_by=["tools/phase6_primary_directional_family_selection_v0.py"],
                stale_or_auxiliary_surfaces=[surface_ref("tools/hypotheses/README.md")],
                notes=[
                    "Derived summary for selection and audit only.",
                    "Use candidate_strategy_contract_v0.json and candidate_strategy_runtime_binding_v0.json for current translation/runtime truth.",
                ],
            ),
            concept(
                name="primary_directional_selection",
                authoritative_now=[surface_ref("tools/phase6_state/primary_directional_family_selection_v0.json")],
                produced_by=["tools/phase6_primary_directional_family_selection_v0.py"],
                consumed_by=["tools/phase6_candidate_strategy_contract_v0.py"],
                stale_or_auxiliary_surfaces=[surface_ref("tools/phase6_state/hypothesis_family_role_classification_v0.json")],
                notes=[
                    "selected_family_id is the default translation preference.",
                    "This summary is not the runtime binding ledger.",
                ],
            ),
            concept(
                name="candidate_review",
                authoritative_now=[
                    surface_ref("tools/phase6_state/candidate_review_v2.json"),
                    surface_ref("tools/phase6_state/candidate_review_v2.tsv"),
                ],
                produced_by=["tools/phase6_candidate_review_v2.py"],
                consumed_by=[
                    "tools/phase5_nightly_orchestrator_v0.py",
                    "tools/phase6_candidate_strategy_contract_v0.py",
                    "tools/shadow_candidate_bridge_v0.py",
                ],
                stale_or_auxiliary_surfaces=[
                    surface_ref("tools/phase6_state/candidate_review.json"),
                    surface_ref("tools/phase6_state/candidate_review.tsv"),
                    surface_ref("tools/phase6_state/candidate_queue.jsonl"),
                    surface_ref("tools/phase6_state/candidate_index.json"),
                ],
                notes=[
                    "Ranking and observation-selection truth for candidate packs.",
                    "review_class/class_priority are authoritative; scalar score is secondary within class.",
                ],
            ),
            concept(
                name="candidate_strategy_contract",
                authoritative_now=[surface_ref("tools/phase6_state/candidate_strategy_contract_v0.json")],
                produced_by=["tools/phase6_candidate_strategy_contract_v0.py"],
                consumed_by=[
                    "tools/phase6_strategy_runtime_binding_v0.py",
                    "tools/phase6_hypothesis_family_role_classification_v0.py",
                ],
                stale_or_auxiliary_surfaces=[surface_ref("tools/phase6_state/primary_directional_family_selection_v0.json")],
                notes=[
                    "Translation truth for candidate->strategy mapping.",
                ],
            ),
            concept(
                name="runtime_binding",
                authoritative_now=[surface_ref("tools/phase6_state/candidate_strategy_runtime_binding_v0.json")],
                produced_by=["tools/phase6_strategy_runtime_binding_v0.py"],
                consumed_by=[
                    "tools/run-bound-shadow-launch-v0.py",
                    "tools/run-momentum-continuous-shadow-session-v1.py",
                    "tools/shadow_futures_paper_ledger_v1.py",
                ],
                stale_or_auxiliary_surfaces=[
                    surface_ref("tools/phase6_state/family_shadow_runtime_binding_map_v0.json"),
                    surface_ref("tools/shadow_state/shadow_bound_launch_watchlist_v0.json"),
                ],
                notes=[
                    "Runtime bindability truth.",
                ],
            ),
            concept(
                name="active_shadow_subset",
                authoritative_now=[surface_ref("tools/shadow_state/shadow_watchlist_v0.json")],
                produced_by=["tools/shadow_candidate_bridge_v0.py"],
                consumed_by=[
                    "tools/run-shadow-observation-batch-v0.py",
                    "tools/run-shadow-watchlist-v0.js",
                    "tools/shadow_operator_snapshot_v0.py",
                ],
                stale_or_auxiliary_surfaces=[
                    surface_ref("tools/shadow_state/shadow_bound_launch_watchlist_v0.json"),
                    surface_ref("tools/shadow_state/shadow_operator_snapshot_v0.json"),
                ],
                notes=[
                    "Global observation/live-paper subset truth.",
                    "Separate from runtime binding and continuous session state.",
                ],
            ),
            concept(
                name="continuous_session_state",
                authoritative_now=[surface_ref("tools/shadow_state/momentum_continuous_shadow_session_v1.json")],
                produced_by=["tools/run-momentum-continuous-shadow-session-v1.py"],
                consumed_by=["tools/run-momentum-continuous-shadow-session-v1.py"],
                stale_or_auxiliary_surfaces=[
                    surface_ref("tools/shadow_state/momentum_continuous_shadow_session_artifacts_v1"),
                ],
                notes=[
                    "Family-specific continuous session state.",
                ],
            ),
            concept(
                name="replay_truth",
                authoritative_now=[surface_ref("core/replay/ReplayEngine.js")],
                produced_by=["core/replay/ReplayEngine.js"],
                consumed_by=[
                    "services/replayd/routes/stream.js",
                    "services/strategyd/scripts/runTwin.js",
                    "services/strategyd/scripts/runTriad.js",
                ],
                stale_or_auxiliary_surfaces=[surface_ref("docs/replay.md")],
                notes=[
                    "Replay semantics live in code, not in a generated state file.",
                ],
            ),
            concept(
                name="strategyd_truth",
                authoritative_now=[
                    surface_ref("services/strategyd/routes/live.routes.js"),
                    surface_ref("tools/run-soft-live.js"),
                ],
                produced_by=[
                    "services/strategyd/routes/live.routes.js",
                    "tools/run-soft-live.js",
                ],
                consumed_by=[
                    "services/strategyd/server.js",
                    "tools/run-shadow-watchlist-v0.js",
                ],
                stale_or_auxiliary_surfaces=[surface_ref("services/strategyd/routes/state.js")],
                notes=[
                    "Service lane and tool lane both exist; tool lane is the current critical path.",
                ],
            ),
            concept(
                name="scheduler_truth",
                authoritative_now=[
                    surface_ref("tools/phase5_nightly_orchestrator_v0.py"),
                    surface_ref("core/scheduler/run_daily_prod.js"),
                ],
                produced_by=[
                    "tools/phase5_nightly_orchestrator_v0.py",
                    "core/scheduler/run_daily_prod.js",
                ],
                consumed_by=[
                    "tools/phase5_state/nightly_orchestrator_report_20260315_013640.json",
                    "core/scheduler/run_daily_ml.js",
                ],
                stale_or_auxiliary_surfaces=[surface_ref("docs/architecture.md")],
                notes=[
                    "Two schedulers exist for different lanes; only the phase5 nightly orchestrator sits on the current hypothesis->shadow path.",
                ],
            ),
        ],
        "stale_surfaces": [
            {
                "path": "tools/phase6_state/hypothesis_family_role_classification_v0.json",
                "status": "DERIVED_SUMMARY_ONLY",
                "see_concept": "family_role_classification",
            },
            {
                "path": "tools/phase6_state/primary_directional_family_selection_v0.json",
                "status": "DERIVED_SELECTION_SUMMARY",
                "see_concept": "primary_directional_selection",
            },
            {
                "path": "tools/shadow_state/shadow_bound_launch_watchlist_v0.json",
                "status": "ONE_SHOT_SELECTION_ONLY",
                "see_concept": "active_shadow_subset",
            },
        ],
    }


def main() -> int:
    args = parse_args()
    out_json = Path(args.out_json).resolve()
    payload = build_registry()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"registry_json={out_json}")
    print(f"concept_count={len(payload['concepts'])}")
    print("concepts_csv=" + ",".join(item["concept"] for item in payload["concepts"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
