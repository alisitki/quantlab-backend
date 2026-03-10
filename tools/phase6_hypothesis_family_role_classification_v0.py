#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "hypothesis_family_role_classification_v0"
PRIMARY_DIRECTIONAL = "PRIMARY_DIRECTIONAL"
CONTEXT_GUARD = "CONTEXT_GUARD"
DISCOVERY_RESEARCH = "DISCOVERY_RESEARCH"

ROLE_BUCKET_DEFINITIONS = {
    PRIMARY_DIRECTIONAL: (
        "Family signal targets signed future return or signed reversal strongly enough "
        "to justify an outright long/short strategy contract once runtime work exists."
    ),
    CONTEXT_GUARD: (
        "Family signal measures context, regime, volatility, liquidity, or spread quality "
        "that can guard or filter a primary strategy but should not be forced into outright futures direction."
    ),
    DISCOVERY_RESEARCH: (
        "Family output is useful for discovery, edge enumeration, or research, but it does not yet define "
        "a clean runtime strategy or support/guard contract."
    ),
}

CLASSIFICATION = {
    "family_a_patternscanner": {
        "role": DISCOVERY_RESEARCH,
        "rationale": (
            "Family-A reports pattern and edge enumeration metrics (`patternsScanned`, "
            "`edgeCandidatesGenerated`, `edgeCandidatesRegistered`, `edges`) rather than "
            "a signed forward-return contract."
        ),
        "strategy_translation_eligibility": "NOT_FOR_DIRECT_STRATEGY_TRANSLATION",
        "runtime_binding_eligibility": "NOT_FOR_DIRECT_RUNTIME_BINDING",
        "paper_execution_eligibility": "RESEARCH_ONLY_NOT_READY",
        "next_path": (
            "Keep in discovery/edge-mining lane; only promote later if a separate edge-to-guard "
            "or edge-to-direction contract is defined."
        ),
    },
    "family_b_simple_momentum": {
        "role": PRIMARY_DIRECTIONAL,
        "rationale": (
            "Simple-momentum family explicitly measures forward signed return "
            "(`mean_forward_return`, `t_stat`, `signal_support`) after strong prior returns, "
            "so it is the closest legacy trade-stream continuation family to an outright directional contract."
        ),
        "strategy_translation_eligibility": "PRIMARY_DIRECTIONAL_NOT_IN_TRANSLATOR_SCOPE",
        "runtime_binding_eligibility": "NO_RUNTIME_BINDING_FOR_FAMILY",
        "paper_execution_eligibility": "PRIMARY_DIRECTIONAL_BUT_NOT_IMPLEMENTED",
        "next_path": (
            "Add a narrow single-symbol trade-stream translator slice, then bind a family-specific "
            "paper runtime after the directional open/close contract is written."
        ),
    },
    "return_reversal_v1": {
        "role": PRIMARY_DIRECTIONAL,
        "rationale": (
            "Return-reversal scores `past_return_bps * fwd_return_bps` and passes only when "
            "`mean_product < 0` with negative `t_stat`, which is a signed price-reversion contract."
        ),
        "strategy_translation_eligibility": "PRIMARY_DIRECTIONAL_NOT_IN_TRANSLATOR_SCOPE",
        "runtime_binding_eligibility": "NO_RUNTIME_BINDING_FOR_FAMILY",
        "paper_execution_eligibility": "PRIMARY_DIRECTIONAL_BUT_NOT_IMPLEMENTED",
        "next_path": (
            "Prioritize as a future primary directional family: define a one-position return-reversion "
            "paper contract, then add translator and binding support."
        ),
    },
    "volatility_clustering_v1": {
        "role": CONTEXT_GUARD,
        "rationale": (
            "Volatility-clustering measures correlation between past and future realized volatility "
            "(`corr`, `t_stat`) and does not predict signed price direction."
        ),
        "strategy_translation_eligibility": "NOT_FOR_DIRECT_STRATEGY_TRANSLATION",
        "runtime_binding_eligibility": "NOT_FOR_DIRECT_RUNTIME_BINDING",
        "paper_execution_eligibility": "SUPPORT_ONLY_CONTEXT_FAMILY",
        "next_path": (
            "Treat as regime/volatility context for guards, risk throttles, or watchlist hints rather than "
            "an outright long/short family."
        ),
    },
    "spread_reversion_v1": {
        "role": CONTEXT_GUARD,
        "rationale": (
            "Spread-reversion scores `past_change * fwd_change` on spread width and passes when "
            "`mean_product < 0`; this supports spread-quality reversion context, not honest outright futures direction."
        ),
        "strategy_translation_eligibility": "CURRENT_TRANSLATABLE_SUBSET_PRESENT",
        "runtime_binding_eligibility": "CURRENT_BOUND_RUNTIME_PRESENT",
        "paper_execution_eligibility": "SUPPORT_ONLY_NON_DIRECTIONAL",
        "next_path": (
            "Keep on the support/guard path. Do not force into outright futures long/short paper execution "
            "unless future family evidence adds explicit directional semantics."
        ),
    },
    "momentum_v1": {
        "role": PRIMARY_DIRECTIONAL,
        "rationale": (
            "Momentum-v1 passes when `past_return_bps * fwd_return_bps` is positive with positive `t_stat`, "
            "which is an explicit signed continuation contract."
        ),
        "strategy_translation_eligibility": "PRIMARY_DIRECTIONAL_NOT_IN_TRANSLATOR_SCOPE",
        "runtime_binding_eligibility": "NO_RUNTIME_BINDING_FOR_FAMILY",
        "paper_execution_eligibility": "PRIMARY_DIRECTIONAL_BUT_NOT_IMPLEMENTED",
        "next_path": (
            "Prioritize for direct candidate->strategy translation after spread-reversion is removed from the "
            "primary-runtime path expectation."
        ),
    },
    "volume_vol_link_v1": {
        "role": CONTEXT_GUARD,
        "rationale": (
            "Volume-vol-link measures whether activity intensity predicts future realized volatility "
            "(`corr`, `mean_activity`, `mean_rv_fwd`) rather than directional return sign."
        ),
        "strategy_translation_eligibility": "NOT_FOR_DIRECT_STRATEGY_TRANSLATION",
        "runtime_binding_eligibility": "NOT_FOR_DIRECT_RUNTIME_BINDING",
        "paper_execution_eligibility": "SUPPORT_ONLY_CONTEXT_FAMILY",
        "next_path": (
            "Use as execution-quality, liquidity, or volatility guard/context input; keep it off the outright "
            "strategy runtime path."
        ),
    },
    "jump_reversion_v1": {
        "role": PRIMARY_DIRECTIONAL,
        "rationale": (
            "Jump-reversion uses `mean_signed_reversal` after signed jumps, which is directly tied to "
            "directional post-jump reversion."
        ),
        "strategy_translation_eligibility": "PRIMARY_DIRECTIONAL_NOT_IN_TRANSLATOR_SCOPE",
        "runtime_binding_eligibility": "NO_RUNTIME_BINDING_FOR_FAMILY",
        "paper_execution_eligibility": "PRIMARY_DIRECTIONAL_BUT_NOT_IMPLEMENTED",
        "next_path": (
            "Add a small signed jump-reversion runtime contract once a primary-directional family slot is "
            "opened beyond the current spread-reversion observe-only slice."
        ),
    },
}

HYPOTHESIS_SOURCE_PATHS = {
    "family_a_patternscanner": ["tools/hypotheses/family_a_patternscanner.md"],
    "family_b_simple_momentum": [
        "tools/hypotheses/family_b_simple_momentum.md",
        "tools/hypotheses/simple_momentum_family.py",
    ],
    "return_reversal_v1": ["tools/hypotheses/return_reversal_v1.py"],
    "volatility_clustering_v1": ["tools/hypotheses/volatility_clustering_v1.py"],
    "spread_reversion_v1": ["tools/hypotheses/spread_reversion_v1.py"],
    "momentum_v1": ["tools/hypotheses/momentum_v1.py"],
    "volume_vol_link_v1": ["tools/hypotheses/volume_vol_link_v1.py"],
    "jump_reversion_v1": ["tools/hypotheses/jump_reversion_v1.py"],
}

RE_FAMILY_ID_DOC = re.compile(r"family_id:\s*`([^`]+)`")
RE_FAMILY_ID_JSON = re.compile(r'family_id"\s*:\s*"([^"]+)"')


class HypothesisFamilyRoleClassificationError(RuntimeError):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Hypothesis family role classification v0 builder")
    p.add_argument(
        "--hypotheses-readme",
        default="tools/hypotheses/README.md",
        help="Current multi-hypothesis contract README",
    )
    p.add_argument(
        "--hypotheses-dir",
        default="tools/hypotheses",
        help="Hypotheses source directory for extra repo-level family discovery",
    )
    p.add_argument(
        "--candidate-review-tsv",
        default="tools/phase6_state/candidate_review.tsv",
        help="Authoritative candidate review TSV",
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
        default="tools/phase6_state/hypothesis_family_role_classification_v0.json",
        help="Output artifact path",
    )
    return p.parse_args(argv)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_family_ids_from_readme(path: Path) -> list[str]:
    if not path.exists():
        raise HypothesisFamilyRoleClassificationError(f"missing hypotheses readme: {path}")
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- `") and stripped.endswith("`"):
            ids.append(stripped[3:-1])
    if not ids:
        raise HypothesisFamilyRoleClassificationError(f"no family ids found in {path}")
    return ids


def discover_repo_family_ids(hypotheses_dir: Path) -> list[str]:
    if not hypotheses_dir.exists():
        raise HypothesisFamilyRoleClassificationError(f"missing hypotheses dir: {hypotheses_dir}")
    ids: set[str] = set()
    for path in sorted(hypotheses_dir.glob("*")):
        if not path.is_file():
            continue
        if path.suffix not in {".py", ".md"}:
            continue
        text = path.read_text(encoding="utf-8")
        for match in RE_FAMILY_ID_DOC.finditer(text):
            ids.add(match.group(1))
        for match in RE_FAMILY_ID_JSON.finditer(text):
            ids.add(match.group(1))
    return sorted(ids)


def read_candidate_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise HypothesisFamilyRoleClassificationError(f"missing candidate review tsv: {path}")
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        rows = list(reader)
    required = {"rank", "decision_tier", "pack_id", "pack_path"}
    if not rows and (reader.fieldnames is None or not required.issubset(set(reader.fieldnames))):
        raise HypothesisFamilyRoleClassificationError(f"candidate review tsv missing required columns: {path}")
    if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
        raise HypothesisFamilyRoleClassificationError(f"candidate review tsv missing required columns: {path}")
    return rows


def load_candidate_coverage(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    coverage: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "candidate_report_count": 0,
            "candidate_pack_ids": set(),
            "sample_candidate_report_paths": [],
        }
    )
    for row in rows:
        pack_id = str(row.get("pack_id", "")).strip()
        pack_path = Path(str(row.get("pack_path", "")).strip())
        if not pack_id or not pack_path.exists():
            continue
        for report_path in sorted(pack_path.glob("runs/*/artifacts/multi_hypothesis/family_*_report.json")):
            try:
                obj = json.loads(report_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            family_id = str(obj.get("family_id", "")).strip()
            if not family_id:
                base = report_path.name
                if base.startswith("family_") and base.endswith("_report.json"):
                    family_id = base[len("family_") : -len("_report.json")]
            if not family_id:
                continue
            entry = coverage[family_id]
            entry["candidate_report_count"] += 1
            entry["candidate_pack_ids"].add(pack_id)
            sample_paths: list[str] = entry["sample_candidate_report_paths"]
            report_str = str(report_path)
            if report_str not in sample_paths and len(sample_paths) < 2:
                sample_paths.append(report_str)
    return coverage


def load_translation_counts(path: Path) -> Counter[str]:
    if not path.exists():
        raise HypothesisFamilyRoleClassificationError(f"missing candidate strategy contract: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if str(payload.get("schema_version", "")) != "candidate_strategy_contract_v0":
        raise HypothesisFamilyRoleClassificationError(f"unexpected candidate strategy contract schema: {path}")
    counts: Counter[str] = Counter()
    for item in payload.get("items", []):
        if str(item.get("translation_status", "")) != "TRANSLATABLE":
            continue
        spec = item.get("strategy_spec")
        family_id = str(spec.get("family_id", "")).strip() if isinstance(spec, dict) else ""
        if family_id:
            counts[family_id] += 1
    return counts


def load_binding_counts(path: Path) -> Counter[str]:
    if not path.exists():
        raise HypothesisFamilyRoleClassificationError(f"missing runtime binding artifact: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if str(payload.get("schema_version", "")) != "candidate_strategy_runtime_binding_v0":
        raise HypothesisFamilyRoleClassificationError(f"unexpected runtime binding schema: {path}")
    counts: Counter[str] = Counter()
    for item in payload.get("items", []):
        family_id = str(item.get("family_id", "")).strip()
        status = str(item.get("runtime_binding_status", "")).strip()
        if family_id and status == "BOUND_SHADOW_RUNNABLE":
            counts[family_id] += 1
    return counts


def build_payload(
    readme_path: Path,
    hypotheses_dir: Path,
    candidate_review_tsv_path: Path,
    candidate_strategy_contract_path: Path,
    candidate_runtime_binding_path: Path,
) -> dict[str, Any]:
    family_ids = read_family_ids_from_readme(readme_path)
    repo_family_ids = discover_repo_family_ids(hypotheses_dir)
    extra_repo_family_ids = [family_id for family_id in repo_family_ids if family_id not in family_ids]
    missing_classifications = [family_id for family_id in family_ids if family_id not in CLASSIFICATION]
    if missing_classifications:
        raise HypothesisFamilyRoleClassificationError(
            f"missing classification for families: {','.join(sorted(missing_classifications))}"
        )
    rows = read_candidate_rows(candidate_review_tsv_path)
    coverage = load_candidate_coverage(rows)
    translated_counts = load_translation_counts(candidate_strategy_contract_path)
    bound_counts = load_binding_counts(candidate_runtime_binding_path)

    items: list[dict[str, Any]] = []
    role_counts: Counter[str] = Counter()
    for family_id in family_ids:
        meta = CLASSIFICATION[family_id]
        cov = coverage.get(
            family_id,
            {"candidate_report_count": 0, "candidate_pack_ids": set(), "sample_candidate_report_paths": []},
        )
        translated = int(translated_counts.get(family_id, 0))
        bound = int(bound_counts.get(family_id, 0))
        role = str(meta["role"])
        role_counts[role] += 1
        support_only_now = role == CONTEXT_GUARD
        paper_execution_ready_now = False
        not_ready_now = (not support_only_now) and (not paper_execution_ready_now)
        item = {
            "family_id": family_id,
            "role": role,
            "rationale": str(meta["rationale"]),
            "hypothesis_source_paths": HYPOTHESIS_SOURCE_PATHS.get(family_id, []),
            "sample_candidate_report_paths": list(cov["sample_candidate_report_paths"]),
            "evidence_paths": HYPOTHESIS_SOURCE_PATHS.get(family_id, [])
            + list(cov["sample_candidate_report_paths"]),
            "candidate_report_count": int(cov["candidate_report_count"]),
            "candidate_pack_count": len(cov["candidate_pack_ids"]),
            "translated_spec_count": translated,
            "bound_shadow_runnable_count": bound,
            "strategy_translatable_now": translated > 0,
            "runtime_bindable_now": bound > 0,
            "paper_execution_ready_now": paper_execution_ready_now,
            "support_only_now": support_only_now,
            "not_ready_now": not_ready_now,
            "strategy_translation_eligibility": str(meta["strategy_translation_eligibility"]),
            "runtime_binding_eligibility": str(meta["runtime_binding_eligibility"]),
            "paper_execution_eligibility": str(meta["paper_execution_eligibility"]),
            "next_path": str(meta["next_path"]),
        }
        items.append(item)

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now(),
        "source_hypotheses_readme": str(readme_path),
        "source_candidate_review_tsv": str(candidate_review_tsv_path),
        "source_candidate_strategy_contract_json": str(candidate_strategy_contract_path),
        "source_candidate_strategy_runtime_binding_json": str(candidate_runtime_binding_path),
        "source_candidate_row_count": len(rows),
        "current_multi_hypothesis_family_count": len(family_ids),
        "current_multi_hypothesis_family_ids": family_ids,
        "repo_extra_hypothesis_ids": extra_repo_family_ids,
        "repo_reality_note": (
            "Current repo reality contains eight family ids in tools/hypotheses/README.md; "
            "latency_leadlag_v1 exists in repo code but is outside the current multi-hypothesis contract and candidate pack surface."
        ),
        "role_bucket_definitions": ROLE_BUCKET_DEFINITIONS,
        "role_counts": dict(role_counts),
        "items": items,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_payload(
        readme_path=Path(args.hypotheses_readme),
        hypotheses_dir=Path(args.hypotheses_dir),
        candidate_review_tsv_path=Path(args.candidate_review_tsv),
        candidate_strategy_contract_path=Path(args.candidate_strategy_contract_json),
        candidate_runtime_binding_path=Path(args.candidate_strategy_runtime_binding_json),
    )
    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(f"family_count={payload['current_multi_hypothesis_family_count']}")
    print(f"role_counts={json.dumps(payload['role_counts'], sort_keys=True)}")
    print(f"repo_extra_hypothesis_ids={','.join(payload['repo_extra_hypothesis_ids'])}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HypothesisFamilyRoleClassificationError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
