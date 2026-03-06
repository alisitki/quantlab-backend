#!/usr/bin/env python3
"""Phase-6 promotion guards v2: context-aware + tiered promotion decisions."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from phase6_promotion_guards_v0 import GuardResult, PackContractMismatch, discover_pack_contract, eval_g1_evidence, eval_g3_resource
    from phase6_promotion_guards_v1 import (
        DEFAULT_POLICY_FILENAME,
        DEFAULT_STATE_DIR,
        INDEX_FILENAME,
        RECORDS_FILENAME,
        canonical_policy_hash,
        determine_pack_id,
        ensure_state_files,
        eval_g2_determinism_policy,
        load_policy,
        read_jsonl_records,
    )
except ImportError:  # pragma: no cover - module import path fallback
    from tools.phase6_promotion_guards_v0 import GuardResult, PackContractMismatch, discover_pack_contract, eval_g1_evidence, eval_g3_resource
    from tools.phase6_promotion_guards_v1 import (
        DEFAULT_POLICY_FILENAME,
        DEFAULT_STATE_DIR,
        INDEX_FILENAME,
        RECORDS_FILENAME,
        canonical_policy_hash,
        determine_pack_id,
        ensure_state_files,
        eval_g2_determinism_policy,
        load_policy,
        read_jsonl_records,
    )


DEFAULT_CONTEXT_POLICY_FILENAME = "context_policy_v2.json"
REQUIRED_CONTEXT_POLICY_KEYS = {
    "base_decision_if_v1_pass",
    "strong_promotion_requires",
    "hold_conditions",
    "unsupported_oi_behavior",
    "absent_context_behavior",
}
REQUIRED_CONTEXT_COLUMNS = {
    "exchange",
    "symbol",
    "core_stream",
    "ctx_mark_price_status",
    "ctx_mark_trade_basis_max_abs_bps",
    "ctx_funding_status",
    "ctx_funding_mean",
    "ctx_oi_status",
    "ctx_oi_change_pct",
}
DECISION_PROMOTE_STRONG = "PROMOTE_STRONG"
DECISION_PROMOTE = "PROMOTE"
DECISION_HOLD = "HOLD"
STATUS_OK = "OK"
STATUS_ABSENT = "ABSENT"
STATUS_UNSUPPORTED_EXCHANGE = "UNSUPPORTED_EXCHANGE"
NEUTRAL_BEHAVIOR = "NEUTRAL"
HOLD_BEHAVIOR = "HOLD"
PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"
SKIPPED = "SKIPPED"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase-6 promotion guards v2 (context-aware, advisory only)")
    p.add_argument("--pack", required=True, help="Archive pack directory")
    p.add_argument("--policy", default="", help="Base v1 policy JSON path")
    p.add_argument("--context-policy", default="", help="Context policy JSON path")
    p.add_argument("--state-dir", default="", help="State dir (default: tools/phase6_state)")
    p.add_argument("--out-dir", default="", help="Default: <pack>/guards")
    return p.parse_args(argv)


def format_float(value: float) -> str:
    return f"{float(value):.15f}"


def format_optional_float(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    return format_float(value)


def parse_optional_float(raw: str) -> Optional[float]:
    value = str(raw or "").strip()
    if not value or value.upper() == "NA":
        return None
    return float(value)


def normalize_name(raw: str) -> str:
    return str(raw or "").strip().lower()


def load_context_policy(policy_path: Path) -> Tuple[Dict[str, Any], str]:
    if not policy_path.exists():
        raise RuntimeError(f"context_policy_missing:{policy_path}")
    try:
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"context_policy_invalid_json:{policy_path}:{exc}") from exc
    if not isinstance(policy, dict):
        raise RuntimeError(f"context_policy_not_object:{policy_path}")

    missing = sorted(REQUIRED_CONTEXT_POLICY_KEYS - set(policy.keys()))
    if missing:
        raise RuntimeError(f"context_policy_missing_keys:{','.join(missing)}")

    strong = policy.get("strong_promotion_requires")
    hold = policy.get("hold_conditions")
    if not isinstance(strong, dict):
        raise RuntimeError("context_policy_invalid:strong_promotion_requires must be object")
    if not isinstance(hold, dict):
        raise RuntimeError("context_policy_invalid:hold_conditions must be object")

    required_strong = {
        "max_mark_trade_basis_max_abs_bps",
        "max_abs_funding_mean",
        "oi_confirmation_required_exchanges",
    }
    required_hold = {
        "max_mark_trade_basis_max_abs_bps",
        "max_abs_funding_mean",
        "min_oi_change_pct_confirm",
    }
    strong_missing = sorted(required_strong - set(strong.keys()))
    hold_missing = sorted(required_hold - set(hold.keys()))
    if strong_missing:
        raise RuntimeError(f"context_policy_missing_strong_keys:{','.join(strong_missing)}")
    if hold_missing:
        raise RuntimeError(f"context_policy_missing_hold_keys:{','.join(hold_missing)}")
    if not isinstance(strong.get("oi_confirmation_required_exchanges"), list):
        raise RuntimeError("context_policy_invalid:oi_confirmation_required_exchanges must be list")

    normalized = {
        "base_decision_if_v1_pass": str(policy["base_decision_if_v1_pass"]).strip().upper(),
        "strong_promotion_requires": {
            "max_mark_trade_basis_max_abs_bps": float(strong["max_mark_trade_basis_max_abs_bps"]),
            "max_abs_funding_mean": float(strong["max_abs_funding_mean"]),
            "oi_confirmation_required_exchanges": sorted(
                normalize_name(v) for v in strong["oi_confirmation_required_exchanges"] if str(v).strip()
            ),
        },
        "hold_conditions": {
            "max_mark_trade_basis_max_abs_bps": float(hold["max_mark_trade_basis_max_abs_bps"]),
            "max_abs_funding_mean": float(hold["max_abs_funding_mean"]),
            "min_oi_change_pct_confirm": float(hold["min_oi_change_pct_confirm"]),
        },
        "unsupported_oi_behavior": str(policy["unsupported_oi_behavior"]).strip().upper(),
        "absent_context_behavior": str(policy["absent_context_behavior"]).strip().upper(),
    }
    if normalized["base_decision_if_v1_pass"] != DECISION_PROMOTE:
        raise RuntimeError("context_policy_invalid:base_decision_if_v1_pass must be PROMOTE")
    if normalized["unsupported_oi_behavior"] not in {NEUTRAL_BEHAVIOR, HOLD_BEHAVIOR}:
        raise RuntimeError("context_policy_invalid:unsupported_oi_behavior must be NEUTRAL or HOLD")
    if normalized["absent_context_behavior"] not in {NEUTRAL_BEHAVIOR, HOLD_BEHAVIOR}:
        raise RuntimeError("context_policy_invalid:absent_context_behavior must be NEUTRAL or HOLD")
    return normalized, canonical_policy_hash(normalized)


def discover_context_summary_paths(pack: Path) -> List[Path]:
    expected = sorted(
        p for p in pack.glob("runs/*/artifacts/context/context_summary.tsv") if p.is_file()
    )
    found = sorted(p for p in pack.glob("**/context_summary.tsv") if p.is_file())
    if expected:
        return expected
    if found:
        raise PackContractMismatch(
            expected=["runs/*/artifacts/context/context_summary.tsv"],
            found=[str(p) for p in found[:50]],
            detail="context_summary_found_outside_expected_layout",
        )
    return []


def load_context_rows(paths: List[Path]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for path in sorted(paths, key=lambda p: str(p)):
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            fieldnames = set(reader.fieldnames or [])
            missing = sorted(REQUIRED_CONTEXT_COLUMNS - fieldnames)
            if missing:
                raise PackContractMismatch(
                    expected=[f"context_summary columns include {','.join(sorted(REQUIRED_CONTEXT_COLUMNS))}"],
                    found=[f"context_summary columns={','.join(sorted(fieldnames))}"],
                    detail=f"context_summary_missing_columns={','.join(missing)}",
                )
            file_rows = list(reader)
        if not file_rows:
            raise PackContractMismatch(
                expected=["context_summary.tsv with one data row"],
                found=[str(path)],
                detail="context_summary_missing_rows",
            )
        for row in file_rows:
            copied = {str(k): str(v or "") for k, v in row.items()}
            copied["_source_path"] = str(path)
            rows.append(copied)
    return rows


def absent_guard(
    *,
    guard_id: str,
    observed: str,
    threshold: str,
    detail: str,
    behavior: str,
) -> GuardResult:
    status = FAIL if behavior == HOLD_BEHAVIOR else WARN
    return GuardResult(
        guard_id=guard_id,
        status=status,
        observed=observed,
        threshold=threshold,
        detail=detail,
    )


def skipped_context_guard(guard_id: str) -> Tuple[GuardResult, Dict[str, Any]]:
    return (
        GuardResult(
            guard_id=guard_id,
            status=SKIPPED,
            observed="base_v1_decision=HOLD",
            threshold="base_v1_decision=PROMOTE",
            detail="context guard skipped because v1 decision is HOLD",
        ),
        {"skipped_due_to_base_v1_hold": True},
    )


def eval_g4_mark_context(rows: List[Dict[str, str]], policy: Dict[str, Any]) -> Tuple[GuardResult, Dict[str, Any]]:
    strong_limit = float(policy["strong_promotion_requires"]["max_mark_trade_basis_max_abs_bps"])
    hold_limit = float(policy["hold_conditions"]["max_mark_trade_basis_max_abs_bps"])
    absent_behavior = str(policy["absent_context_behavior"])
    trade_rows = [row for row in rows if normalize_name(row.get("core_stream", "")) == "trade"]
    relevant = [row for row in trade_rows if normalize_name(row.get("ctx_mark_price_status", "")) == normalize_name(STATUS_OK)]
    absent_rows = [row for row in trade_rows if normalize_name(row.get("ctx_mark_price_status", "")) == normalize_name(STATUS_ABSENT)]
    basis_values: List[float] = []
    basis_na_count = 0
    for row in relevant:
        basis_value = parse_optional_float(row.get("ctx_mark_trade_basis_max_abs_bps", ""))
        if basis_value is None:
            basis_na_count += 1
            continue
        basis_values.append(abs(basis_value))

    observed = (
        f"trade_rows={len(trade_rows)};ok_rows={len(relevant)};absent_rows={len(absent_rows)};"
        f"basis_na_rows={basis_na_count};max_basis_max_abs_bps={format_optional_float(max(basis_values) if basis_values else None)}"
    )
    threshold = (
        f"strong<={format_float(strong_limit)};hold<={format_float(hold_limit)}"
    )
    detail_bits: List[str] = []
    if not trade_rows:
        result = GuardResult(
            guard_id="G4_MARK_CONTEXT",
            status=PASS,
            observed=observed,
            threshold=threshold,
            detail="mark/trade basis not applicable because no trade-core context rows were found",
        )
        return result, {
            "trade_row_count": 0,
            "ok_row_count": 0,
            "absent_row_count": 0,
            "basis_na_row_count": 0,
            "max_mark_trade_basis_max_abs_bps": None,
            "not_applicable": True,
        }

    if absent_rows:
        detail_bits.append("one_or_more_trade_rows_have_absent_mark_context")
    if basis_na_count:
        detail_bits.append("one_or_more_trade_rows_have_na_basis")

    if basis_values and max(basis_values) > hold_limit:
        result = GuardResult(
            guard_id="G4_MARK_CONTEXT",
            status=FAIL,
            observed=observed,
            threshold=threshold,
            detail="max mark/trade basis exceeds hold threshold",
        )
    elif not basis_values:
        result = absent_guard(
            guard_id="G4_MARK_CONTEXT",
            observed=observed,
            threshold=threshold,
            detail="mark/trade basis unavailable for trade rows",
            behavior=absent_behavior,
        )
    elif absent_rows:
        result = absent_guard(
            guard_id="G4_MARK_CONTEXT",
            observed=observed,
            threshold=threshold,
            detail="trade rows missing mark context",
            behavior=absent_behavior,
        )
    elif max(basis_values) <= strong_limit:
        result = GuardResult(
            guard_id="G4_MARK_CONTEXT",
            status=PASS,
            observed=observed,
            threshold=threshold,
            detail="max mark/trade basis satisfies strong threshold",
        )
    else:
        result = GuardResult(
            guard_id="G4_MARK_CONTEXT",
            status=WARN,
            observed=observed,
            threshold=threshold,
            detail="max mark/trade basis exceeds strong threshold but not hold threshold",
        )

    if detail_bits and result.detail:
        result = GuardResult(
            guard_id=result.guard_id,
            status=result.status,
            observed=result.observed,
            threshold=result.threshold,
            detail=result.detail + "; " + "; ".join(sorted(detail_bits)),
        )

    return result, {
        "trade_row_count": len(trade_rows),
        "ok_row_count": len(relevant),
        "absent_row_count": len(absent_rows),
        "basis_na_row_count": basis_na_count,
        "max_mark_trade_basis_max_abs_bps": max(basis_values) if basis_values else None,
        "not_applicable": False,
    }


def eval_g5_funding_context(rows: List[Dict[str, str]], policy: Dict[str, Any]) -> Tuple[GuardResult, Dict[str, Any]]:
    strong_limit = float(policy["strong_promotion_requires"]["max_abs_funding_mean"])
    hold_limit = float(policy["hold_conditions"]["max_abs_funding_mean"])
    absent_behavior = str(policy["absent_context_behavior"])
    ok_rows = [row for row in rows if normalize_name(row.get("ctx_funding_status", "")) == normalize_name(STATUS_OK)]
    absent_rows = [row for row in rows if normalize_name(row.get("ctx_funding_status", "")) == normalize_name(STATUS_ABSENT)]
    funding_values: List[float] = []
    funding_na_count = 0
    for row in ok_rows:
        mean_value = parse_optional_float(row.get("ctx_funding_mean", ""))
        if mean_value is None:
            funding_na_count += 1
            continue
        funding_values.append(abs(mean_value))

    observed = (
        f"rows={len(rows)};ok_rows={len(ok_rows)};absent_rows={len(absent_rows)};"
        f"mean_na_rows={funding_na_count};max_abs_funding_mean={format_optional_float(max(funding_values) if funding_values else None)}"
    )
    threshold = (
        f"strong<={format_float(strong_limit)};hold<={format_float(hold_limit)}"
    )
    if not rows:
        result = absent_guard(
            guard_id="G5_FUNDING_CONTEXT",
            observed=observed,
            threshold=threshold,
            detail="no context_summary rows found in pack",
            behavior=absent_behavior,
        )
    elif funding_values and max(funding_values) > hold_limit:
        result = GuardResult(
            guard_id="G5_FUNDING_CONTEXT",
            status=FAIL,
            observed=observed,
            threshold=threshold,
            detail="max abs funding mean exceeds hold threshold",
        )
    elif not funding_values:
        result = absent_guard(
            guard_id="G5_FUNDING_CONTEXT",
            observed=observed,
            threshold=threshold,
            detail="funding mean unavailable across pack context rows",
            behavior=absent_behavior,
        )
    elif absent_rows:
        result = absent_guard(
            guard_id="G5_FUNDING_CONTEXT",
            observed=observed,
            threshold=threshold,
            detail="one or more rows have absent funding context",
            behavior=absent_behavior,
        )
    elif max(funding_values) <= strong_limit:
        result = GuardResult(
            guard_id="G5_FUNDING_CONTEXT",
            status=PASS,
            observed=observed,
            threshold=threshold,
            detail="max abs funding mean satisfies strong threshold",
        )
    else:
        result = GuardResult(
            guard_id="G5_FUNDING_CONTEXT",
            status=WARN,
            observed=observed,
            threshold=threshold,
            detail="max abs funding mean exceeds strong threshold but not hold threshold",
        )
    return result, {
        "row_count": len(rows),
        "ok_row_count": len(ok_rows),
        "absent_row_count": len(absent_rows),
        "mean_na_row_count": funding_na_count,
        "max_abs_funding_mean": max(funding_values) if funding_values else None,
    }


def eval_g6_oi_context(rows: List[Dict[str, str]], policy: Dict[str, Any]) -> Tuple[GuardResult, Dict[str, Any]]:
    required_exchanges = set(policy["strong_promotion_requires"]["oi_confirmation_required_exchanges"])
    hold_threshold = float(policy["hold_conditions"]["min_oi_change_pct_confirm"])
    absent_behavior = str(policy["absent_context_behavior"])
    unsupported_behavior = str(policy["unsupported_oi_behavior"])
    required_rows = [row for row in rows if normalize_name(row.get("exchange", "")) in required_exchanges]
    ok_rows = [row for row in required_rows if row.get("ctx_oi_status", "") == STATUS_OK]
    absent_rows = [row for row in required_rows if row.get("ctx_oi_status", "") == STATUS_ABSENT]
    unsupported_rows = [
        row for row in required_rows if row.get("ctx_oi_status", "") == STATUS_UNSUPPORTED_EXCHANGE
    ]
    oi_values: List[float] = []
    oi_na_count = 0
    for row in ok_rows:
        oi_change = parse_optional_float(row.get("ctx_oi_change_pct", ""))
        if oi_change is None:
            oi_na_count += 1
            continue
        oi_values.append(oi_change)

    observed = (
        f"required_rows={len(required_rows)};ok_rows={len(ok_rows)};absent_rows={len(absent_rows)};"
        f"unsupported_rows={len(unsupported_rows)};oi_na_rows={oi_na_count};"
        f"min_oi_change_pct={format_optional_float(min(oi_values) if oi_values else None)}"
    )
    threshold = f"strong_and_hold>={format_float(hold_threshold)}"

    if not required_rows:
        result = GuardResult(
            guard_id="G6_OI_CONTEXT",
            status=PASS,
            observed=observed,
            threshold=threshold,
            detail="OI confirmation not required for exchanges observed in pack context",
        )
        return result, {
            "required_row_count": 0,
            "ok_row_count": 0,
            "absent_row_count": 0,
            "unsupported_row_count": 0,
            "oi_na_row_count": 0,
            "min_oi_change_pct": None,
            "required_exchanges": sorted(required_exchanges),
        }

    if oi_values and min(oi_values) < hold_threshold:
        result = GuardResult(
            guard_id="G6_OI_CONTEXT",
            status=FAIL,
            observed=observed,
            threshold=threshold,
            detail="minimum OI change confirmation is below hold threshold",
        )
    elif unsupported_rows and unsupported_behavior == HOLD_BEHAVIOR:
        result = GuardResult(
            guard_id="G6_OI_CONTEXT",
            status=FAIL,
            observed=observed,
            threshold=threshold,
            detail="required OI rows are unsupported and policy requires HOLD",
        )
    elif absent_rows and absent_behavior == HOLD_BEHAVIOR:
        result = GuardResult(
            guard_id="G6_OI_CONTEXT",
            status=FAIL,
            observed=observed,
            threshold=threshold,
            detail="required OI rows are absent and policy requires HOLD",
        )
    elif not oi_values:
        result = absent_guard(
            guard_id="G6_OI_CONTEXT",
            observed=observed,
            threshold=threshold,
            detail="required OI confirmation unavailable",
            behavior=absent_behavior,
        )
    elif absent_rows:
        result = absent_guard(
            guard_id="G6_OI_CONTEXT",
            observed=observed,
            threshold=threshold,
            detail="one or more required OI rows are absent",
            behavior=absent_behavior,
        )
    elif unsupported_rows:
        if unsupported_behavior == HOLD_BEHAVIOR:
            result = GuardResult(
                guard_id="G6_OI_CONTEXT",
                status=FAIL,
                observed=observed,
                threshold=threshold,
                detail="required OI rows are unsupported and policy requires HOLD",
            )
        else:
            result = GuardResult(
                guard_id="G6_OI_CONTEXT",
                status=WARN,
                observed=observed,
                threshold=threshold,
                detail="required OI rows unsupported but policy treats them as neutral",
            )
    elif min(oi_values) >= hold_threshold:
        result = GuardResult(
            guard_id="G6_OI_CONTEXT",
            status=PASS,
            observed=observed,
            threshold=threshold,
            detail="minimum OI change confirmation satisfies threshold",
        )
    else:
        result = GuardResult(
            guard_id="G6_OI_CONTEXT",
            status=WARN,
            observed=observed,
            threshold=threshold,
            detail="minimum OI change confirmation does not support strong promotion",
        )

    return result, {
        "required_row_count": len(required_rows),
        "ok_row_count": len(ok_rows),
        "absent_row_count": len(absent_rows),
        "unsupported_row_count": len(unsupported_rows),
        "oi_na_row_count": oi_na_count,
        "min_oi_change_pct": min(oi_values) if oi_values else None,
        "required_exchanges": sorted(required_exchanges),
    }


def write_reports_v2(
    out_dir: Path,
    pack: Path,
    final_decision: str,
    guard_results: List[GuardResult],
    details: Dict[str, Any],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    txt_path = out_dir / "decision_report.txt"
    tsv_path = out_dir / "decision_report.tsv"
    json_path = out_dir / "guard_details.json"

    fail_reasons = [f"{g.guard_id}:{g.detail}" for g in guard_results if g.status == FAIL]
    warn_reasons = [f"{g.guard_id}:{g.detail}" for g in guard_results if g.status == WARN]

    txt_lines = [
        f"decision={final_decision}",
        f"pack={pack}",
        f"out_dir={out_dir}",
        f"guard_count={len(guard_results)}",
        f"base_v1_decision={details['base_v1_decision']}",
    ]
    for g in guard_results:
        txt_lines.append(
            f"{g.guard_id}={g.status} observed=[{g.observed}] threshold=[{g.threshold}] detail=[{g.detail}]"
        )
    txt_lines.append("fail_reasons=" + ("|".join(fail_reasons) if fail_reasons else "NONE"))
    txt_lines.append("warn_reasons=" + ("|".join(warn_reasons) if warn_reasons else "NONE"))
    txt_lines.append(
        "resolved_paths="
        + "|".join(
            [
                f"sha_verify={details['resolved_paths']['sha_verify']}",
                f"campaign_meta={details['resolved_paths']['campaign_meta']}",
                f"run_summary={details['resolved_paths']['run_summary']}",
                f"determinism_count={details['resolved_paths']['determinism_count']}",
                f"context_summary_count={details['resolved_paths']['context_summary_count']}",
            ]
        )
    )
    txt_path.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")

    with tsv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["guard_id", "status", "observed", "threshold", "detail"])
        for g in guard_results:
            w.writerow([g.guard_id, g.status, g.observed, g.threshold, g.detail])
        w.writerow(
            [
                "FINAL_DECISION",
                PASS if final_decision in {DECISION_PROMOTE, DECISION_PROMOTE_STRONG} else FAIL,
                final_decision,
                "no_context_guard_failures_for_promotion",
                "advisory_only",
            ]
        )

    json_path.write_text(
        json.dumps(details, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def record_fingerprint_v2(record: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "pack_path",
        "pack_id",
        "decision",
        "decision_tier",
        "policy_hash",
        "context_policy_hash",
        "sha_tar_ok",
        "max_rss_kb",
        "max_elapsed_sec",
        "det_pass",
        "det_supported",
        "det_skipped",
        "context_metrics",
    ]
    return {k: record.get(k) for k in keys}


def decision_tier_from_record(record: Dict[str, Any]) -> str:
    value = str(record.get("decision_tier") or record.get("decision") or "").strip().upper()
    if value in {DECISION_PROMOTE_STRONG, DECISION_PROMOTE, DECISION_HOLD}:
        return value
    return DECISION_HOLD


def rebuild_index_v2(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest: Dict[str, Dict[str, Any]] = {}
    for i, rec in enumerate(records, start=1):
        pack_id = str(rec.get("pack_id", "")).strip()
        if not pack_id:
            continue
        merged = dict(rec)
        merged["_line_no"] = i
        if not merged.get("decision_tier"):
            merged["decision_tier"] = decision_tier_from_record(merged)
        latest[pack_id] = merged

    ordered_pack_ids = sorted(latest.keys())
    pack_latest = {pid: latest[pid] for pid in ordered_pack_ids}
    promote_pack_ids = sorted(
        pid for pid in ordered_pack_ids if decision_tier_from_record(pack_latest[pid]) in {DECISION_PROMOTE, DECISION_PROMOTE_STRONG}
    )
    promote_strong_pack_ids = sorted(
        pid for pid in ordered_pack_ids if decision_tier_from_record(pack_latest[pid]) == DECISION_PROMOTE_STRONG
    )
    promote_packs = sorted({str(pack_latest[pid].get("pack_path", "")) for pid in promote_pack_ids if str(pack_latest[pid].get("pack_path", "")).strip()})
    promote_strong_packs = sorted(
        {
            str(pack_latest[pid].get("pack_path", ""))
            for pid in promote_strong_pack_ids
            if str(pack_latest[pid].get("pack_path", "")).strip()
        }
    )
    return {
        "record_count": len(records),
        "pack_latest": pack_latest,
        "promote_pack_ids": promote_pack_ids,
        "promote_strong_pack_ids": promote_strong_pack_ids,
        "promote_packs": promote_packs,
        "promote_strong_packs": promote_strong_packs,
    }


def write_index(index_path: Path, index_obj: Dict[str, Any]) -> None:
    index_path.write_text(
        json.dumps(index_obj, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def append_record_if_needed_v2(
    records_path: Path,
    index_path: Path,
    candidate: Dict[str, Any],
) -> Tuple[bool, int, Dict[str, Any]]:
    records = read_jsonl_records(records_path)
    current_index = rebuild_index_v2(records)
    pack_id = candidate["pack_id"]
    latest = current_index.get("pack_latest", {}).get(pack_id)

    should_append = True
    if latest is not None and record_fingerprint_v2(latest) == record_fingerprint_v2(candidate):
        should_append = False

    if should_append:
        line = json.dumps(candidate, sort_keys=True, separators=(",", ":"))
        with records_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        records.append(candidate)

    new_index = rebuild_index_v2(records)
    write_index(index_path, new_index)
    return should_append, len(records), new_index


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    pack = Path(args.pack).resolve()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else DEFAULT_STATE_DIR
    policy_path = Path(args.policy).resolve() if args.policy else (state_dir / DEFAULT_POLICY_FILENAME)
    context_policy_path = (
        Path(args.context_policy).resolve()
        if args.context_policy
        else (state_dir / DEFAULT_CONTEXT_POLICY_FILENAME)
    )
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (pack / "guards")

    try:
        policy, policy_hash = load_policy(policy_path)
    except Exception as exc:  # noqa: BLE001
        print(f"STOP: POLICY_LOAD_FAIL detail={exc}", file=sys.stderr)
        return 2

    try:
        context_policy, context_policy_hash = load_context_policy(context_policy_path)
    except Exception as exc:  # noqa: BLE001
        print(f"STOP: CONTEXT_POLICY_LOAD_FAIL detail={exc}", file=sys.stderr)
        return 2

    records_path, index_path = ensure_state_files(state_dir)

    try:
        contract = discover_pack_contract(pack)
    except PackContractMismatch as exc:
        print("STOP: PACK_CONTRACT_MISMATCH", file=sys.stderr)
        print(f"detail={exc.detail}", file=sys.stderr)
        print("expected=", file=sys.stderr)
        for e in exc.expected:
            print(f"  - {e}", file=sys.stderr)
        print("found=", file=sys.stderr)
        for f in exc.found:
            print(f"  - {f}", file=sys.stderr)
        print(
            "minimal_adapter_plan=add path resolver for discovered determinism/run_summary layout while keeping guard semantics unchanged",
            file=sys.stderr,
        )
        return 2

    try:
        context_paths = discover_context_summary_paths(pack)
        context_rows = load_context_rows(context_paths)
    except PackContractMismatch as exc:
        print("STOP: CONTEXT_CONTRACT_MISMATCH", file=sys.stderr)
        print(f"detail={exc.detail}", file=sys.stderr)
        print("expected=", file=sys.stderr)
        for e in exc.expected:
            print(f"  - {e}", file=sys.stderr)
        print("found=", file=sys.stderr)
        for f in exc.found:
            print(f"  - {f}", file=sys.stderr)
        return 2

    g1, g1_detail = eval_g1_evidence(contract["sha_verify"])
    required_sha_ok = int(policy["require_sha_ok_lines"])
    if g1_detail.get("ok_line_count", 0) < required_sha_ok:
        g1 = GuardResult(
            guard_id="G1_EVIDENCE",
            status=FAIL,
            observed=f"ok_line_count={g1_detail.get('ok_line_count', 0)}",
            threshold=f">={required_sha_ok}",
            detail=f"path={contract['sha_verify']}",
        )

    g2, g2_detail = eval_g2_determinism_policy(
        contract["determinism_paths"],
        float(policy["pass_ratio"]),
        list(policy["exclude_statuses"]),
        list(policy["supported_statuses"]),
    )
    try:
        g3, g3_detail = eval_g3_resource(
            contract["run_summary"],
            float(policy["max_rss_kb"]),
            float(policy["max_elapsed_sec"]),
        )
    except PackContractMismatch as exc:
        print("STOP: PACK_CONTRACT_MISMATCH", file=sys.stderr)
        print(f"detail={exc.detail}", file=sys.stderr)
        print("expected=", file=sys.stderr)
        for e in exc.expected:
            print(f"  - {e}", file=sys.stderr)
        print("found=", file=sys.stderr)
        for f in exc.found:
            print(f"  - {f}", file=sys.stderr)
        print(
            "minimal_adapter_plan=align run_summary parser to actual column names while preserving G3 threshold semantics",
            file=sys.stderr,
        )
        return 2

    base_guards = [g1, g2, g3]
    base_v1_decision = (
        str(context_policy["base_decision_if_v1_pass"])
        if all(g.status == PASS for g in base_guards)
        else DECISION_HOLD
    )

    if base_v1_decision == DECISION_HOLD:
        g4, g4_detail = skipped_context_guard("G4_MARK_CONTEXT")
        g5, g5_detail = skipped_context_guard("G5_FUNDING_CONTEXT")
        g6, g6_detail = skipped_context_guard("G6_OI_CONTEXT")
        final_decision = DECISION_HOLD
    else:
        g4, g4_detail = eval_g4_mark_context(context_rows, context_policy)
        g5, g5_detail = eval_g5_funding_context(context_rows, context_policy)
        g6, g6_detail = eval_g6_oi_context(context_rows, context_policy)
        context_guards = [g4, g5, g6]
        if any(g.status == FAIL for g in context_guards):
            final_decision = DECISION_HOLD
        elif all(g.status == PASS for g in context_guards):
            final_decision = DECISION_PROMOTE_STRONG
        else:
            final_decision = DECISION_PROMOTE

    guards = [g1, g2, g3, g4, g5, g6]
    pack_id, sha_tar_ok, tar_path, tar_sha = determine_pack_id(pack, contract["sha_verify"])
    ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    context_metrics = {
        "context_row_count": len(context_rows),
        "mark_trade_row_count": int(g4_detail.get("trade_row_count", 0)),
        "mark_trade_ok_row_count": int(g4_detail.get("ok_row_count", 0)),
        "mark_trade_absent_row_count": int(g4_detail.get("absent_row_count", 0)),
        "max_mark_trade_basis_max_abs_bps": g4_detail.get("max_mark_trade_basis_max_abs_bps"),
        "funding_row_count": int(g5_detail.get("row_count", 0)),
        "funding_ok_row_count": int(g5_detail.get("ok_row_count", 0)),
        "funding_absent_row_count": int(g5_detail.get("absent_row_count", 0)),
        "max_abs_funding_mean": g5_detail.get("max_abs_funding_mean"),
        "oi_required_row_count": int(g6_detail.get("required_row_count", 0)),
        "oi_ok_row_count": int(g6_detail.get("ok_row_count", 0)),
        "oi_absent_row_count": int(g6_detail.get("absent_row_count", 0)),
        "oi_unsupported_row_count": int(g6_detail.get("unsupported_row_count", 0)),
        "min_oi_change_pct": g6_detail.get("min_oi_change_pct"),
    }

    details = {
        "advisory_only": True,
        "decision": final_decision,
        "decision_tier": final_decision,
        "base_v1_decision": base_v1_decision,
        "config": {
            "pack": str(pack),
            "out_dir": str(out_dir),
            "policy_path": str(policy_path),
            "policy_hash": policy_hash,
            "context_policy_path": str(context_policy_path),
            "context_policy_hash": context_policy_hash,
            "pass_ratio": float(policy["pass_ratio"]),
            "max_rss_kb": float(policy["max_rss_kb"]),
            "max_elapsed_sec": float(policy["max_elapsed_sec"]),
            "exclude_statuses": list(policy["exclude_statuses"]),
            "supported_statuses": list(policy["supported_statuses"]),
            "require_sha_ok_lines": int(policy["require_sha_ok_lines"]),
            "context_policy": context_policy,
        },
        "resolved_paths": {
            "sha_verify": str(contract["sha_verify"]),
            "campaign_meta": str(contract["campaign_meta"]),
            "run_summary": str(contract["run_summary"]),
            "determinism_paths": [str(p) for p in contract["determinism_paths"]],
            "determinism_count": len(contract["determinism_paths"]),
            "context_summary_paths": [str(p) for p in context_paths],
            "context_summary_count": len(context_paths),
        },
        "guards": {
            "G1_EVIDENCE": g1_detail,
            "G2_DETERMINISM": g2_detail,
            "G3_RESOURCE": g3_detail,
            "G4_MARK_CONTEXT": g4_detail,
            "G5_FUNDING_CONTEXT": g5_detail,
            "G6_OI_CONTEXT": g6_detail,
        },
        "context_aggregate": context_metrics,
        "promotion_record_preview": {
            "ts_utc": ts_utc,
            "pack_id": pack_id,
            "pack_path": str(pack),
            "decision": final_decision,
            "decision_tier": final_decision,
            "policy_hash": policy_hash,
            "context_policy_hash": context_policy_hash,
            "sha_tar_ok": sha_tar_ok,
            "tar_path": tar_path,
            "tar_sha256": tar_sha,
            "context_metrics": context_metrics,
        },
    }

    write_reports_v2(out_dir, pack, final_decision, guards, details)

    candidate_record = {
        "ts_utc": ts_utc,
        "pack_path": str(pack),
        "pack_id": pack_id,
        "decision": final_decision,
        "decision_tier": final_decision,
        "policy_hash": policy_hash,
        "context_policy_hash": context_policy_hash,
        "guards": {g.guard_id: g.status for g in guards},
        "sha_tar_ok": bool(sha_tar_ok),
        "max_rss_kb": float(g3_detail["max_rss_kb"]),
        "max_elapsed_sec": float(g3_detail["max_elapsed_sec"]),
        "det_pass": int(g2_detail["pass_count"]),
        "det_supported": int(g2_detail["supported_count"]),
        "det_skipped": int(g2_detail["skipped_unsupported_count"]),
        "context_metrics": context_metrics,
    }

    appended, record_count, index_obj = append_record_if_needed_v2(records_path, index_path, candidate_record)

    print(f"decision={final_decision}")
    print(f"report_txt={out_dir / 'decision_report.txt'}")
    print(f"report_tsv={out_dir / 'decision_report.tsv'}")
    print(f"report_json={out_dir / 'guard_details.json'}")
    print(f"state_dir={state_dir}")
    print(f"policy_path={policy_path}")
    print(f"context_policy_path={context_policy_path}")
    print(f"records_path={records_path}")
    print(f"index_path={index_path}")
    print(f"record_appended={'true' if appended else 'false'}")
    print(f"record_count={record_count}")
    print(f"pack_id={pack_id}")
    print(f"promote_packs={len(index_obj.get('promote_packs', []))}")
    print(f"promote_strong_packs={len(index_obj.get('promote_strong_packs', []))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
