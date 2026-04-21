#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATE_REVIEW_TSV = ROOT / "tools" / "phase6_state" / "candidate_review_v2.tsv"
DEFAULT_ROLE_CLASSIFICATION_JSON = ROOT / "tools" / "phase6_state" / "hypothesis_family_role_classification_v0.json"
DEFAULT_OUT_JSON = ROOT / "tools" / "phase6_state" / "directional_expectancy_contract_v0.json"
SCHEMA_VERSION = "directional_expectancy_contract_v0"
STRATEGY_SPEC_VERSION = "candidate_strategy_spec_v0"
TRANSLATABLE = "TRANSLATABLE"
PRIMARY_DIRECTIONAL_ROLE = "PRIMARY_DIRECTIONAL"
REQUIRED_REVIEW_COLUMNS = {
    "rank",
    "class_priority",
    "review_class",
    "score",
    "decision_tier",
    "pack_id",
    "pack_path",
    "trade_surface_bucket",
}

FAMILY_REPORT_FILENAMES = {
    "momentum_v1": "family_momentum_report.json",
    "return_reversal_v1": "family_return_reversal_report.json",
    "jump_reversion_v1": "family_jump_reversion_report.json",
    "family_b_simple_momentum": "family_B_report.json",
}


class DirectionalExpectancyContractError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise DirectionalExpectancyContractError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Directional expectancy contract v0")
    parser.add_argument("--candidate-review-tsv", default=str(DEFAULT_CANDIDATE_REVIEW_TSV))
    parser.add_argument("--role-classification-json", default=str(DEFAULT_ROLE_CLASSIFICATION_JSON))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    return parser.parse_args(argv)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"{label}_missing:{path}")
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"{label}_not_object:{path}")
    return obj


def load_candidate_review_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        fail(f"candidate_review_tsv_missing:{path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_REVIEW_COLUMNS - fieldnames)
        if missing:
            fail(f"candidate_review_tsv_missing_columns:{','.join(missing)}")
        return [{str(key): str(value or "") for key, value in row.items()} for row in reader]


def as_int(raw: str) -> int:
    try:
        return int(str(raw or "").strip())
    except ValueError as exc:
        fail(f"invalid_int:{raw}:{exc}")


def as_float(raw: str) -> float:
    try:
        return float(str(raw or "").strip())
    except ValueError as exc:
        fail(f"invalid_float:{raw}:{exc}")


def contract_row_id_for(family_id: str, pack_id: str, selected_symbol: str) -> str:
    return f"directional_expectancy_contract::{family_id}::{pack_id}::{selected_symbol}"


def load_primary_directional_families(path: Path) -> set[str]:
    obj = load_json(path, "role_classification_json")
    items = obj.get("families")
    if not isinstance(items, list):
        items = obj.get("items")
    if not isinstance(items, list):
        fail(f"role_classification_items_invalid:{path}")
    out: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        family_id = str(item.get("family_id") or "").strip()
        role = str(item.get("role") or "").strip()
        if family_id and role == PRIMARY_DIRECTIONAL_ROLE:
            out.add(family_id)
    return out


def selected_symbols_from_plan(plan: dict[str, Any]) -> list[str]:
    raw = plan.get("selected_symbols")
    if not isinstance(raw, list):
        return []
    selected: list[str] = []
    seen: set[str] = set()
    for value in raw:
        symbol = str(value or "").strip().lower()
        if not symbol or symbol in seen:
            continue
        selected.append(symbol)
        seen.add(symbol)
    return selected


def load_campaign_plan(pack_path: Path) -> dict[str, Any]:
    plan_path = pack_path / "campaign_plan.json"
    return load_json(plan_path, "campaign_plan")


def load_report(path: Path) -> dict[str, Any] | None:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def report_passes(report: dict[str, Any], family_id: str, symbol: str) -> bool:
    if str(report.get("family_id") or "").strip() != family_id:
        return False
    status = str(report.get("status") or "").strip().lower()
    if status not in {"", "ok"}:
        return False
    if str(report.get("symbol") or "").strip().lower() != symbol:
        return False
    result_obj = report.get("result")
    if not isinstance(result_obj, dict):
        return False
    if result_obj.get("pass_signal") is not True:
        return False
    if family_id == "family_b_simple_momentum":
        return True
    return isinstance(result_obj.get("selected_cell"), dict)


def selected_cell_for_report(report: dict[str, Any], family_id: str) -> dict[str, Any]:
    result_obj = report.get("result") if isinstance(report.get("result"), dict) else {}
    if family_id != "family_b_simple_momentum":
        selected_cell = result_obj.get("selected_cell")
        if not isinstance(selected_cell, dict):
            fail(f"report_selected_cell_invalid:{family_id}")
        return selected_cell

    params = report.get("params") if isinstance(report.get("params"), dict) else {}
    lookback_minutes = params.get("lookback_minutes")
    forward_minutes = params.get("forward_minutes")
    signal_support = result_obj.get("signal_support")
    lookback_quantile_threshold = result_obj.get("lookback_quantile_threshold")
    mean_forward_return = result_obj.get("mean_forward_return")
    t_stat = result_obj.get("t_stat")
    if lookback_minutes is None or forward_minutes is None:
        fail("family_b_simple_momentum_params_missing")
    return {
        "exchange": report.get("exchange"),
        "stream": report.get("stream"),
        "symbol": report.get("symbol"),
        "lookback_minutes": lookback_minutes,
        "forward_minutes": forward_minutes,
        "delta_ms": int(lookback_minutes) * 60 * 1000,
        "h_ms": int(forward_minutes) * 60 * 1000,
        "event_count": signal_support,
        "signal_support": signal_support,
        "lookback_quantile_threshold": lookback_quantile_threshold,
        "mean_forward_return": mean_forward_return,
        "t_stat": t_stat,
    }


def build_item(
    *,
    row: dict[str, str],
    family_id: str,
    report_path: Path,
    report_obj: dict[str, Any],
    selected_symbol: str,
) -> dict[str, Any]:
    pack_id = str(row["pack_id"]).strip()
    selected_cell = selected_cell_for_report(report_obj, family_id)
    params = report_obj.get("params") if isinstance(report_obj.get("params"), dict) else {}
    source_review_rank = as_int(row["rank"])
    source_review_class_priority = as_int(row["class_priority"])
    source_review_score = as_float(row["score"])
    contract_row_id = contract_row_id_for(family_id, pack_id, selected_symbol)
    strategy_id = f"candidate_strategy::{family_id}::{pack_id}::{selected_symbol}"
    return {
        "rank": source_review_rank,
        "source_review_rank": source_review_rank,
        "source_review_class": str(row["review_class"]).strip(),
        "source_review_class_priority": source_review_class_priority,
        "source_review_score": source_review_score,
        "pack_id": pack_id,
        "source_pack_id": pack_id,
        "pack_path": str(row["pack_path"]).strip(),
        "decision_tier": str(row["decision_tier"]).strip(),
        "contract_row_id": contract_row_id,
        "selected_symbol": selected_symbol,
        "selected_family_id": family_id,
        "selected_family_report_path": str(report_path),
        "translation_status": TRANSLATABLE,
        "reject_reason": "",
        "family_metric_snapshot": clone_family_metric_snapshot(family_id, selected_cell),
        "strategy_spec": {
            "strategy_spec_version": STRATEGY_SPEC_VERSION,
            "strategy_id": strategy_id,
            "source_pack_id": pack_id,
            "source_pack_path": str(row["pack_path"]).strip(),
            "source_contract_row_id": contract_row_id,
            "source_decision_tier": str(row["decision_tier"]).strip(),
            "source_selected_symbol": selected_symbol,
            "source_review_rank": source_review_rank,
            "source_review_class": str(row["review_class"]).strip(),
            "source_review_class_priority": source_review_class_priority,
            "source_review_score": source_review_score,
            "family_id": family_id,
            "exchange": str(report_obj.get("exchange") or "").strip().lower(),
            "stream": str(report_obj.get("stream") or "").strip().lower(),
            "symbols": [selected_symbol],
            "activation_mode": "SPEC_ONLY",
            "runtime_binding_status": "UNBOUND",
            "source_family_report_path": str(report_path),
            "strategy_params": {
                "window": str(report_obj.get("window") or "").strip() or None,
                "params": params,
                "selected_cell": selected_cell,
            },
        },
    }


def clone_family_metric_snapshot(family_id: str, selected_cell: dict[str, Any]) -> dict[str, Any]:
    snapshot = {
        "family_id": family_id,
        "symbol": str(selected_cell.get("symbol") or "").strip().lower(),
    }
    for key in (
        "delta_ms",
        "h_ms",
        "event_count",
        "mean_product",
        "t_stat",
        "jump_thresh_bps",
        "jump_count",
        "mean_signed_reversal",
        "lookback_minutes",
        "forward_minutes",
        "signal_support",
        "lookback_quantile_threshold",
        "mean_forward_return",
    ):
        if key in selected_cell:
            snapshot[key] = selected_cell[key]
    return snapshot


def eligible_review_row(row: dict[str, str]) -> bool:
    decision_tier = str(row.get("decision_tier") or "").strip()
    if decision_tier not in {"PROMOTE", "PROMOTE_STRONG"}:
        return False
    if str(row.get("trade_surface_bucket") or "").strip() != "RUNNABLE_DIRECTIONAL":
        return False
    pack_id = str(row.get("pack_id") or "").strip()
    return "-binance-trade-" in pack_id


def expand_review_row(row: dict[str, str], primary_directional_families: set[str]) -> list[dict[str, Any]]:
    pack_path = Path(str(row.get("pack_path") or "").strip())
    if not pack_path.exists():
        return []
    plan = load_campaign_plan(pack_path)
    selected_symbols = selected_symbols_from_plan(plan)
    items: list[dict[str, Any]] = []
    for symbol in selected_symbols:
        report_dir = pack_path / "runs" / symbol / "artifacts" / "multi_hypothesis"
        for family_id, filename in FAMILY_REPORT_FILENAMES.items():
            if family_id not in primary_directional_families:
                continue
            report_path = report_dir / filename
            report_obj = load_report(report_path)
            if report_obj is None or not report_passes(report_obj, family_id, symbol):
                continue
            items.append(
                build_item(
                    row=row,
                    family_id=family_id,
                    report_path=report_path,
                    report_obj=report_obj,
                    selected_symbol=symbol,
                )
            )
    return items


def build_payload(
    candidate_review_tsv: Path,
    role_classification_json: Path,
    source_rows: list[dict[str, str]],
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    counts_by_family = Counter(str(item.get("selected_family_id") or "").strip() for item in items)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_candidate_review_tsv": str(candidate_review_tsv),
        "source_role_classification_json": str(role_classification_json),
        "source_candidate_review_row_count": len(source_rows),
        "eligible_review_row_count": sum(1 for row in source_rows if eligible_review_row(row)),
        "source_row_count": len(items),
        "translatable_count": len(items),
        "family_counts": dict(sorted(counts_by_family.items())),
        "items": items,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    candidate_review_tsv = Path(args.candidate_review_tsv).resolve()
    role_classification_json = Path(args.role_classification_json).resolve()
    out_json = Path(args.out_json).resolve()

    primary_directional_families = load_primary_directional_families(role_classification_json)
    source_rows = load_candidate_review_rows(candidate_review_tsv)
    items: list[dict[str, Any]] = []
    for row in source_rows:
        if not eligible_review_row(row):
            continue
        items.extend(expand_review_row(row, primary_directional_families))
    payload = build_payload(candidate_review_tsv, role_classification_json, source_rows, items)
    write_json(out_json, payload)
    print(f"directional_expectancy_contract_json={out_json}")
    print(f"source_candidate_review_row_count={payload['source_candidate_review_row_count']}")
    print(f"eligible_review_row_count={payload['eligible_review_row_count']}")
    print(f"source_row_count={payload['source_row_count']}")
    print(f"translatable_count={payload['translatable_count']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DirectionalExpectancyContractError as exc:
        print(f"DIRECTIONAL_EXPECTANCY_CONTRACT_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
