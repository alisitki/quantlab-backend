#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATE_REVIEW_TSV = ROOT / "tools" / "phase6_state" / "candidate_review_v2.tsv"
DEFAULT_FAMILY_SELECTION_JSON = ROOT / "tools" / "phase6_state" / "primary_directional_family_selection_v0.json"
DEFAULT_OUT_JSON = ROOT / "tools" / "phase6_state" / "candidate_strategy_contract_v0.json"
SCHEMA_VERSION = "candidate_strategy_contract_v0"
STRATEGY_SPEC_VERSION = "candidate_strategy_spec_v0"
TRANSLATABLE = "TRANSLATABLE"
NOT_TRANSLATABLE_YET = "NOT_TRANSLATABLE_YET"
INSUFFICIENT_CONTRACT = "INSUFFICIENT_CONTRACT"
UNSUPPORTED_FAMILY = "UNSUPPORTED_FAMILY"
REQUIRED_REVIEW_COLUMNS = {"rank", "decision_tier", "pack_id", "pack_path"}


class CandidateStrategyContractError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise CandidateStrategyContractError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Candidate -> strategy contract v0 translator")
    parser.add_argument("--candidate-review-tsv", default=str(DEFAULT_CANDIDATE_REVIEW_TSV))
    parser.add_argument("--family-selection-json", default=str(DEFAULT_FAMILY_SELECTION_JSON))
    parser.add_argument("--preferred-family-id", default="")
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    return parser.parse_args(argv)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def resolve_preferred_family_id(path: Path, explicit_family_id: str) -> str:
    explicit = str(explicit_family_id or "").strip()
    if explicit:
        return explicit
    if not path.exists():
        return ""
    try:
        obj = load_json(path, "family_selection_json")
    except CandidateStrategyContractError:
        return ""
    if str(obj.get("schema_version") or "").strip() != "primary_directional_family_selection_v0":
        return ""
    return str(obj.get("selected_family_id") or "").strip()


def as_int(value: str) -> int:
    try:
        return int(str(value or "").strip())
    except ValueError as exc:
        fail(f"candidate_review_invalid_rank:{value}:{exc}")


def contract_row_id_for(pack_id: str, selected_symbol: str = "") -> str:
    pack = str(pack_id or "").strip()
    symbol = str(selected_symbol or "").strip().lower()
    return f"candidate_contract::{pack}::{symbol or 'pack'}"


def build_base_item(
    row: dict[str, str],
    *,
    selected_symbol: str = "",
    selected_symbol_index: int | None = None,
) -> dict[str, Any]:
    pack_id = str(row.get("pack_id", "")).strip()
    symbol_value = str(selected_symbol or "").strip().lower()
    return {
        "rank": as_int(row.get("rank", "")),
        "pack_id": pack_id,
        "source_pack_id": pack_id,
        "pack_path": str(row.get("pack_path", "")).strip(),
        "decision_tier": str(row.get("decision_tier", "")).strip(),
        "contract_row_id": contract_row_id_for(pack_id, symbol_value),
        "selected_symbol": symbol_value or None,
        "selected_symbol_index": selected_symbol_index,
        "selected_family_id": None,
        "selected_family_report_path": None,
        "translation_status": INSUFFICIENT_CONTRACT,
        "reject_reason": "",
        "strategy_spec": None,
    }


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


def report_passes_family_contract(obj: dict[str, Any]) -> bool:
    family_id = str(obj.get("family_id", "")).strip()
    if family_id != "momentum_v1":
        return True
    result_obj = obj.get("result")
    if not isinstance(result_obj, dict):
        return False
    if result_obj.get("pass_signal") is not True:
        return False
    return True


def usable_supported_reports(report_dir: Path, selected_symbol: str) -> list[tuple[Path, dict[str, Any]]]:
    supported: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(report_dir.glob("family_*_report.json")):
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        family_id = str(obj.get("family_id", "")).strip()
        status = str(obj.get("status", "")).strip().lower()
        exchange = str(obj.get("exchange", "")).strip()
        stream = str(obj.get("stream", "")).strip()
        symbol = str(obj.get("symbol", "")).strip().lower()
        result_obj = obj.get("result")
        selected_cell = result_obj.get("selected_cell") if isinstance(result_obj, dict) else None
        if not family_id or status != "ok" or not exchange or not stream or symbol != selected_symbol:
            continue
        if not isinstance(selected_cell, dict):
            continue
        if not report_passes_family_contract(obj):
            continue
        supported.append((path, obj))
    return supported


def resolve_supported_report(
    supported: list[tuple[Path, dict[str, Any]]],
    preferred_family_id: str = "",
) -> tuple[tuple[Path, dict[str, Any]] | None, str, str]:
    if not supported:
        return None, UNSUPPORTED_FAMILY, "NO_SUPPORTED_FAMILY_REPORT"
    if len(supported) == 1:
        return supported[0], "", ""

    preferred_family = str(preferred_family_id or "").strip()
    if preferred_family:
        matching_supported = [
            (path, obj)
            for path, obj in supported
            if str(obj.get("family_id") or "").strip() == preferred_family
        ]
        if len(matching_supported) == 1:
            return matching_supported[0], "", ""
        if len(matching_supported) > 1:
            return None, NOT_TRANSLATABLE_YET, "MULTIPLE_SELECTED_FAMILY_REPORTS"
    return None, NOT_TRANSLATABLE_YET, "MULTIPLE_SUPPORTED_FAMILY_REPORTS"


def translate_symbol_item(
    row: dict[str, str],
    *,
    pack_path: Path,
    selected_symbol: str,
    selected_symbol_index: int,
    preferred_family_id: str = "",
) -> dict[str, Any]:
    item = build_base_item(
        row,
        selected_symbol=selected_symbol,
        selected_symbol_index=selected_symbol_index,
    )
    report_dir = pack_path / "runs" / selected_symbol / "artifacts" / "multi_hypothesis"
    if not report_dir.exists():
        item["reject_reason"] = "FAMILY_REPORT_DIR_MISSING"
        return item

    supported = usable_supported_reports(report_dir, selected_symbol)
    resolved, translation_status, reject_reason = resolve_supported_report(
        supported,
        preferred_family_id=preferred_family_id,
    )
    if resolved is None:
        item["translation_status"] = translation_status
        item["reject_reason"] = reject_reason
        return item

    report_path, report_obj = resolved
    family_id = str(report_obj.get("family_id", "")).strip()
    exchange = str(report_obj.get("exchange", "")).strip()
    stream = str(report_obj.get("stream", "")).strip()
    symbol = str(report_obj.get("symbol", "")).strip().lower()
    params = report_obj.get("params")
    result_obj = report_obj.get("result")
    selected_cell = result_obj.get("selected_cell") if isinstance(result_obj, dict) else None
    window = str(report_obj.get("window", "")).strip()
    if not family_id or not exchange or not stream or symbol != selected_symbol:
        item["reject_reason"] = "SUPPORTED_REPORT_MISMATCH"
        return item
    if not isinstance(params, dict):
        item["reject_reason"] = "SUPPORTED_REPORT_PARAMS_INVALID"
        return item
    if not isinstance(selected_cell, dict):
        item["reject_reason"] = "SUPPORTED_REPORT_SELECTED_CELL_INVALID"
        return item

    item["selected_family_id"] = family_id
    item["selected_family_report_path"] = str(report_path)
    item["translation_status"] = TRANSLATABLE
    item["reject_reason"] = ""
    item["strategy_spec"] = {
        "strategy_spec_version": STRATEGY_SPEC_VERSION,
        "strategy_id": f"candidate_strategy::{family_id}::{item['pack_id']}::{symbol}",
        "source_pack_id": item["source_pack_id"],
        "source_contract_row_id": item["contract_row_id"],
        "source_decision_tier": item["decision_tier"],
        "source_selected_symbol": symbol,
        "family_id": family_id,
        "exchange": exchange,
        "stream": stream,
        "symbols": [symbol],
        "activation_mode": "SPEC_ONLY",
        "runtime_binding_status": "UNBOUND",
        "source_family_report_path": str(report_path),
        "strategy_params": {
            "window": window,
            "params": params,
            "selected_cell": selected_cell,
        },
    }
    return item


def translate_row(row: dict[str, str], preferred_family_id: str = "") -> list[dict[str, Any]]:
    item = build_base_item(row)
    pack_path_raw = item["pack_path"]
    if not item["pack_id"]:
        item["reject_reason"] = "PACK_ID_MISSING"
        return [item]
    if not pack_path_raw:
        item["reject_reason"] = "PACK_PATH_MISSING"
        return [item]

    pack_path = Path(pack_path_raw)
    if not pack_path.exists():
        item["reject_reason"] = "PACK_PATH_MISSING"
        return [item]

    campaign_plan_path = pack_path / "campaign_plan.json"
    if not campaign_plan_path.exists():
        item["reject_reason"] = "CAMPAIGN_PLAN_MISSING"
        return [item]

    try:
        plan = load_json(campaign_plan_path, "campaign_plan")
    except CandidateStrategyContractError:
        item["reject_reason"] = "CAMPAIGN_PLAN_INVALID"
        return [item]

    selected_symbols = selected_symbols_from_plan(plan)
    if not selected_symbols:
        item["reject_reason"] = "NO_SELECTED_SYMBOLS"
        return [item]

    return [
        translate_symbol_item(
            row,
            pack_path=pack_path,
            selected_symbol=selected_symbol,
            selected_symbol_index=index,
            preferred_family_id=preferred_family_id,
        )
        for index, selected_symbol in enumerate(selected_symbols)
    ]


def build_payload(
    candidate_review_tsv: Path,
    preferred_family_id: str,
    source_candidate_review_row_count: int,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    counts = {
        TRANSLATABLE: 0,
        NOT_TRANSLATABLE_YET: 0,
        INSUFFICIENT_CONTRACT: 0,
        UNSUPPORTED_FAMILY: 0,
    }
    for item in items:
        counts[str(item.get("translation_status") or INSUFFICIENT_CONTRACT)] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_candidate_review_tsv": str(candidate_review_tsv),
        "source_candidate_review_row_count": int(source_candidate_review_row_count),
        "preferred_family_id": preferred_family_id,
        "source_row_count": len(items),
        "translatable_count": counts[TRANSLATABLE],
        "not_translatable_yet_count": counts[NOT_TRANSLATABLE_YET],
        "insufficient_contract_count": counts[INSUFFICIENT_CONTRACT],
        "unsupported_family_count": counts[UNSUPPORTED_FAMILY],
        "items": items,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    candidate_review_tsv = Path(args.candidate_review_tsv).resolve()
    family_selection_json = Path(args.family_selection_json).resolve()
    out_json = Path(args.out_json).resolve()
    rows = load_candidate_review_rows(candidate_review_tsv)
    preferred_family_id = resolve_preferred_family_id(family_selection_json, args.preferred_family_id)
    items: list[dict[str, Any]] = []
    for row in rows:
        items.extend(translate_row(row, preferred_family_id=preferred_family_id))
    payload = build_payload(
        candidate_review_tsv,
        preferred_family_id,
        source_candidate_review_row_count=len(rows),
        items=items,
    )
    write_json(out_json, payload)
    print(f"candidate_strategy_contract_json={out_json}")
    print(f"preferred_family_id={payload['preferred_family_id']}")
    print(f"source_candidate_review_row_count={payload['source_candidate_review_row_count']}")
    print(f"source_row_count={payload['source_row_count']}")
    print(f"translatable_count={payload['translatable_count']}")
    print(f"not_translatable_yet_count={payload['not_translatable_yet_count']}")
    print(f"insufficient_contract_count={payload['insufficient_contract_count']}")
    print(f"unsupported_family_count={payload['unsupported_family_count']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CandidateStrategyContractError as exc:
        print(f"CANDIDATE_STRATEGY_CONTRACT_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
