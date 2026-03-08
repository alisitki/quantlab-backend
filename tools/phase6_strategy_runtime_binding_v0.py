#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATE_STRATEGY_CONTRACT_JSON = ROOT / "tools" / "phase6_state" / "candidate_strategy_contract_v0.json"
DEFAULT_BINDING_MAP_JSON = ROOT / "tools" / "phase6_state" / "family_shadow_runtime_binding_map_v0.json"
DEFAULT_OUT_JSON = ROOT / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
SCHEMA_VERSION = "candidate_strategy_runtime_binding_v0"
BINDING_MAP_SCHEMA_VERSION = "family_shadow_runtime_binding_map_v0"
CANDIDATE_STRATEGY_CONTRACT_SCHEMA_VERSION = "candidate_strategy_contract_v0"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
UNBOUND_NO_RUNTIME_IMPL = "UNBOUND_NO_RUNTIME_IMPL"
UNBOUND_CONFIG_GAP = "UNBOUND_CONFIG_GAP"
UNBOUND_TRANSLATION_REJECTED = "UNBOUND_TRANSLATION_REJECTED"


class CandidateStrategyRuntimeBindingError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise CandidateStrategyRuntimeBindingError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strategy spec -> shadow runtime binding v0")
    parser.add_argument("--candidate-strategy-contract-json", default=str(DEFAULT_CANDIDATE_STRATEGY_CONTRACT_JSON))
    parser.add_argument("--binding-map-json", default=str(DEFAULT_BINDING_MAP_JSON))
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


def load_candidate_strategy_contract(path: Path) -> dict[str, Any]:
    obj = load_json(path, "candidate_strategy_contract_json")
    if str(obj.get("schema_version") or "").strip() != CANDIDATE_STRATEGY_CONTRACT_SCHEMA_VERSION:
        fail(f"candidate_strategy_contract_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"candidate_strategy_contract_items_invalid:{path}")
    return obj


def load_binding_map(path: Path) -> dict[str, Any]:
    obj = load_json(path, "binding_map_json")
    if str(obj.get("schema_version") or "").strip() != BINDING_MAP_SCHEMA_VERSION:
        fail(f"binding_map_schema_mismatch:{path}")
    bindings = obj.get("bindings")
    if not isinstance(bindings, dict):
        fail(f"binding_map_bindings_invalid:{path}")
    return obj


def resolve_strategy_file_path(strategy_file: str) -> Path:
    raw = str(strategy_file or "").strip()
    if not raw:
        return Path("")
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    return ROOT / raw


def normalize_str_list(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(value or "").strip() for value in raw if str(value or "").strip()]


def base_item(source_item: dict[str, Any]) -> dict[str, Any]:
    spec = source_item.get("strategy_spec")
    spec_obj = spec if isinstance(spec, dict) else {}
    return {
        "rank": source_item.get("rank"),
        "pack_id": str(source_item.get("pack_id") or "").strip(),
        "translation_status": str(source_item.get("translation_status") or "").strip(),
        "strategy_id": str(spec_obj.get("strategy_id") or "").strip() or None,
        "family_id": str(spec_obj.get("family_id") or "").strip() or None,
        "exchange": str(spec_obj.get("exchange") or "").strip() or None,
        "stream": str(spec_obj.get("stream") or "").strip() or None,
        "symbols": normalize_str_list(spec_obj.get("symbols")),
        "runtime_binding_status": UNBOUND_TRANSLATION_REJECTED,
        "runtime_strategy_file": None,
        "runtime_strategy_config": None,
        "binding_reason": "",
    }


def validate_translatable_spec(spec: dict[str, Any]) -> tuple[bool, str]:
    if not str(spec.get("strategy_id") or "").strip():
        return False, "STRATEGY_ID_MISSING"
    if not str(spec.get("family_id") or "").strip():
        return False, "FAMILY_ID_MISSING"
    if not str(spec.get("exchange") or "").strip():
        return False, "EXCHANGE_MISSING"
    if not str(spec.get("stream") or "").strip():
        return False, "STREAM_MISSING"
    symbols = normalize_str_list(spec.get("symbols"))
    if not symbols:
        return False, "SYMBOLS_MISSING"
    strategy_params = spec.get("strategy_params")
    if strategy_params is not None and not isinstance(strategy_params, dict):
        return False, "STRATEGY_PARAMS_INVALID"
    return True, ""


def build_runtime_strategy_config(spec: dict[str, Any], static_config: dict[str, Any]) -> dict[str, Any]:
    strategy_params = spec.get("strategy_params")
    strategy_params_obj = strategy_params if isinstance(strategy_params, dict) else {}
    runtime_config: dict[str, Any] = dict(static_config)
    runtime_config["family_id"] = str(spec.get("family_id") or "").strip()
    runtime_config["source_pack_id"] = str(spec.get("source_pack_id") or "").strip()
    runtime_config["source_decision_tier"] = str(spec.get("source_decision_tier") or "").strip()
    runtime_config["exchange"] = str(spec.get("exchange") or "").strip()
    runtime_config["stream"] = str(spec.get("stream") or "").strip()
    runtime_config["symbols"] = normalize_str_list(spec.get("symbols"))
    runtime_config["source_family_report_path"] = str(spec.get("source_family_report_path") or "").strip()
    runtime_config["window"] = str(strategy_params_obj.get("window") or "").strip() or None
    runtime_config["params"] = strategy_params_obj.get("params") if isinstance(strategy_params_obj.get("params"), dict) else None
    runtime_config["selected_cell"] = (
        strategy_params_obj.get("selected_cell")
        if isinstance(strategy_params_obj.get("selected_cell"), dict)
        else None
    )
    return runtime_config


def bind_item(source_item: dict[str, Any], bindings: dict[str, Any]) -> dict[str, Any]:
    item = base_item(source_item)
    translation_status = item["translation_status"]
    reject_reason = str(source_item.get("reject_reason") or "").strip()
    spec = source_item.get("strategy_spec")

    if translation_status != "TRANSLATABLE" or not isinstance(spec, dict):
        item["runtime_binding_status"] = UNBOUND_TRANSLATION_REJECTED
        item["binding_reason"] = (
            f"TRANSLATION_STATUS:{translation_status}:{reject_reason or 'NONE'}"
        )
        return item

    valid_spec, spec_reason = validate_translatable_spec(spec)
    if not valid_spec:
        item["runtime_binding_status"] = UNBOUND_CONFIG_GAP
        item["binding_reason"] = spec_reason
        return item

    family_id = str(spec.get("family_id") or "").strip()
    binding = bindings.get(family_id)
    if not isinstance(binding, dict):
        item["runtime_binding_status"] = UNBOUND_NO_RUNTIME_IMPL
        item["binding_reason"] = f"NO_RUNTIME_BINDING_FOR_FAMILY:{family_id}"
        return item

    strategy_file = str(binding.get("strategy_file") or "").strip()
    strategy_config = binding.get("strategy_config")
    supported_streams = normalize_str_list(binding.get("supported_streams"))
    supported_exchanges = normalize_str_list(binding.get("supported_exchanges"))
    if not strategy_file:
        item["runtime_binding_status"] = UNBOUND_CONFIG_GAP
        item["binding_reason"] = "BINDING_STRATEGY_FILE_MISSING"
        return item
    if strategy_config is None:
        strategy_config = {}
    if not isinstance(strategy_config, dict):
        item["runtime_binding_status"] = UNBOUND_CONFIG_GAP
        item["binding_reason"] = "BINDING_STRATEGY_CONFIG_INVALID"
        return item
    if supported_streams and str(spec.get("stream") or "").strip() not in supported_streams:
        item["runtime_binding_status"] = UNBOUND_CONFIG_GAP
        item["binding_reason"] = "SPEC_STREAM_UNSUPPORTED"
        return item
    if supported_exchanges and str(spec.get("exchange") or "").strip() not in supported_exchanges:
        item["runtime_binding_status"] = UNBOUND_CONFIG_GAP
        item["binding_reason"] = "SPEC_EXCHANGE_UNSUPPORTED"
        return item
    resolved_strategy_file = resolve_strategy_file_path(strategy_file)
    if not resolved_strategy_file.exists():
        item["runtime_binding_status"] = UNBOUND_CONFIG_GAP
        item["binding_reason"] = "BINDING_STRATEGY_FILE_NOT_FOUND"
        return item

    item["runtime_binding_status"] = BOUND_SHADOW_RUNNABLE
    item["runtime_strategy_file"] = strategy_file
    item["runtime_strategy_config"] = build_runtime_strategy_config(spec, strategy_config)
    item["binding_reason"] = ""
    return item


def build_payload(
    candidate_strategy_contract_json: Path,
    binding_map_json: Path,
    binding_map: dict[str, Any],
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    counts = {
        BOUND_SHADOW_RUNNABLE: 0,
        UNBOUND_NO_RUNTIME_IMPL: 0,
        UNBOUND_CONFIG_GAP: 0,
        UNBOUND_TRANSLATION_REJECTED: 0,
    }
    for item in items:
        counts[str(item.get("runtime_binding_status") or UNBOUND_CONFIG_GAP)] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_ts_utc": utc_now_iso(),
        "source_candidate_strategy_contract_json": str(candidate_strategy_contract_json),
        "source_binding_map_json": str(binding_map_json),
        "source_row_count": len(items),
        "translated_spec_count": sum(1 for item in items if str(item.get("translation_status") or "") == "TRANSLATABLE"),
        "bound_shadow_runnable_count": counts[BOUND_SHADOW_RUNNABLE],
        "unbound_no_runtime_impl_count": counts[UNBOUND_NO_RUNTIME_IMPL],
        "unbound_config_gap_count": counts[UNBOUND_CONFIG_GAP],
        "unbound_translation_rejected_count": counts[UNBOUND_TRANSLATION_REJECTED],
        "bindable_family_ids": sorted(str(key) for key in (binding_map.get("bindings") or {}).keys()),
        "items": items,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    candidate_strategy_contract_json = Path(args.candidate_strategy_contract_json).resolve()
    binding_map_json = Path(args.binding_map_json).resolve()
    out_json = Path(args.out_json).resolve()

    contract = load_candidate_strategy_contract(candidate_strategy_contract_json)
    binding_map = load_binding_map(binding_map_json)
    bindings = dict(binding_map.get("bindings") or {})
    source_items = list(contract.get("items") or [])
    items = [bind_item(item, bindings) for item in source_items]
    payload = build_payload(candidate_strategy_contract_json, binding_map_json, binding_map, items)
    write_json(out_json, payload)
    print(f"candidate_strategy_runtime_binding_json={out_json}")
    print(f"source_row_count={payload['source_row_count']}")
    print(f"translated_spec_count={payload['translated_spec_count']}")
    print(f"bound_shadow_runnable_count={payload['bound_shadow_runnable_count']}")
    print(f"unbound_no_runtime_impl_count={payload['unbound_no_runtime_impl_count']}")
    print(f"unbound_config_gap_count={payload['unbound_config_gap_count']}")
    print(f"unbound_translation_rejected_count={payload['unbound_translation_rejected_count']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CandidateStrategyRuntimeBindingError as exc:
        print(f"CANDIDATE_STRATEGY_RUNTIME_BINDING_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
