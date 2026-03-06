#!/usr/bin/env python3
"""Lane-aware Phase-5 runtime policy helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


DEFAULT_LANE_POLICY_PATH = "tools/phase5_state/lane_policy_v0.json"
RUNTIME_FIELDS = ("max_symbols", "per_run_timeout_min", "max_wall_min")


def lane_key(exchange: str, stream: str) -> str:
    return f"{str(exchange or '').strip().lower()}/{str(stream or '').strip().lower()}"


def _normalize_runtime_values(raw: Dict[str, Any], *, section: str) -> Dict[str, int]:
    if not isinstance(raw, dict):
        raise ValueError(f"invalid_lane_policy_section:{section}")
    out: Dict[str, int] = {}
    for field in RUNTIME_FIELDS:
        if field not in raw:
            raise ValueError(f"missing_lane_policy_field:{section}:{field}")
        value = int(raw.get(field, 0) or 0)
        if value <= 0:
            raise ValueError(f"invalid_lane_policy_value:{section}:{field}")
        out[field] = value
    return out


def load_lane_policy(path: str | Path) -> Dict[str, Any]:
    policy_path = Path(path)
    obj = json.loads(policy_path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("invalid_lane_policy_root")
    default = _normalize_runtime_values(obj.get("default", {}), section="default")
    overrides_raw = obj.get("overrides", {})
    if not isinstance(overrides_raw, dict):
        raise ValueError("invalid_lane_policy_overrides")
    overrides: Dict[str, Dict[str, int]] = {}
    for raw_key, raw_value in sorted(overrides_raw.items()):
        key = str(raw_key or "").strip().lower()
        if not key or "/" not in key:
            raise ValueError(f"invalid_lane_policy_key:{raw_key}")
        merged = dict(default)
        if not isinstance(raw_value, dict):
            raise ValueError(f"invalid_lane_policy_override:{key}")
        for field, value in raw_value.items():
            if field not in RUNTIME_FIELDS:
                continue
            ivalue = int(value or 0)
            if ivalue <= 0:
                raise ValueError(f"invalid_lane_policy_value:{key}:{field}")
            merged[field] = ivalue
        overrides[key] = merged
    return {
        "path": str(policy_path),
        "default": default,
        "overrides": overrides,
    }


def resolve_lane_policy(exchange: str, stream: str, policy_json: Dict[str, Any]) -> Dict[str, int]:
    key = lane_key(exchange, stream)
    default = dict(policy_json.get("default", {}))
    override = dict((policy_json.get("overrides") or {}).get(key, {}))
    resolved = dict(default)
    resolved.update(override)
    return {
        field: int(resolved[field])
        for field in RUNTIME_FIELDS
    }
