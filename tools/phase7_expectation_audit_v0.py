#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PHASE6 = ROOT / "tools" / "phase6_state"
PHASE7_RESULT = ROOT / "tools" / "phase7_shadow_result_v0.json"
OUTPUT_DIR = ROOT / "tools" / "phase7_expectation_audit_output"
DEFAULT_CANONICAL_TRUTH_REGISTRY = ROOT / "tools" / "system_state" / "canonical_truth_registry_v0.json"
DEFAULT_SHORTLIST_JSON = PHASE6 / "shadow_shortlist_v0.json"
DEFAULT_CONTRACT_JSON = PHASE6 / "candidate_strategy_contract_v0.json"
DEFAULT_BINDING_JSON = PHASE6 / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_TRIAGE_REPORT_JSON = PHASE6 / "tradability_triage_report_v0.json"
DEFAULT_SHADOW_RESULT_JSON = PHASE7_RESULT
DEFAULT_EXPECTATION_REPORT_JSON = OUTPUT_DIR / "expectation_audit_report_v0.json"
DEFAULT_EXECUTION_PLAN_JSON = OUTPUT_DIR / "shadow_window_plan_v0.json"
DEFAULT_FEE_RATE = 0.0004
FINALIZE_PATTERN = re.compile(
    r"finalize matched_trade_events=(?P<matched>\d+) signal_event_count=(?P<signal>\d+) order_event_count=(?P<order>\d+)"
)
HIGH_FREQUENCY = "HIGH_FREQUENCY"
MEDIUM_FREQUENCY = "MEDIUM_FREQUENCY"
LOW_FREQUENCY = "LOW_FREQUENCY"
UNKNOWN_FREQUENCY = "UNKNOWN_FREQUENCY"
SUPPORTED_BANDS = {HIGH_FREQUENCY, MEDIUM_FREQUENCY, LOW_FREQUENCY, UNKNOWN_FREQUENCY}


class ExpectationAuditError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise ExpectationAuditError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase7 expectation audit and minimum observation window planner")
    parser.add_argument("--canonical-truth-registry", default=str(DEFAULT_CANONICAL_TRUTH_REGISTRY))
    parser.add_argument("--shortlist-json", default=str(DEFAULT_SHORTLIST_JSON))
    parser.add_argument("--contract-json", default=str(DEFAULT_CONTRACT_JSON))
    parser.add_argument("--binding-json", default=str(DEFAULT_BINDING_JSON))
    parser.add_argument("--triage-report-json", default=str(DEFAULT_TRIAGE_REPORT_JSON))
    parser.add_argument("--shadow-result-json", default=str(DEFAULT_SHADOW_RESULT_JSON))
    parser.add_argument("--out-report-json", default=str(DEFAULT_EXPECTATION_REPORT_JSON))
    parser.add_argument("--out-plan-json", default=str(DEFAULT_EXECUTION_PLAN_JSON))
    parser.add_argument(
        "--contextual-prior-run-minutes",
        type=float,
        default=14.0,
        help="Contextual prior-run duration claim from operator discussion; compared against authoritative artifact if present.",
    )
    args = parser.parse_args(argv)
    if args.contextual_prior_run_minutes <= 0:
        fail(f"invalid_contextual_prior_run_minutes:{args.contextual_prior_run_minutes}")
    return args


def load_json(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        fail(f"{label}_missing:{path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"{label}_invalid_json:{path}:{exc}")
    if not isinstance(obj, dict):
        fail(f"{label}_not_object:{path}")
    return obj


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def require_registry_path(registry: dict[str, Any], concept_name: str, expected_path: Path) -> dict[str, Any]:
    expected_resolved = str(expected_path.resolve())
    for item in registry.get("concepts", []):
        if str(item.get("concept") or "").strip() != concept_name:
            continue
        authoritative_now = item.get("authoritative_now")
        if not isinstance(authoritative_now, list):
            fail(f"canonical_registry_concept_missing_authoritative_now:{concept_name}")
        for entry in authoritative_now:
            if not isinstance(entry, dict):
                continue
            entry_path = str(entry.get("path") or "").strip()
            if not entry_path:
                continue
            if str(Path(entry_path).resolve()) == expected_resolved:
                return item
        fail(f"canonical_registry_path_mismatch:{concept_name}:{expected_path}")
    fail(f"canonical_registry_concept_missing:{concept_name}")


def contract_by_strategy_id(contract_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = contract_doc.get("items")
    if not isinstance(items, list):
        fail("contract_json_missing_items")
    out: dict[str, dict[str, Any]] = {}
    for row in items:
        if not isinstance(row, dict):
            continue
        strategy_spec = row.get("strategy_spec")
        if not isinstance(strategy_spec, dict):
            continue
        strategy_id = str(strategy_spec.get("strategy_id") or "").strip()
        if strategy_id:
            out[strategy_id] = row
    return out


def binding_by_strategy_id(binding_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = binding_doc.get("items")
    if not isinstance(items, list):
        fail("binding_json_missing_items")
    out: dict[str, dict[str, Any]] = {}
    for row in items:
        if not isinstance(row, dict):
            continue
        strategy_id = str(row.get("strategy_id") or "").strip()
        if strategy_id:
            out[strategy_id] = row
    return out


def triage_by_strategy_id(triage_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = triage_doc.get("report")
    if not isinstance(rows, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        strategy_id = str(row.get("strategy_id") or "").strip()
        if strategy_id:
            out[strategy_id] = row
    return out


def shadow_result_by_strategy_id(shadow_result_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = shadow_result_doc.get("results")
    if not isinstance(rows, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        strategy_id = str(row.get("strategy_id") or "").strip()
        if strategy_id:
            out[strategy_id] = row
    return out


def shortlist_rows(shortlist_doc: dict[str, Any]) -> list[dict[str, Any]]:
    rows = shortlist_doc.get("strategies")
    if not isinstance(rows, list):
        fail("shortlist_json_missing_strategies")
    ordered: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            fail("shortlist_strategy_not_object")
        ordered.append(row)
    if not ordered:
        fail("shortlist_empty")
    return ordered


def positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def non_negative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def contract_runtime_details(contract_row: dict[str, Any], binding_row: dict[str, Any]) -> dict[str, Any]:
    strategy_spec = contract_row.get("strategy_spec")
    if not isinstance(strategy_spec, dict):
        fail(f"contract_strategy_spec_missing:{binding_row.get('strategy_id')}")
    strategy_params = strategy_spec.get("strategy_params")
    if not isinstance(strategy_params, dict):
        fail(f"contract_strategy_params_missing:{binding_row.get('strategy_id')}")
    selected_cell = strategy_params.get("selected_cell")
    if not isinstance(selected_cell, dict):
        fail(f"contract_selected_cell_missing:{binding_row.get('strategy_id')}")
    runtime_cfg = binding_row.get("runtime_strategy_config")
    if not isinstance(runtime_cfg, dict):
        fail(f"binding_runtime_strategy_config_missing:{binding_row.get('strategy_id')}")

    family_id = str(strategy_spec.get("family_id") or binding_row.get("family_id") or "").strip()
    exchange = str(strategy_spec.get("exchange") or binding_row.get("exchange") or "").strip()
    stream = str(strategy_spec.get("stream") or binding_row.get("stream") or "").strip()
    symbol = str(strategy_spec.get("source_selected_symbol") or binding_row.get("selected_symbol") or "").strip()
    delta_ms = non_negative_int(selected_cell.get("delta_ms"))
    h_ms = non_negative_int(selected_cell.get("h_ms"))
    event_count = non_negative_int(selected_cell.get("event_count"))
    t_stat = positive_float(abs(float(selected_cell.get("t_stat", 0.0))))
    mean_product = selected_cell.get("mean_product")
    fee_rate = positive_float((runtime_cfg.get("execution_config") or {}).get("feeRate")) or DEFAULT_FEE_RATE
    min_edge_cost_multiple = positive_float(runtime_cfg.get("min_edge_cost_multiple")) or 1.25
    explicit_min_abs = positive_float(runtime_cfg.get("min_abs_past_return_bps"))
    if delta_ms is None or delta_ms <= 0 or h_ms is None or h_ms <= 0 or event_count is None or event_count <= 0 or t_stat is None:
        fail(f"selected_cell_missing_required_fields:{binding_row.get('strategy_id')}")

    confidence_ratio = min(t_stat / 2.0, event_count / 200.0)
    if confidence_ratio >= 20:
        confidence_bucket = "HIGH"
        confidence_bucket_multiplier = 0.5
    elif confidence_ratio >= 5:
        confidence_bucket = "MEDIUM"
        confidence_bucket_multiplier = 0.75
    else:
        confidence_bucket = "LOW"
        confidence_bucket_multiplier = 1.0
    fee_floor_bps = fee_rate * 10_000 * 2 * min_edge_cost_multiple
    min_abs_past_return_bps = explicit_min_abs if explicit_min_abs is not None else fee_floor_bps * confidence_bucket_multiplier

    return {
        "family_id": family_id,
        "exchange": exchange,
        "stream": stream,
        "symbol": symbol,
        "delta_ms": delta_ms,
        "h_ms": h_ms,
        "selected_cell_event_count": event_count,
        "selected_cell_abs_t_stat": t_stat,
        "selected_cell_mean_product": mean_product,
        "confidence_ratio": confidence_ratio,
        "confidence_bucket": confidence_bucket,
        "confidence_bucket_multiplier": confidence_bucket_multiplier,
        "fee_rate": fee_rate,
        "fee_floor_bps": fee_floor_bps,
        "min_edge_cost_multiple": min_edge_cost_multiple,
        "min_abs_past_return_bps": min_abs_past_return_bps,
    }


def load_batch_stdout_log(prior_row: dict[str, Any]) -> tuple[Path | None, Path | None]:
    artifacts = prior_row.get("artifacts")
    if not isinstance(artifacts, dict):
        return None, None
    batch_result_path = Path(str(artifacts.get("batch_result_json_path") or "")).resolve()
    if not batch_result_path.exists():
        return None, None
    batch_result = load_json(batch_result_path, "phase7_batch_result_json")
    results = batch_result.get("results")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        return None, None
    item = results[0]
    stdout_path = Path(str(item.get("stdout_log_path") or "")).resolve()
    stderr_path = Path(str(item.get("stderr_log_path") or "")).resolve()
    return stdout_path if stdout_path.exists() else None, stderr_path if stderr_path.exists() else None


def parse_finalize_metrics(stdout_path: Path | None) -> dict[str, Any]:
    if stdout_path is None:
        return {
            "stdout_log_present": False,
            "matched_trade_events": None,
            "signal_event_count": None,
            "order_event_count": None,
            "finalize_line_present": False,
        }
    text = stdout_path.read_text(encoding="utf-8", errors="replace")
    match = FINALIZE_PATTERN.search(text)
    if not match:
        return {
            "stdout_log_present": True,
            "matched_trade_events": None,
            "signal_event_count": None,
            "order_event_count": None,
            "finalize_line_present": False,
        }
    return {
        "stdout_log_present": True,
        "matched_trade_events": int(match.group("matched")),
        "signal_event_count": int(match.group("signal")),
        "order_event_count": int(match.group("order")),
        "finalize_line_present": True,
    }


def per_minute(count: int | None, run_duration_sec: float | None) -> float | None:
    if count is None or run_duration_sec is None or run_duration_sec <= 0:
        return None
    return (count / run_duration_sec) * 60.0


def infer_decision_frequency_band(runtime: dict[str, Any], observed: dict[str, Any], triage_row: dict[str, Any] | None) -> tuple[str, str, list[str]]:
    family_id = str(runtime["family_id"])
    if family_id != "return_reversal_v1":
        return UNKNOWN_FREQUENCY, "Unsupported family for expectation estimator.", [
            "Current estimator only has a narrow rule for return_reversal_v1.",
        ]

    signal_events_per_min = observed.get("signal_events_per_min")
    matched_trade_events = observed.get("matched_trade_events")
    min_abs_past_return_bps = float(runtime["min_abs_past_return_bps"])
    delta_ms = int(runtime["delta_ms"])
    reasons: list[str] = [
        f"Runtime requires about {min_abs_past_return_bps:.2f} bps over delta={delta_ms} ms before an order can trigger.",
        "Observed signal_event_count is used only as a signal-evaluation pace proxy, not as direct decision frequency.",
    ]
    if triage_row is not None:
        reasons.append(
            f"Triage classified this row as {str(triage_row.get('classification') or 'UNKNOWN')} with reason "
            f"{str((triage_row.get('reasoning') or {}).get('reason_code') or 'UNKNOWN')}."
        )

    if signal_events_per_min is None or matched_trade_events is None:
        reasons.append("No finalize signal metrics were available from the prior bounded run.")
        return UNKNOWN_FREQUENCY, "Missing finalize metrics from prior smoke run.", reasons

    if signal_events_per_min < 1.0 and int(matched_trade_events) <= 1:
        reasons.append("The prior smoke run produced too few anchored signal samples to estimate decision pace safely.")
        return UNKNOWN_FREQUENCY, "Too few anchored signal samples in prior smoke run.", reasons

    score = 0
    if delta_ms <= 100:
        score += 2
        reasons.append("Short 100ms lookback/horizon increases the chance of multiple intraday opportunities.")
    elif delta_ms <= 250:
        score += 1
        reasons.append("250ms lookback/horizon is still intraday-reactive, but less aggressive than 100ms.")
    else:
        reasons.append("500ms lookback/horizon is slower and usually reduces order frequency relative to shorter cells.")

    if min_abs_past_return_bps <= 5.0:
        score += 2
        reasons.append("The runtime threshold is about 5bps, which is the loosest threshold in this shortlist.")
    elif min_abs_past_return_bps <= 7.5:
        score += 1
        reasons.append("The runtime threshold is about 7.5bps, which is moderate rather than ultra-loose.")
    else:
        reasons.append("The runtime threshold is tighter than 7.5bps.")

    if signal_events_per_min >= 120.0:
        score += 2
        reasons.append("The smoke run showed a high anchored signal-evaluation pace.")
    elif signal_events_per_min >= 30.0:
        score += 1
        reasons.append("The smoke run showed a moderate anchored signal-evaluation pace.")
    else:
        reasons.append("The smoke run showed a sparse anchored signal-evaluation pace.")

    order_event_count = observed.get("order_event_count")
    if score >= 5:
        if order_event_count == 0:
            reasons.append("No orders were observed in the smoke run, so the band is conservatively capped at MEDIUM.")
            return MEDIUM_FREQUENCY, "High theoretical opportunity score capped to MEDIUM because observed orders stayed at zero.", reasons
        return HIGH_FREQUENCY, "Fast cell, loose threshold, and high observed signal pace.", reasons
    if score >= 3:
        return MEDIUM_FREQUENCY, "At least one of horizon, threshold, or observed signal pace supports multi-hour rather than multi-day judgment.", reasons
    return LOW_FREQUENCY, "Slower cell and/or sparse signal-evaluation pace imply the strategy should not be judged from a short run.", reasons


def derive_mow(band: str) -> dict[str, Any]:
    if band == HIGH_FREQUENCY:
        return {
            "mow_hours_min": 2,
            "mow_hours_target": 6,
            "reason": "Even high-frequency directional rows should clear a multi-hour window before judgment.",
            "judgment_lane": "SHORT_RUN_JUDGEABLE",
        }
    if band == MEDIUM_FREQUENCY:
        return {
            "mow_hours_min": 6,
            "mow_hours_target": 12,
            "reason": "Medium-frequency rows need multi-hour observation; minutes are not enough.",
            "judgment_lane": "MULTI_HOUR",
        }
    if band == LOW_FREQUENCY:
        return {
            "mow_hours_min": 24,
            "mow_hours_target": 72,
            "reason": "Low-frequency rows need at least a day, and often 1-3 days, before judgment.",
            "judgment_lane": "MULTI_DAY",
        }
    return {
        "mow_hours_min": 24,
        "mow_hours_target": 48,
        "reason": "Unknown-frequency rows default to a conservative long window until better instrumentation exists.",
        "judgment_lane": "MULTI_DAY",
    }


def sufficiency_assessment(
    *,
    mow_hours_min: int,
    prior_run_row: dict[str, Any] | None,
    contextual_prior_run_minutes: float,
) -> dict[str, Any]:
    required_minutes = float(mow_hours_min) * 60.0
    actual_minutes = None
    if prior_run_row is not None:
        metrics = prior_run_row.get("metrics")
        if isinstance(metrics, dict):
            run_duration_sec = positive_float(metrics.get("run_duration_sec"))
            if run_duration_sec is not None:
                actual_minutes = run_duration_sec / 60.0
    actual_sufficient = actual_minutes is not None and actual_minutes >= required_minutes
    contextual_sufficient = contextual_prior_run_minutes >= required_minutes
    mismatch_note = ""
    if actual_minutes is not None and abs(actual_minutes - contextual_prior_run_minutes) >= 1.0:
        mismatch_note = (
            f"Authoritative Phase7 artifact records about {actual_minutes:.3f} minutes, "
            f"while contextual input claimed about {contextual_prior_run_minutes:.1f} minutes."
        )
    note_parts = []
    if actual_minutes is None:
        note_parts.append("No authoritative prior-run duration was available.")
    else:
        note_parts.append(
            f"Authoritative prior run {actual_minutes:.3f}m vs minimum required {required_minutes:.1f}m."
        )
    note_parts.append(
        f"Contextual 14m assumption {'would' if contextual_sufficient else 'would not'} meet the same minimum."
    )
    if mismatch_note:
        note_parts.append(mismatch_note)
    return {
        "mow_required_minutes": required_minutes,
        "authoritative_prior_run_seen": actual_minutes is not None,
        "authoritative_prior_run_minutes": actual_minutes,
        "authoritative_prior_run_sufficient": actual_sufficient,
        "contextual_prior_run_minutes": contextual_prior_run_minutes,
        "contextual_prior_run_sufficient": contextual_sufficient,
        "note": " ".join(note_parts),
    }


def stable_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(row.get("rank") or 10**9),
        str(row.get("symbol") or ""),
        str(row.get("strategy_id") or ""),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    registry_path = Path(args.canonical_truth_registry).resolve()
    shortlist_path = Path(args.shortlist_json).resolve()
    contract_path = Path(args.contract_json).resolve()
    binding_path = Path(args.binding_json).resolve()
    triage_report_path = Path(args.triage_report_json).resolve()
    shadow_result_path = Path(args.shadow_result_json).resolve()
    out_report_path = Path(args.out_report_json).resolve()
    out_plan_path = Path(args.out_plan_json).resolve()

    registry = load_json(registry_path, "canonical_truth_registry")
    contract_truth = require_registry_path(registry, "candidate_strategy_contract", contract_path)
    binding_truth = require_registry_path(registry, "runtime_binding", binding_path)
    require_registry_path(registry, "family_role_classification", PHASE6 / "hypothesis_family_role_classification_v0.json")

    shortlist_doc = load_json(shortlist_path, "shadow_shortlist_json")
    contract_doc = load_json(contract_path, "contract_json")
    binding_doc = load_json(binding_path, "binding_json")
    triage_doc = load_json(triage_report_path, "triage_report_json") if triage_report_path.exists() else {}
    shadow_result_doc = load_json(shadow_result_path, "shadow_result_json") if shadow_result_path.exists() else {}

    shortlist = shortlist_rows(shortlist_doc)
    contract_map = contract_by_strategy_id(contract_doc)
    binding_map = binding_by_strategy_id(binding_doc)
    triage_map = triage_by_strategy_id(triage_doc)
    prior_result_map = shadow_result_by_strategy_id(shadow_result_doc)

    report_rows: list[dict[str, Any]] = []
    group_membership: dict[str, list[dict[str, Any]]] = {band: [] for band in sorted(SUPPORTED_BANDS)}
    short_run_judgeable: list[str] = []
    multi_hour: list[str] = []
    multi_day: list[str] = []

    for shortlist_row in shortlist:
        strategy_id = str(shortlist_row.get("strategy_id") or "").strip()
        if not strategy_id:
            fail("shortlist_strategy_id_missing")
        contract_row = contract_map.get(strategy_id)
        binding_row = binding_map.get(strategy_id)
        if contract_row is None:
            fail(f"contract_row_missing_for_strategy:{strategy_id}")
        if binding_row is None:
            fail(f"binding_row_missing_for_strategy:{strategy_id}")

        runtime = contract_runtime_details(contract_row, binding_row)
        prior_result_row = prior_result_map.get(strategy_id)
        stdout_path, _ = load_batch_stdout_log(prior_result_row) if prior_result_row is not None else (None, None)
        finalize_metrics = parse_finalize_metrics(stdout_path)

        prior_metrics = {}
        if prior_result_row is not None and isinstance(prior_result_row.get("metrics"), dict):
            prior_metrics = prior_result_row["metrics"]
        run_duration_sec = positive_float(prior_metrics.get("run_duration_sec"))

        observed = {
            **finalize_metrics,
            "run_duration_sec": run_duration_sec,
            "signal_events_per_min": per_minute(finalize_metrics.get("signal_event_count"), run_duration_sec),
            "matched_trade_events_per_min": per_minute(finalize_metrics.get("matched_trade_events"), run_duration_sec),
            "order_events_per_min": per_minute(finalize_metrics.get("order_event_count"), run_duration_sec),
            "processed_event_count": prior_metrics.get("processed_event_count"),
            "processed_events_per_min": per_minute(non_negative_int(prior_metrics.get("processed_event_count")), run_duration_sec),
        }

        band, expectation_reason, assumption_notes = infer_decision_frequency_band(runtime, observed, triage_map.get(strategy_id))
        mow = derive_mow(band)
        sufficiency = sufficiency_assessment(
            mow_hours_min=int(mow["mow_hours_min"]),
            prior_run_row=prior_result_row,
            contextual_prior_run_minutes=float(args.contextual_prior_run_minutes),
        )

        row = {
            "strategy_id": strategy_id,
            "rank": int(shortlist_row.get("rank") or 0),
            "family_id": runtime["family_id"],
            "exchange": runtime["exchange"] or str(shortlist_row.get("exchange") or "").strip(),
            "symbol": runtime["symbol"] or str(shortlist_row.get("selected_symbol") or "").strip(),
            "stream": runtime["stream"] or str(shortlist_row.get("stream") or "").strip(),
            "classification_band": band,
            "expected_decision_frequency_reason": expectation_reason,
            "assumptions": assumption_notes,
            "runtime_thresholds": {
                "delta_ms": runtime["delta_ms"],
                "h_ms": runtime["h_ms"],
                "selected_cell_event_count": runtime["selected_cell_event_count"],
                "selected_cell_abs_t_stat": round(float(runtime["selected_cell_abs_t_stat"]), 6),
                "confidence_ratio": round(float(runtime["confidence_ratio"]), 6),
                "confidence_bucket": runtime["confidence_bucket"],
                "fee_floor_bps": round(float(runtime["fee_floor_bps"]), 6),
                "min_abs_past_return_bps": round(float(runtime["min_abs_past_return_bps"]), 6),
            },
            "observed_smoke_window": {
                "run_duration_sec": run_duration_sec,
                "processed_event_count": prior_metrics.get("processed_event_count"),
                "processed_events_per_min": (
                    round(float(observed["processed_events_per_min"]), 6)
                    if observed["processed_events_per_min"] is not None
                    else None
                ),
                "matched_trade_events": observed["matched_trade_events"],
                "matched_trade_events_per_min": (
                    round(float(observed["matched_trade_events_per_min"]), 6)
                    if observed["matched_trade_events_per_min"] is not None
                    else None
                ),
                "signal_event_count": observed["signal_event_count"],
                "signal_events_per_min": (
                    round(float(observed["signal_events_per_min"]), 6)
                    if observed["signal_events_per_min"] is not None
                    else None
                ),
                "order_event_count": observed["order_event_count"],
                "order_events_per_min": (
                    round(float(observed["order_events_per_min"]), 6)
                    if observed["order_events_per_min"] is not None
                    else None
                ),
                "finalize_line_present": bool(observed["finalize_line_present"]),
                "stdout_log_path": str(stdout_path) if stdout_path is not None else "",
            },
            "triage_context": {
                "shortlist_reason": str(shortlist_row.get("shortlist_reason") or ""),
                "triage_classification": str((triage_map.get(strategy_id) or {}).get("classification") or ""),
                "triage_reason_code": str(((triage_map.get(strategy_id) or {}).get("reasoning") or {}).get("reason_code") or ""),
            },
            "mow": mow,
            "prior_run_sufficiency": sufficiency,
        }
        report_rows.append(row)

        group_membership[band].append(
            {
                "strategy_id": strategy_id,
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "mow_hours_min": int(mow["mow_hours_min"]),
                "mow_hours_target": int(mow["mow_hours_target"]),
            }
        )
        if mow["judgment_lane"] == "SHORT_RUN_JUDGEABLE":
            short_run_judgeable.append(strategy_id)
        elif mow["judgment_lane"] == "MULTI_HOUR":
            multi_hour.append(strategy_id)
        else:
            multi_day.append(strategy_id)

    report_rows.sort(key=stable_sort_key)
    for values in group_membership.values():
        values.sort(key=lambda row: (row["mow_hours_min"], row["symbol"], row["strategy_id"]))

    conclusion_note = (
        "Methodology correction confirmed. The current authoritative Phase7 result "
        f"{shadow_result_doc.get('generated_ts_utc')} records about "
        f"{report_rows[0]['prior_run_sufficiency']['authoritative_prior_run_minutes']:.3f} minutes per strategy "
        "for the available shortlist runs, not 14 minutes. Regardless, both the authoritative smoke run and a hypothetical 14-minute run "
        "remain below every derived MOW in this audit."
    )

    expectation_report = {
        "schema_version": "phase7_expectation_audit_report_v0",
        "generated_ts_utc": utc_now_iso(),
        "governance": {
            "authoritative_inputs": [
                {
                    "concept": "candidate_strategy_contract",
                    "path": str(contract_path),
                    "registry_entry": contract_truth,
                    "file_generated_ts_utc": contract_doc.get("generated_ts_utc"),
                },
                {
                    "concept": "runtime_binding",
                    "path": str(binding_path),
                    "registry_entry": binding_truth,
                    "file_generated_ts_utc": binding_doc.get("generated_ts_utc"),
                },
            ],
            "task_local_inputs": [
                {
                    "concept": "shadow_shortlist",
                    "path": str(shortlist_path),
                    "generated_ts_utc": shortlist_doc.get("generated_ts_utc"),
                },
                {
                    "concept": "tradability_triage_report",
                    "path": str(triage_report_path),
                    "generated_ts_utc": triage_doc.get("generated_ts_utc"),
                },
                {
                    "concept": "phase7_shadow_result",
                    "path": str(shadow_result_path),
                    "generated_ts_utc": shadow_result_doc.get("generated_ts_utc"),
                },
            ],
            "notes": [
                "This sprint does not issue tradable/fail verdicts.",
                "Global ranking, shortlist membership, and runtime bindings remain unchanged.",
                "Signal-event pace is inferred from isolated Phase7 smoke-run finalize logs; it is an evidence-backed pace proxy, not a direct decision-rate measurement.",
            ],
        },
        "summary": {
            "shortlist_count": len(report_rows),
            "band_counts": {
                HIGH_FREQUENCY: sum(1 for row in report_rows if row["classification_band"] == HIGH_FREQUENCY),
                MEDIUM_FREQUENCY: sum(1 for row in report_rows if row["classification_band"] == MEDIUM_FREQUENCY),
                LOW_FREQUENCY: sum(1 for row in report_rows if row["classification_band"] == LOW_FREQUENCY),
                UNKNOWN_FREQUENCY: sum(1 for row in report_rows if row["classification_band"] == UNKNOWN_FREQUENCY),
            },
            "prior_run_contextual_minutes": float(args.contextual_prior_run_minutes),
        },
        "rows": report_rows,
        "conclusion": {
            "methodology_correction": "confirmed",
            "prior_short_run_can_support_final_verdict": False,
            "reason": conclusion_note,
        },
    }

    execution_plan = {
        "schema_version": "phase7_shadow_window_plan_v0",
        "generated_ts_utc": utc_now_iso(),
        "principle": "Do not issue a tradability verdict before the strategy has cleared its minimum observation window.",
        "notes": [
            "The Phase7 smoke run is valid as infra and instrumentation evidence only.",
            "It is not long enough to support a tradability fail verdict for any shortlisted strategy.",
            "Frequency bands are intentionally broad and conservative because direct order-frequency evidence is still sparse.",
        ],
        "short_run_judgeable_strategy_ids": short_run_judgeable,
        "multi_hour_strategy_ids": multi_hour,
        "multi_day_strategy_ids": multi_day,
        "frequency_groups": {
            HIGH_FREQUENCY: {
                "recommended_shadow_window_hours": 6,
                "judgment_rule": "Only after >=2h minimum, with >=6h preferred, and still no signal/decision activity.",
            },
            MEDIUM_FREQUENCY: {
                "recommended_shadow_window_hours": 12,
                "judgment_rule": "Need >=6h minimum and preferably >=12h before treating inactivity as evidence.",
            },
            LOW_FREQUENCY: {
                "recommended_shadow_window_hours": 48,
                "judgment_rule": "Need >=24h minimum and preferably 48-72h before treating inactivity as evidence.",
            },
            UNKNOWN_FREQUENCY: {
                "recommended_shadow_window_hours": 48,
                "judgment_rule": "Treat as unknown; default to long observation until better expectation instrumentation exists.",
            },
        },
        "group_membership": group_membership,
    }

    write_json(out_report_path, expectation_report)
    write_json(out_plan_path, execution_plan)

    print("EXPECTATION_AUDIT_COMPLETE")
    print(f"report_json={out_report_path}")
    print(f"plan_json={out_plan_path}")
    print(f"shortlist_count={len(report_rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ExpectationAuditError as exc:
        print(f"PHASE7_EXPECTATION_AUDIT_ERROR: {exc}")
        raise SystemExit(1)
