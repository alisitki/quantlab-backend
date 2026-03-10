#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_BINDING_ARTIFACT = ROOT / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"
DEFAULT_CHILD_TOOL = ROOT / "tools" / "run-bound-shadow-launch-with-telegram-v0.py"
DEFAULT_SESSION_JSON = ROOT / "tools" / "shadow_state" / "momentum_long_shadow_session_v0.json"
DEFAULT_SESSION_ARTIFACTS_DIR = ROOT / "tools" / "shadow_state" / "momentum_long_shadow_session_artifacts_v0"
DEFAULT_FUTURES_PAPER_LEDGER_JSON = ROOT / "tools" / "shadow_state" / "shadow_futures_paper_ledger_v1.json"
DEFAULT_PER_RUN_TIMEOUT_SEC = 240
DEFAULT_RUN_MAX_DURATION_SEC = 180
DEFAULT_HEARTBEAT_MS = 5000
DEFAULT_COOLDOWN_SEC = 5
DEFAULT_FAILURE_COOLDOWN_SEC = 15
DEFAULT_MAX_CONSECUTIVE_INVALID_CYCLES = 2
DEFAULT_AUDIT_BASE_DIR = Path("/tmp/quantlab-momentum-long-shadow-session-audit-v0")
DEFAULT_OUT_BASE_DIR = Path("/tmp/quantlab-momentum-long-shadow-session-out-v0")
DEFAULT_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
SCHEMA_VERSION = "momentum_long_shadow_session_v0"
BINDING_SCHEMA_VERSION = "candidate_strategy_runtime_binding_v0"
TELEGRAM_CHILD_SCHEMA_VERSION = "shadow_bound_launch_telegram_v0"
BOUND_SHADOW_RUNNABLE = "BOUND_SHADOW_RUNNABLE"
TARGET_FAMILY_ID = "momentum_v1"
FUNDING_AWARE_PROFITABILITY_STATUSES = {
    "NET_AFTER_FEES_AND_FUNDING",
    "NET_MARK_TO_MARKET_AFTER_FEES_AND_FUNDING",
    "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
}


STOP_REQUESTED = False
STOP_REASON = ""


class MomentumLongShadowSessionError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise MomentumLongShadowSessionError(message)


def request_stop(reason: str) -> None:
    global STOP_REQUESTED, STOP_REASON
    STOP_REQUESTED = True
    STOP_REASON = str(reason or "manual_stop")


def _signal_handler(signum: int, _frame: Any) -> None:
    if signum == signal.SIGINT:
        request_stop("signal:SIGINT")
        return
    if signum == signal.SIGTERM:
        request_stop("signal:SIGTERM")
        return
    request_stop(f"signal:{signum}")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_env_file() -> Path:
    override = str(os.environ.get("QUANTLAB_ENV_FILE") or "").strip()
    if override:
        return Path(override).resolve()
    return DEFAULT_ENV_FILE


def load_env_defaults_from_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if str(os.environ.get(key) or "").strip():
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key in os.environ:
            continue
        os.environ[key] = value


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a manually-stoppable momentum_v1 long shadow session.")
    parser.add_argument("--binding-artifact", default=str(DEFAULT_BINDING_ARTIFACT))
    parser.add_argument("--child-tool", default=str(DEFAULT_CHILD_TOOL))
    parser.add_argument("--strategy-id", default="")
    parser.add_argument("--pack-id", default="")
    parser.add_argument("--session-json", default=str(DEFAULT_SESSION_JSON))
    parser.add_argument("--session-artifacts-dir", default=str(DEFAULT_SESSION_ARTIFACTS_DIR))
    parser.add_argument("--futures-paper-ledger-json", default=str(DEFAULT_FUTURES_PAPER_LEDGER_JSON))
    parser.add_argument("--audit-base-dir", default=str(DEFAULT_AUDIT_BASE_DIR))
    parser.add_argument("--out-base-dir", default=str(DEFAULT_OUT_BASE_DIR))
    parser.add_argument("--per-run-timeout-sec", type=int, default=DEFAULT_PER_RUN_TIMEOUT_SEC)
    parser.add_argument("--run-max-duration-sec", type=int, default=DEFAULT_RUN_MAX_DURATION_SEC)
    parser.add_argument("--heartbeat-ms", type=int, default=DEFAULT_HEARTBEAT_MS)
    parser.add_argument("--cooldown-sec", type=int, default=DEFAULT_COOLDOWN_SEC)
    parser.add_argument("--failure-cooldown-sec", type=int, default=DEFAULT_FAILURE_COOLDOWN_SEC)
    parser.add_argument("--max-consecutive-invalid-cycles", type=int, default=DEFAULT_MAX_CONSECUTIVE_INVALID_CYCLES)
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument("--session-max-runtime-sec", type=int, default=0)
    parser.add_argument("--stop-file", default="")
    parser.add_argument("--telegram-api-base-url", default=os.environ.get("TELEGRAM_API_BASE_URL", DEFAULT_TELEGRAM_API_BASE_URL))
    parser.add_argument("--telegram-message-style", choices=["compact", "verbose"], default="compact")
    parser.add_argument("--telegram-dry-run", action="store_true")
    args = parser.parse_args(argv)
    args.strategy_id = str(args.strategy_id or "").strip()
    args.pack_id = str(args.pack_id or "").strip()
    if args.per_run_timeout_sec <= 0:
        fail(f"invalid_per_run_timeout_sec:{args.per_run_timeout_sec}")
    if args.run_max_duration_sec <= 0:
        fail(f"invalid_run_max_duration_sec:{args.run_max_duration_sec}")
    if args.heartbeat_ms <= 0:
        fail(f"invalid_heartbeat_ms:{args.heartbeat_ms}")
    if args.cooldown_sec < 0:
        fail(f"invalid_cooldown_sec:{args.cooldown_sec}")
    if args.failure_cooldown_sec < 0:
        fail(f"invalid_failure_cooldown_sec:{args.failure_cooldown_sec}")
    if args.max_consecutive_invalid_cycles <= 0:
        fail(f"invalid_max_consecutive_invalid_cycles:{args.max_consecutive_invalid_cycles}")
    if args.max_cycles < 0:
        fail(f"invalid_max_cycles:{args.max_cycles}")
    if args.session_max_runtime_sec < 0:
        fail(f"invalid_session_max_runtime_sec:{args.session_max_runtime_sec}")
    return args


def mask_secret(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) <= 8:
        return "*" * len(raw)
    return f"{raw[:4]}...{raw[-4:]}"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def row_pack_id(item: dict[str, Any]) -> str:
    return str(item.get("pack_id") or "").strip()


def row_strategy_id(item: dict[str, Any]) -> str:
    return str(item.get("strategy_id") or "").strip()


def short_id(value: str, *, tail: int = 8) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    if len(raw) <= tail:
        return raw
    return f"...{raw[-tail:]}"


def short_session_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    prefix = "momentum_session_"
    if raw.startswith(prefix):
        raw = raw[len(prefix):]
    raw = raw.replace("_pid", "/p")
    return raw


def primary_symbol(item: dict[str, Any]) -> str:
    values = [str(value).upper() for value in list(item.get("symbols") or []) if str(value).strip()]
    if not values:
        return "?"
    return values[0]


def compact_identity(item: dict[str, Any]) -> str:
    family = str(item.get("family_id") or "").strip() or TARGET_FAMILY_ID
    symbol = primary_symbol(item)
    exchange = str(item.get("exchange") or "").strip() or "unknown_exchange"
    return f"{family} | {symbol} | {exchange}"


def selection_mode(args: argparse.Namespace) -> str:
    if args.strategy_id and args.pack_id:
        return "STRATEGY_ID_AND_PACK_ID"
    if args.strategy_id:
        return "STRATEGY_ID"
    if args.pack_id:
        return "PACK_ID"
    return "FIRST_BOUND_MOMENTUM"


def load_binding_artifact(path: Path) -> dict[str, Any]:
    obj = load_json(path, "binding_artifact")
    if str(obj.get("schema_version") or "").strip() != BINDING_SCHEMA_VERSION:
        fail(f"binding_artifact_schema_mismatch:{path}")
    items = obj.get("items")
    if not isinstance(items, list):
        fail(f"binding_artifact_items_invalid:{path}")
    return obj


def is_momentum_bound(item: dict[str, Any]) -> bool:
    return (
        str(item.get("family_id") or "").strip() == TARGET_FAMILY_ID
        and str(item.get("runtime_binding_status") or "").strip() == BOUND_SHADOW_RUNNABLE
    )


def select_momentum_item(items: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    by_strategy = None
    by_pack = None
    if args.strategy_id:
        by_strategy = next((item for item in items if row_strategy_id(item) == args.strategy_id), None)
        if by_strategy is None:
            fail(f"strategy_id_not_found:{args.strategy_id}")
    if args.pack_id:
        by_pack = next((item for item in items if row_pack_id(item) == args.pack_id), None)
        if by_pack is None:
            fail(f"pack_id_not_found:{args.pack_id}")
    if by_strategy is not None and by_pack is not None:
        if row_strategy_id(by_strategy) != row_strategy_id(by_pack):
            fail(f"selector_conflict:strategy_id={args.strategy_id}:pack_id={args.pack_id}")
        selected = by_strategy
    elif by_strategy is not None:
        selected = by_strategy
    elif by_pack is not None:
        selected = by_pack
    else:
        selected = next((item for item in items if is_momentum_bound(item)), None)
        if selected is None:
            fail("no_bound_shadow_runnable_momentum_rows")
    if str(selected.get("family_id") or "").strip() != TARGET_FAMILY_ID:
        fail(f"selected_row_wrong_family:{str(selected.get('family_id') or '').strip()}")
    if str(selected.get("runtime_binding_status") or "").strip() != BOUND_SHADOW_RUNNABLE:
        fail(f"selected_row_not_bound:{str(selected.get('runtime_binding_status') or '').strip()}")
    return selected


def build_session_binding_artifact(source: dict[str, Any], selected: dict[str, Any], *, generated_ts_utc: str) -> dict[str, Any]:
    row = json.loads(json.dumps(selected))
    return {
        "schema_version": BINDING_SCHEMA_VERSION,
        "generated_ts_utc": generated_ts_utc,
        "source_candidate_strategy_contract_json": str(source.get("source_candidate_strategy_contract_json") or ""),
        "source_binding_map_json": str(source.get("source_binding_map_json") or ""),
        "source_row_count": 1,
        "translated_spec_count": 1 if str(row.get("translation_status") or "").strip() == "TRANSLATABLE" else 0,
        "bound_shadow_runnable_count": 1,
        "unbound_no_runtime_impl_count": 0,
        "unbound_config_gap_count": 0,
        "unbound_translation_rejected_count": 0,
        "bindable_family_ids": [TARGET_FAMILY_ID],
        "items": [row],
    }


def telegram_config(args: argparse.Namespace) -> dict[str, Any]:
    token = str(os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if args.telegram_dry_run:
        return {
            "enabled": True,
            "send_mode": "DRY_RUN",
            "token": token,
            "chat_id": chat_id,
            "token_masked": mask_secret(token),
            "chat_id_masked": mask_secret(chat_id),
            "api_base_url": str(args.telegram_api_base_url).rstrip("/"),
        }
    if not token or not chat_id:
        fail("missing_telegram_env")
    return {
        "enabled": True,
        "send_mode": "LIVE",
        "token": token,
        "chat_id": chat_id,
        "token_masked": mask_secret(token),
        "chat_id_masked": mask_secret(chat_id),
        "api_base_url": str(args.telegram_api_base_url).rstrip("/"),
    }


def send_telegram_message(config: dict[str, Any], *, text: str) -> tuple[bool, str]:
    if config["send_mode"] == "DRY_RUN":
        return True, "DRY_RUN"
    url = f"{config['api_base_url']}/bot{config['token']}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": config["chat_id"],
            "text": text,
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status = getattr(response, "status", 200)
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return False, f"TELEGRAM_HTTP_{exc.code}"
    except urllib.error.URLError:
        return False, "TELEGRAM_FETCH_ERROR"
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "TELEGRAM_INVALID_JSON"
    if status == 200 and parsed.get("ok") is True:
        return True, "OK"
    description = str(parsed.get("description") or "").strip()
    if description:
        return False, f"TELEGRAM_API_{description.replace(' ', '_').upper()[:40]}"
    return False, f"TELEGRAM_HTTP_{status}"


def append_message_result(state: dict[str, Any], *, event_type: str, sent: bool, reason: str) -> None:
    state["telegram_messages_attempted"] += 1
    if sent:
        state["telegram_messages_sent"] += 1
        state["session_event_types_sent"].append(event_type)
    else:
        state["telegram_error_count"] += 1
    state["session_event_types_attempted"].append(event_type)
    state["latest_message_results"].append({"event_type": event_type, "sent": bool(sent), "reason": str(reason or "")})
    state["latest_message_results"] = state["latest_message_results"][-20:]


def send_session_message(
    state: dict[str, Any],
    telegram: dict[str, Any],
    *,
    event_type: str,
    text: str,
) -> None:
    sent, reason = send_telegram_message(telegram, text=text)
    append_message_result(state, event_type=event_type, sent=sent, reason=reason)


def command_preview(parts: list[str]) -> str:
    return " ".join(parts)


def create_session_id() -> str:
    return datetime.now(timezone.utc).strftime("momentum_session_%Y%m%dT%H%M%SZ") + f"_pid{os.getpid()}"


def cycle_dir_name(cycle_index: int) -> str:
    return f"cycle_{cycle_index:04d}"


def build_cycle_paths(
    *,
    session_artifacts_dir: Path,
    audit_base_dir: Path,
    out_base_dir: Path,
    cycle_index: int,
) -> dict[str, Path]:
    cycle_dir = session_artifacts_dir / cycle_dir_name(cycle_index)
    return {
        "cycle_dir": cycle_dir,
        "bound_launch_result_json": cycle_dir / "shadow_bound_launch_v0.json",
        "telegram_result_json": cycle_dir / "shadow_bound_launch_telegram_v0.json",
        "generated_watchlist_json": cycle_dir / "shadow_bound_launch_watchlist_v0.json",
        "child_launch_result_json": cycle_dir / "shadow_bound_long_shadow_launch_v0.json",
        "child_batch_result_json": cycle_dir / "shadow_bound_long_shadow_batch_result_v0.json",
        "batch_stdout_log": cycle_dir / "shadow_bound_long_shadow_batch_stdout_v0.log",
        "batch_stderr_log": cycle_dir / "shadow_bound_long_shadow_batch_stderr_v0.log",
        "audit_base_dir": audit_base_dir / cycle_dir_name(cycle_index),
        "out_dir": out_base_dir / cycle_dir_name(cycle_index),
    }


def build_child_command(
    args: argparse.Namespace,
    *,
    session_binding_artifact: Path,
    selected: dict[str, Any],
    cycle_paths: dict[str, Path],
) -> list[str]:
    cmd = [
        sys.executable,
        str(Path(args.child_tool).resolve()),
        "--binding-artifact",
        str(session_binding_artifact.resolve()),
        "--strategy-id",
        row_strategy_id(selected),
        "--bound-launch-result-json",
        str(cycle_paths["bound_launch_result_json"].resolve()),
        "--telegram-result-json",
        str(cycle_paths["telegram_result_json"].resolve()),
        "--generated-watchlist-json",
        str(cycle_paths["generated_watchlist_json"].resolve()),
        "--child-launch-result-json",
        str(cycle_paths["child_launch_result_json"].resolve()),
        "--child-batch-result-json",
        str(cycle_paths["child_batch_result_json"].resolve()),
        "--batch-stdout-log",
        str(cycle_paths["batch_stdout_log"].resolve()),
        "--batch-stderr-log",
        str(cycle_paths["batch_stderr_log"].resolve()),
        "--audit-base-dir",
        str(cycle_paths["audit_base_dir"].resolve()),
        "--out-dir",
        str(cycle_paths["out_dir"].resolve()),
        "--per-run-timeout-sec",
        str(int(args.per_run_timeout_sec)),
        "--run-max-duration-sec",
        str(int(args.run_max_duration_sec)),
        "--heartbeat-ms",
        str(int(args.heartbeat_ms)),
        "--telegram-api-base-url",
        str(args.telegram_api_base_url),
        "--telegram-message-style",
        str(args.telegram_message_style),
    ]
    if args.telegram_dry_run:
        cmd.append("--telegram-dry-run")
    return cmd


def start_child_process(cmd: list[str]) -> subprocess.Popen[str]:
    return subprocess.Popen(cmd, cwd=str(ROOT), text=True, start_new_session=True)


def terminate_child_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.time() + 10.0
    while proc.poll() is None and time.time() < deadline:
        time.sleep(0.2)
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return


def stop_file_requested(stop_file: Path | None) -> bool:
    if stop_file is None:
        return False
    return stop_file.exists()


def wait_for_cycle_completion(
    proc: subprocess.Popen[str],
    *,
    stop_file: Path | None,
) -> tuple[int | None, bool, str]:
    interrupted = False
    interrupt_reason = ""
    while proc.poll() is None:
        if STOP_REQUESTED:
            interrupted = True
            interrupt_reason = STOP_REASON or "manual_stop"
            break
        if stop_file_requested(stop_file):
            interrupted = True
            interrupt_reason = f"stop_file:{stop_file}"
            request_stop(interrupt_reason)
            break
        time.sleep(0.5)
    if interrupted:
        terminate_child_process(proc)
        try:
            return proc.wait(timeout=10), True, interrupt_reason
        except subprocess.TimeoutExpired:
            return None, True, interrupt_reason
    return proc.wait(), False, ""


def load_cycle_artifacts(cycle_paths: dict[str, Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    bound_launch = load_json(cycle_paths["bound_launch_result_json"], "cycle_bound_launch_result")
    telegram_result = load_json(cycle_paths["telegram_result_json"], "cycle_telegram_result")
    if str(telegram_result.get("schema_version") or "").strip() != TELEGRAM_CHILD_SCHEMA_VERSION:
        fail(f"cycle_telegram_result_schema_mismatch:{cycle_paths['telegram_result_json']}")
    return bound_launch, telegram_result


def load_futures_item(futures_paper_ledger_json: Path, *, pack_id: str, live_run_id: str) -> dict[str, Any] | None:
    payload = load_json(futures_paper_ledger_json, "futures_paper_ledger_json")
    items = payload.get("items")
    if not isinstance(items, list):
        fail(f"futures_paper_ledger_items_invalid:{futures_paper_ledger_json}")
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("selected_pack_id") or "").strip() != pack_id:
            continue
        if str(item.get("live_run_id") or "").strip() != live_run_id:
            continue
        return item
    return None


def collect_action_labels(item: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    for raw in list(item.get("action_sequence") or []):
        if not isinstance(raw, dict):
            continue
        label = str(raw.get("action") or "").strip()
        if not label or label == "HOLD":
            continue
        labels.append(label)
    return labels


def render_session_started_message(selected: dict[str, Any], *, session_id: str, cycle_profile: dict[str, Any]) -> str:
    if cycle_profile.get("telegram_message_style") == "verbose":
        symbols = ", ".join(str(value).upper() for value in list(selected.get("symbols") or []))
        return "\n".join(
            [
                "QuantLab Momentum Session START",
                "Mode: shadow/paper",
                f"Session ID: {session_id}",
                f"Family: {TARGET_FAMILY_ID}",
                f"Strategy ID: {row_strategy_id(selected)}",
                f"Pack: {row_pack_id(selected)}",
                f"Exchange/Stream: {str(selected.get('exchange') or '').strip()}/{str(selected.get('stream') or '').strip()}",
                f"Symbols: {symbols}",
                f"Cycle Profile: timeout={cycle_profile['per_run_timeout_sec']}s run={cycle_profile['run_max_duration_sec']}s heartbeat={cycle_profile['heartbeat_ms']}ms cooldown={cycle_profile['cooldown_sec']}s",
                "Note: paper fill-backed events are deterministic paper execution, not live exchange execution.",
            ]
        )
    return "\n".join(
        [
            "🟢 Momentum session started",
            f"{compact_identity(selected)}",
            f"Session {short_session_id(session_id)}",
            f"Cycle {cycle_profile['run_max_duration_sec']}s | timeout {cycle_profile['per_run_timeout_sec']}s | cooldown {cycle_profile['cooldown_sec']}s",
            "Mode: shadow/paper",
            "Caveat: paper fills only, not live execution.",
        ]
    )


def render_session_stop_message(state: dict[str, Any]) -> str:
    if state.get("telegram_message_style") == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Session STOPPED BY USER",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Strategy ID: {state['selected_strategy_id']}",
                f"Pack: {state['selected_pack_id']}",
                f"Cycle Count: {state['cycle_count']}",
                f"Stop Reason: {state['stop_reason']}",
            ]
        )
    return "\n".join(
        [
            "⚠️ Momentum session stopped",
            f"Session {short_session_id(state['session_id'])} | cycles {state['cycle_count']}",
            f"Reason: {state['stop_reason']}",
            "Mode: shadow/paper",
        ]
    )


def render_session_failed_message(state: dict[str, Any], reason: str) -> str:
    if state.get("telegram_message_style") == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Session FAILED",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Strategy ID: {state['selected_strategy_id']}",
                f"Pack: {state['selected_pack_id']}",
                f"Reason: {reason}",
            ]
        )
    return "\n".join(
        [
            "🔴 Momentum session failed",
            f"Session {short_session_id(state['session_id'])}",
            f"Reason: {reason}",
            "Mode: shadow/paper",
        ]
    )


def render_session_summary_message(state: dict[str, Any]) -> str:
    if state.get("telegram_message_style") == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Session SUMMARY",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Status: {state['session_status']}",
                f"Cycles: {state['cycle_count']}",
                f"Valid cycles: {state['valid_cycle_count']}",
                f"Invalid cycles: {state['invalid_cycle_count']}",
                f"Cycles with execution activity: {state['valid_with_execution_activity_count']}",
                f"Total processed events: {state['total_processed_event_count']}",
                f"Total matched execution events: {state['total_matched_execution_event_count']}",
                f"Total matched trades: {state['total_matched_trade_count']}",
                f"Total fill-backed runs: {state['total_fill_backed_run_count']}",
                f"Funding-aware cycles: {state['funding_aware_cycle_count']}",
                "Caveats: flat fee model; no leverage/margin/liquidation realism; no live exchange execution semantics.",
            ]
        )
    return "\n".join(
        [
            "📋 Momentum session summary",
            f"Status {state['session_status']} | session {short_session_id(state['session_id'])}",
            f"Cycles {state['cycle_count']} | exec {state['valid_with_execution_activity_count']} | invalid {state['invalid_cycle_count']}",
            f"Matched events {state['total_matched_execution_event_count']} | trades {state['total_matched_trade_count']}",
            f"Fill-backed runs {state['total_fill_backed_run_count']} | funding-aware {state['funding_aware_cycle_count']}",
            "Caveat: flat fee model; no leverage/margin/liquidation realism.",
        ]
    )


def render_cycle_paper_activity_message(
    *,
    cycle_index: int,
    state: dict[str, Any],
    futures_item: dict[str, Any],
    actions: list[str],
) -> str:
    if state.get("telegram_message_style") == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Cycle PAPER ACTIVITY",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Cycle: {cycle_index}",
                f"Strategy ID: {state['selected_strategy_id']}",
                f"Pack: {state['selected_pack_id']}",
                f"Paper Run Status: {str(futures_item.get('paper_run_status') or '').strip()}",
                f"Actions: {', '.join(actions)}",
                f"Final Position: {str(futures_item.get('final_position_direction') or '').strip()} qty={futures_item.get('final_position_qty')}",
                f"Fill Events: {futures_item.get('fill_event_count')}",
                "Note: paper fill-backed actions are deterministic paper fills, not live executed trades.",
            ]
        )
    position_direction = str(futures_item.get("final_position_direction") or "").strip() or "?"
    fill_count = futures_item.get("fill_event_count")
    return "\n".join(
        [
            f"📈 Cycle {cycle_index} paper activity",
            f"Action: {', '.join(actions) if actions else 'FILL_BACKED'}",
            f"Position: {position_direction} qty={futures_item.get('final_position_qty')} | fills {fill_count}",
            f"Run: {str(futures_item.get('paper_run_status') or '').strip()}",
            "Paper fill-backed, not live executed.",
        ]
    )


def render_cycle_profitability_message(*, cycle_index: int, state: dict[str, Any], futures_item: dict[str, Any]) -> str:
    if state.get("telegram_message_style") == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Cycle FUNDING-AWARE PROFITABILITY",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Cycle: {cycle_index}",
                f"Strategy ID: {state['selected_strategy_id']}",
                f"Pack: {state['selected_pack_id']}",
                f"Profitability Status: {str(futures_item.get('profitability_status') or '').strip()}",
                f"Funding Support: {str(futures_item.get('funding_support_status') or '').strip()}",
                f"Net MTM After Funding: {futures_item.get('mark_to_market_pnl_quote_net_after_funding')}",
                f"Net MTM After Funding + Exit Estimate: {futures_item.get('mark_to_market_pnl_quote_net_after_funding_and_exit_estimate')}",
                "Note: funding-aware means run-level funding was accounted for or exactly classified as zero; leverage/margin/liquidation remain unsupported.",
            ]
        )
    return "\n".join(
        [
            f"💰 Cycle {cycle_index} profitability",
            f"Status: {str(futures_item.get('profitability_status') or '').strip()}",
            f"Net after funding: {futures_item.get('mark_to_market_pnl_quote_net_after_funding')}",
            f"Net + exit est: {futures_item.get('mark_to_market_pnl_quote_net_after_funding_and_exit_estimate')}",
            f"Funding: {str(futures_item.get('funding_support_status') or '').strip()}",
            "Funding-aware; leverage/margin/liquidation still unsupported.",
        ]
    )


def render_cycle_funding_message(*, cycle_index: int, state: dict[str, Any], futures_item: dict[str, Any]) -> str:
    if state.get("telegram_message_style") == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Cycle FUNDING COST",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Cycle: {cycle_index}",
                f"Strategy ID: {state['selected_strategy_id']}",
                f"Pack: {state['selected_pack_id']}",
                f"Funding Cost Quote: {futures_item.get('funding_cost_quote')}",
                f"Funding Windows Applied: {futures_item.get('funding_applied_count')}",
                f"Funding Alignment: {str(futures_item.get('funding_alignment_status') or '').strip()}",
                "Note: funding cost is run-level paper accounting from persisted funding and mark-price events, not exchange reconciliation.",
            ]
        )
    return "\n".join(
        [
            f"💰 Cycle {cycle_index} funding",
            f"Funding cost: {futures_item.get('funding_cost_quote')}",
            f"Applied windows: {futures_item.get('funding_applied_count')}",
            f"Alignment: {str(futures_item.get('funding_alignment_status') or '').strip()}",
            "Run-level paper funding, not exchange reconciliation.",
        ]
    )


def render_cycle_artifact_missing_message(*, cycle_index: int, state: dict[str, Any]) -> str:
    if state.get("telegram_message_style") == "verbose":
        return "\n".join(
            [
                "QuantLab Momentum Cycle ARTIFACT MISSING",
                "Mode: shadow/paper",
                f"Session ID: {state['session_id']}",
                f"Cycle: {cycle_index}",
                f"Strategy ID: {state['selected_strategy_id']}",
                f"Pack: {state['selected_pack_id']}",
                "Reason: futures paper ledger item missing for the completed cycle",
            ]
        )
    return "\n".join(
        [
            f"⚠️ Cycle {cycle_index} artifact missing",
            "Futures paper ledger item missing for completed cycle.",
            f"Session {short_session_id(state['session_id'])}",
            "Mode: shadow/paper",
        ]
    )


def initial_session_state(
    *,
    args: argparse.Namespace,
    telegram: dict[str, Any],
    selected: dict[str, Any],
    session_id: str,
    session_artifacts_dir: Path,
    session_binding_artifact: Path,
) -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "schema_version": SCHEMA_VERSION,
        "session_id": session_id,
        "generated_ts_utc": now,
        "last_updated_ts_utc": now,
        "session_status": "STARTING",
        "start_ts_utc": now,
        "stop_ts_utc": "",
        "stop_reason": "",
        "selection_mode": selection_mode(args),
        "source_binding_artifact_path": str(Path(args.binding_artifact).resolve()),
        "session_binding_artifact_json": str(session_binding_artifact.resolve()),
        "selected_rank": int(selected.get("rank") or 0),
        "selected_strategy_id": row_strategy_id(selected),
        "selected_pack_id": row_pack_id(selected),
        "selected_family_id": str(selected.get("family_id") or "").strip(),
        "selected_exchange": str(selected.get("exchange") or "").strip(),
        "selected_stream": str(selected.get("stream") or "").strip(),
        "selected_symbols": list(selected.get("symbols") or []),
        "cycle_profile": {
            "per_run_timeout_sec": int(args.per_run_timeout_sec),
            "run_max_duration_sec": int(args.run_max_duration_sec),
            "heartbeat_ms": int(args.heartbeat_ms),
            "cooldown_sec": int(args.cooldown_sec),
            "failure_cooldown_sec": int(args.failure_cooldown_sec),
            "telegram_message_style": str(args.telegram_message_style),
        },
        "max_consecutive_invalid_cycles": int(args.max_consecutive_invalid_cycles),
        "max_cycles": int(args.max_cycles),
        "session_max_runtime_sec": int(args.session_max_runtime_sec),
        "stop_file": str(Path(args.stop_file).resolve()) if str(args.stop_file or "").strip() else "",
        "telegram_send_mode": telegram["send_mode"],
        "telegram_message_style": str(args.telegram_message_style),
        "telegram_api_base_url": telegram["api_base_url"],
        "telegram_bot_token_masked": telegram["token_masked"],
        "telegram_chat_id_masked": telegram["chat_id_masked"],
        "cycle_count": 0,
        "valid_cycle_count": 0,
        "invalid_cycle_count": 0,
        "valid_with_execution_activity_count": 0,
        "total_processed_event_count": 0,
        "total_matched_execution_event_count": 0,
        "total_matched_trade_count": 0,
        "total_fill_backed_run_count": 0,
        "funding_aware_cycle_count": 0,
        "telegram_messages_attempted": 0,
        "telegram_messages_sent": 0,
        "telegram_error_count": 0,
        "session_event_types_attempted": [],
        "session_event_types_sent": [],
        "latest_message_results": [],
        "consecutive_invalid_cycles": 0,
        "latest_cycle_index": 0,
        "latest_cycle_status": "NOT_STARTED",
        "latest_cycle_started_ts_utc": "",
        "latest_cycle_finished_ts_utc": "",
        "latest_cycle_processed_event_count": 0,
        "latest_cycle_matched_execution_event_count": 0,
        "latest_cycle_matched_trade_count": 0,
        "latest_cycle_fill_backed": False,
        "latest_cycle_funding_aware": False,
        "latest_cycle_funding_cost_quote": None,
        "latest_cycle_profitability_status": "",
        "latest_cycle_paper_run_status": "",
        "latest_cycle_bound_launch_result_json": "",
        "latest_cycle_telegram_result_json": "",
        "latest_cycle_child_launch_result_json": "",
        "latest_cycle_child_batch_result_json": "",
        "latest_cycle_batch_stdout_log": "",
        "latest_cycle_batch_stderr_log": "",
        "latest_cycle_audit_base_dir": "",
        "latest_cycle_out_dir": "",
        "session_artifacts_dir": str(session_artifacts_dir.resolve()),
    }


def update_state_file(session_json: Path, state: dict[str, Any], *, status: str | None = None, stop_reason: str | None = None) -> None:
    state["last_updated_ts_utc"] = utc_now_iso()
    if status is not None:
        state["session_status"] = status
    if stop_reason is not None:
        state["stop_reason"] = str(stop_reason or "")
    if state["session_status"] in {"STOPPED_BY_USER", "COMPLETED_LIMIT", "FAILED"} and not state.get("stop_ts_utc"):
        state["stop_ts_utc"] = utc_now_iso()
    write_json(session_json, state)


def apply_child_telegram_counts(state: dict[str, Any], telegram_result: dict[str, Any]) -> None:
    state["telegram_messages_attempted"] += int(telegram_result.get("messages_attempted") or 0)
    state["telegram_messages_sent"] += int(telegram_result.get("messages_sent") or 0)
    state["telegram_error_count"] += int(telegram_result.get("error_count") or 0)


def update_cycle_state(
    state: dict[str, Any],
    *,
    cycle_index: int,
    cycle_status: str,
    bound_launch_result: dict[str, Any],
    futures_item: dict[str, Any] | None,
    cycle_paths: dict[str, Path],
) -> None:
    processed = int(bound_launch_result.get("summary_processed_event_count") or 0)
    matched_events = int(bound_launch_result.get("matched_execution_event_count") or 0)
    matched_trades = int(bound_launch_result.get("matched_trade_count") or 0)
    fill_backed = bool(futures_item and str(futures_item.get("paper_run_status") or "").strip().startswith("FILL_BACKED_"))
    funding_aware = bool(futures_item and str(futures_item.get("profitability_status") or "").strip() in FUNDING_AWARE_PROFITABILITY_STATUSES)
    state["cycle_count"] += 1
    if cycle_status in {"VALID_NO_EXECUTION_ACTIVITY", "VALID_WITH_EXECUTION_ACTIVITY"}:
        state["valid_cycle_count"] += 1
        state["consecutive_invalid_cycles"] = 0
    else:
        state["invalid_cycle_count"] += 1
        state["consecutive_invalid_cycles"] += 1
    if cycle_status == "VALID_WITH_EXECUTION_ACTIVITY":
        state["valid_with_execution_activity_count"] += 1
    state["total_processed_event_count"] += processed
    state["total_matched_execution_event_count"] += matched_events
    state["total_matched_trade_count"] += matched_trades
    if fill_backed:
        state["total_fill_backed_run_count"] += 1
    if funding_aware:
        state["funding_aware_cycle_count"] += 1
    state["latest_cycle_index"] = cycle_index
    state["latest_cycle_status"] = cycle_status
    state["latest_cycle_finished_ts_utc"] = utc_now_iso()
    state["latest_cycle_processed_event_count"] = processed
    state["latest_cycle_matched_execution_event_count"] = matched_events
    state["latest_cycle_matched_trade_count"] = matched_trades
    state["latest_cycle_fill_backed"] = fill_backed
    state["latest_cycle_funding_aware"] = funding_aware
    state["latest_cycle_funding_cost_quote"] = futures_item.get("funding_cost_quote") if futures_item else None
    state["latest_cycle_profitability_status"] = str(futures_item.get("profitability_status") or "").strip() if futures_item else ""
    state["latest_cycle_paper_run_status"] = str(futures_item.get("paper_run_status") or "").strip() if futures_item else ""
    state["latest_cycle_bound_launch_result_json"] = str(cycle_paths["bound_launch_result_json"].resolve())
    state["latest_cycle_telegram_result_json"] = str(cycle_paths["telegram_result_json"].resolve())
    state["latest_cycle_child_launch_result_json"] = str(cycle_paths["child_launch_result_json"].resolve())
    state["latest_cycle_child_batch_result_json"] = str(cycle_paths["child_batch_result_json"].resolve())
    state["latest_cycle_batch_stdout_log"] = str(cycle_paths["batch_stdout_log"].resolve())
    state["latest_cycle_batch_stderr_log"] = str(cycle_paths["batch_stderr_log"].resolve())
    state["latest_cycle_audit_base_dir"] = str(cycle_paths["audit_base_dir"].resolve())
    state["latest_cycle_out_dir"] = str(cycle_paths["out_dir"].resolve())


def session_limit_reached(args: argparse.Namespace, *, cycle_count: int, session_started_monotonic: float) -> tuple[bool, str]:
    if args.max_cycles > 0 and cycle_count >= args.max_cycles:
        return True, f"max_cycles_reached:{args.max_cycles}"
    if args.session_max_runtime_sec > 0 and (time.monotonic() - session_started_monotonic) >= args.session_max_runtime_sec:
        return True, f"session_max_runtime_reached:{args.session_max_runtime_sec}"
    return False, ""


def maybe_sleep(duration_sec: int, *, stop_file: Path | None) -> None:
    deadline = time.monotonic() + max(duration_sec, 0)
    while time.monotonic() < deadline:
        if STOP_REQUESTED:
            return
        if stop_file_requested(stop_file):
            request_stop(f"stop_file:{stop_file}")
            return
        time.sleep(0.5)


def main(argv: list[str] | None = None) -> int:
    session_json = Path(DEFAULT_SESSION_JSON)
    state: dict[str, Any] | None = None
    try:
        load_env_defaults_from_file(resolve_env_file())
        args = parse_args(argv or sys.argv[1:])
        session_json = Path(args.session_json).resolve()
        stop_file = Path(args.stop_file).resolve() if str(args.stop_file or "").strip() else None
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        source_binding = load_binding_artifact(Path(args.binding_artifact).resolve())
        selected = select_momentum_item(list(source_binding.get("items") or []), args)
        telegram = telegram_config(args)

        session_id = create_session_id()
        session_artifacts_dir = Path(args.session_artifacts_dir).resolve() / session_id
        session_artifacts_dir.mkdir(parents=True, exist_ok=True)
        session_binding_artifact = session_artifacts_dir / "session_binding_artifact.json"
        write_json(
            session_binding_artifact,
            build_session_binding_artifact(source_binding, selected, generated_ts_utc=utc_now_iso()),
        )

        state = initial_session_state(
            args=args,
            telegram=telegram,
            selected=selected,
            session_id=session_id,
            session_artifacts_dir=session_artifacts_dir,
            session_binding_artifact=session_binding_artifact,
        )
        update_state_file(session_json, state, status="RUNNING")

        send_session_message(
            state,
            telegram,
            event_type="session_started",
            text=render_session_started_message(selected, session_id=session_id, cycle_profile=state["cycle_profile"]),
        )
        update_state_file(session_json, state, status="RUNNING")

        session_started_monotonic = time.monotonic()
        cycle_index = 0
        while True:
            if STOP_REQUESTED:
                update_state_file(session_json, state, status="STOPPING", stop_reason=STOP_REASON)
                break
            if stop_file_requested(stop_file):
                request_stop(f"stop_file:{stop_file}")
                update_state_file(session_json, state, status="STOPPING", stop_reason=STOP_REASON)
                break
            limit_reached, limit_reason = session_limit_reached(
                args,
                cycle_count=state["cycle_count"],
                session_started_monotonic=session_started_monotonic,
            )
            if limit_reached:
                update_state_file(session_json, state, status="COMPLETED_LIMIT", stop_reason=limit_reason)
                break

            cycle_index += 1
            cycle_paths = build_cycle_paths(
                session_artifacts_dir=session_artifacts_dir,
                audit_base_dir=Path(args.audit_base_dir).resolve() / session_id,
                out_base_dir=Path(args.out_base_dir).resolve() / session_id,
                cycle_index=cycle_index,
            )
            state["latest_cycle_index"] = cycle_index
            state["latest_cycle_started_ts_utc"] = utc_now_iso()
            update_state_file(session_json, state, status="RUNNING")

            child_command = build_child_command(
                args,
                session_binding_artifact=session_binding_artifact,
                selected=selected,
                cycle_paths=cycle_paths,
            )
            proc = start_child_process(child_command)
            child_exit_code, interrupted, interrupt_reason = wait_for_cycle_completion(proc, stop_file=stop_file)
            if interrupted:
                state["latest_cycle_status"] = "INTERRUPTED_BY_USER"
                state["stop_reason"] = interrupt_reason
                update_state_file(session_json, state, status="STOPPED_BY_USER", stop_reason=interrupt_reason)
                break

            bound_launch_result, child_telegram_result = load_cycle_artifacts(cycle_paths)
            apply_child_telegram_counts(state, child_telegram_result)

            cycle_status = str(bound_launch_result.get("launch_status") or "").strip()
            if child_exit_code != 0 and cycle_status not in {"VALID_NO_EXECUTION_ACTIVITY", "VALID_WITH_EXECUTION_ACTIVITY", "INVALID"}:
                cycle_status = "FAILED"

            futures_item = load_futures_item(
                Path(args.futures_paper_ledger_json).resolve(),
                pack_id=str(bound_launch_result.get("selected_pack_id") or "").strip(),
                live_run_id=str(bound_launch_result.get("selected_live_run_id") or "").strip(),
            )

            if futures_item is None and cycle_status in {"VALID_NO_EXECUTION_ACTIVITY", "VALID_WITH_EXECUTION_ACTIVITY"}:
                send_session_message(
                    state,
                    telegram,
                    event_type="cycle_artifact_missing",
                    text=render_cycle_artifact_missing_message(cycle_index=cycle_index, state=state),
                )

            if futures_item is not None:
                actions = collect_action_labels(futures_item)
                if actions:
                    send_session_message(
                        state,
                        telegram,
                        event_type="cycle_paper_activity",
                        text=render_cycle_paper_activity_message(
                            cycle_index=cycle_index,
                            state=state,
                            futures_item=futures_item,
                            actions=actions,
                        ),
                    )
                profitability_status = str(futures_item.get("profitability_status") or "").strip()
                if profitability_status in FUNDING_AWARE_PROFITABILITY_STATUSES:
                    send_session_message(
                        state,
                        telegram,
                        event_type="cycle_profitability",
                        text=render_cycle_profitability_message(
                            cycle_index=cycle_index,
                            state=state,
                            futures_item=futures_item,
                        ),
                    )
                funding_cost_quote = futures_item.get("funding_cost_quote")
                if isinstance(funding_cost_quote, (int, float)) and float(funding_cost_quote) != 0.0:
                    send_session_message(
                        state,
                        telegram,
                        event_type="cycle_funding_cost",
                        text=render_cycle_funding_message(
                            cycle_index=cycle_index,
                            state=state,
                            futures_item=futures_item,
                        ),
                    )

            update_cycle_state(
                state,
                cycle_index=cycle_index,
                cycle_status=cycle_status,
                bound_launch_result=bound_launch_result,
                futures_item=futures_item,
                cycle_paths=cycle_paths,
            )
            update_state_file(session_json, state, status="RUNNING")

            if cycle_status in {"INVALID", "FAILED"} and state["consecutive_invalid_cycles"] >= int(args.max_consecutive_invalid_cycles):
                reason = f"repeated_invalid_cycles:{state['consecutive_invalid_cycles']}"
                state["stop_reason"] = reason
                send_session_message(state, telegram, event_type="session_failed", text=render_session_failed_message(state, reason))
                update_state_file(session_json, state, status="FAILED", stop_reason=reason)
                break

            if cycle_status in {"INVALID", "FAILED"}:
                maybe_sleep(int(args.failure_cooldown_sec), stop_file=stop_file)
            else:
                maybe_sleep(int(args.cooldown_sec), stop_file=stop_file)

        if state["session_status"] == "STOPPING":
            update_state_file(session_json, state, status="STOPPED_BY_USER", stop_reason=state.get("stop_reason") or STOP_REASON)
        if state["session_status"] == "STOPPED_BY_USER":
            send_session_message(state, telegram, event_type="session_stopped_by_user", text=render_session_stop_message(state))
        elif state["session_status"] == "FAILED" and not state.get("stop_reason"):
            state["stop_reason"] = "session_failed"
        if state["session_status"] != "FAILED":
            send_session_message(state, telegram, event_type="session_summary", text=render_session_summary_message(state))
        else:
            send_session_message(state, telegram, event_type="session_summary", text=render_session_summary_message(state))
        update_state_file(session_json, state, status=state["session_status"], stop_reason=state.get("stop_reason") or "")

        print(f"session_json={session_json}")
        print(f"session_status={state['session_status']}")
        print(f"cycle_count={state['cycle_count']}")
        print(f"valid_cycle_count={state['valid_cycle_count']}")
        print(f"invalid_cycle_count={state['invalid_cycle_count']}")
        print(f"valid_with_execution_activity_count={state['valid_with_execution_activity_count']}")
        print(f"telegram_messages_attempted={state['telegram_messages_attempted']}")
        print(f"telegram_messages_sent={state['telegram_messages_sent']}")
        print(f"telegram_error_count={state['telegram_error_count']}")
        if state["session_status"] == "FAILED":
            return 1
        return 0
    except MomentumLongShadowSessionError as exc:
        payload = state or {
            "schema_version": SCHEMA_VERSION,
            "generated_ts_utc": utc_now_iso(),
            "last_updated_ts_utc": utc_now_iso(),
            "session_status": "FAILED",
            "stop_reason": str(exc),
            "telegram_messages_attempted": 0,
            "telegram_messages_sent": 0,
            "telegram_error_count": 0,
            "cycle_count": 0,
            "valid_cycle_count": 0,
            "invalid_cycle_count": 0,
            "valid_with_execution_activity_count": 0,
        }
        payload["session_status"] = "FAILED"
        payload["stop_reason"] = str(exc)
        payload.setdefault("stop_ts_utc", utc_now_iso())
        payload["last_updated_ts_utc"] = utc_now_iso()
        write_json(session_json, payload)
        print(f"session_json={session_json}")
        print("session_status=FAILED")
        print(f"error={str(exc)}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
