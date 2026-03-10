import json
import os
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-bound-shadow-launch-with-telegram-v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def binding_artifact(items: list[dict]) -> dict:
    return {
        "schema_version": "candidate_strategy_runtime_binding_v0",
        "generated_ts_utc": "2026-03-09T00:00:00Z",
        "source_candidate_strategy_contract_json": "/tmp/candidate_strategy_contract_v0.json",
        "source_binding_map_json": "/tmp/family_shadow_runtime_binding_map_v0.json",
        "source_row_count": len(items),
        "translated_spec_count": sum(1 for item in items if item.get("translation_status") == "TRANSLATABLE"),
        "bound_shadow_runnable_count": sum(1 for item in items if item.get("runtime_binding_status") == "BOUND_SHADOW_RUNNABLE"),
        "unbound_no_runtime_impl_count": sum(1 for item in items if item.get("runtime_binding_status") == "UNBOUND_NO_RUNTIME_IMPL"),
        "unbound_config_gap_count": 0,
        "unbound_translation_rejected_count": sum(1 for item in items if item.get("runtime_binding_status") == "UNBOUND_TRANSLATION_REJECTED"),
        "bindable_family_ids": ["spread_reversion_v1"],
        "items": items,
    }


def bound_item(*, rank: int = 1, pack_id: str = "pack_bound", strategy_id: str = "strategy_bound") -> dict:
    return {
        "rank": rank,
        "pack_id": pack_id,
        "translation_status": "TRANSLATABLE",
        "strategy_id": strategy_id,
        "family_id": "spread_reversion_v1",
        "exchange": "bybit",
        "stream": "bbo",
        "symbols": ["bnbusdt"],
        "runtime_binding_status": "BOUND_SHADOW_RUNNABLE",
        "runtime_strategy_file": "core/strategy/strategies/SpreadReversionV1Strategy.js",
        "runtime_strategy_config": {
            "binding_mode": "OBSERVE_ONLY",
            "family_id": "spread_reversion_v1",
            "source_pack_id": pack_id,
            "source_decision_tier": "PROMOTE_STRONG",
            "exchange": "bybit",
            "stream": "bbo",
            "symbols": ["bnbusdt"],
            "source_family_report_path": f"/tmp/{pack_id}.json",
            "window": "20260123..20260123",
            "params": {"delta_ms_list": [1000, 5000], "h_ms_list": [1000, 5000], "tolerance_ms": 0},
            "selected_cell": {
                "exchange": "bybit",
                "stream": "bbo",
                "symbol": "bnbusdt",
                "delta_ms": 5000,
                "h_ms": 5000,
                "mean_product": -0.001,
                "t_stat": -9.0,
            },
        },
        "binding_reason": "",
    }


def write_fake_bound_launch_tool(path: Path) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "result_json = Path(args[args.index(\"--result-json\") + 1])\n"
            "status = args[args.index(\"--launch-status\") + 1] if \"--launch-status\" in args else \"VALID_NO_EXECUTION_ACTIVITY\"\n"
            "pack_id = args[args.index(\"--pack-id\") + 1] if \"--pack-id\" in args else \"pack_bound\"\n"
            "strategy_id = args[args.index(\"--strategy-id\") + 1] if \"--strategy-id\" in args else \"strategy_bound\"\n"
            "payload = {\n"
            "  'schema_version': 'shadow_bound_launch_v0',\n"
            "  'launch_status': status,\n"
            "  'valid_run': status != 'INVALID',\n"
            "  'invalid_reason': '' if status != 'INVALID' else 'fake_invalid',\n"
            "  'required_artifacts_ok': status != 'INVALID',\n"
            "  'selected_pack_id': pack_id,\n"
            "  'selected_strategy_id': strategy_id,\n"
            "  'selected_live_run_id': 'live_' + pack_id,\n"
            "  'summary_processed_event_count': 42,\n"
            "}\n"
            "result_json.parent.mkdir(parents=True, exist_ok=True)\n"
            "result_json.write_text(json.dumps(payload) + '\\n', encoding='utf-8')\n"
            "raise SystemExit(0)\n"
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


class _TelegramHandler(BaseHTTPRequestHandler):
    messages: list[str] = []

    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")) or 0)
        payload = json.loads(body.decode("utf-8"))
        self.__class__.messages.append(str(payload.get("text") or ""))
        encoded = json.dumps({"ok": True, "result": {"message_id": len(self.__class__.messages)}}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):  # noqa: A003
        return


class RunBoundShadowLaunchWithTelegramV0Tests(unittest.TestCase):
    def _run(self, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        run_env = os.environ.copy()
        if env is not None:
            run_env.update(env)
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
            env=run_env,
        )

    def test_telegram_dry_run_sends_start_and_finish_without_env(self):
        with tempfile.TemporaryDirectory(prefix="bound_shadow_telegram_dry_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            launch_tool = root / "fake_launch.py"
            telegram_result_json = root / "telegram_result.json"
            bound_launch_result_json = root / "bound_launch_result.json"
            write_json(binding_json, binding_artifact([bound_item()]))
            write_fake_bound_launch_tool(launch_tool)

            res = self._run(
                "--telegram-dry-run",
                "--bound-launch-tool",
                str(launch_tool),
                "--binding-artifact",
                str(binding_json),
                "--telegram-result-json",
                str(telegram_result_json),
                "--bound-launch-result-json",
                str(bound_launch_result_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(telegram_result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["telegram_send_mode"], "DRY_RUN")
            self.assertEqual(payload["messages_attempted"], 2)
            self.assertEqual(payload["messages_sent"], 2)
            self.assertEqual(payload["event_types_attempted"], ["launch_started", "launch_finished_valid_no_execution_activity"])
            self.assertEqual(payload["selected_pack_id"], "pack_bound")
            self.assertTrue(payload["bound_launch_result_json"].endswith("bound_launch_result.json"))

    def test_missing_env_fails_honestly(self):
        with tempfile.TemporaryDirectory(prefix="bound_shadow_telegram_missing_env_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            telegram_result_json = root / "telegram_result.json"
            write_json(binding_json, binding_artifact([bound_item()]))

            env = {"QUANTLAB_ENV_FILE": str(root / "missing.env")}
            res = self._run(
                "--binding-artifact",
                str(binding_json),
                "--telegram-result-json",
                str(telegram_result_json),
                env=env,
            )

            self.assertNotEqual(res.returncode, 0)
            payload = json.loads(telegram_result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["telegram_send_mode"], "CONFIG_ERROR")
            self.assertEqual(payload["final_error"], "missing_telegram_env")
            self.assertEqual(payload["messages_attempted"], 1)
            self.assertEqual(payload["messages_sent"], 0)
            self.assertEqual(payload["event_types_attempted"], ["launch_failed"])

    def test_live_send_posts_honest_messages(self):
        with tempfile.TemporaryDirectory(prefix="bound_shadow_telegram_live_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            launch_tool = root / "fake_launch.py"
            telegram_result_json = root / "telegram_result.json"
            bound_launch_result_json = root / "bound_launch_result.json"
            execution_events_jsonl = root / "execution_events.jsonl"
            trade_ledger_jsonl = root / "trade_ledger.jsonl"
            write_json(binding_json, binding_artifact([bound_item()]))
            write_fake_bound_launch_tool(launch_tool)
            write_jsonl(
                execution_events_jsonl,
                [
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "selected_pack_id": "pack_bound",
                        "live_run_id": "live_pack_bound",
                        "event_type": "DECISION",
                    },
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "selected_pack_id": "pack_bound",
                        "live_run_id": "live_pack_bound",
                        "event_type": "FILL",
                    },
                ],
            )
            write_jsonl(
                trade_ledger_jsonl,
                [
                    {
                        "schema_version": "shadow_trade_ledger_v1",
                        "selected_pack_id": "pack_bound",
                        "open_live_run_id": "live_pack_bound",
                        "last_live_run_id": "live_pack_bound",
                        "status": "OPEN",
                    },
                    {
                        "schema_version": "shadow_trade_ledger_v1",
                        "selected_pack_id": "pack_bound",
                        "open_live_run_id": "live_pack_bound",
                        "last_live_run_id": "live_pack_bound",
                        "status": "CLOSED",
                    },
                ],
            )

            _TelegramHandler.messages = []
            server = HTTPServer(("127.0.0.1", 0), _TelegramHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                env_file = root / "telegram.env"
                env_file.write_text(
                    "TELEGRAM_BOT_TOKEN=123456:TESTTOKEN\nTELEGRAM_CHAT_ID=123456789\n",
                    encoding="utf-8",
                )
                env = {"QUANTLAB_ENV_FILE": str(env_file)}
                res = self._run(
                    "--bound-launch-tool",
                    str(launch_tool),
                    "--binding-artifact",
                    str(binding_json),
                    "--telegram-result-json",
                    str(telegram_result_json),
                    "--bound-launch-result-json",
                    str(bound_launch_result_json),
                    "--execution-events-jsonl",
                    str(execution_events_jsonl),
                    "--trade-ledger-jsonl",
                    str(trade_ledger_jsonl),
                    "--telegram-api-base-url",
                    f"http://127.0.0.1:{server.server_port}",
                    env=env,
                )
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(telegram_result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["telegram_send_mode"], "LIVE")
            self.assertEqual(payload["messages_attempted"], 2)
            self.assertEqual(payload["messages_sent"], 2)
            self.assertEqual(payload["event_types_attempted"], ["launch_started", "launch_finished_valid_no_execution_activity"])
            self.assertEqual(payload["execution_event_counts"], {"DECISION": 1, "RISK_REJECT": 0, "FILL": 1})
            self.assertEqual(payload["synthetic_trade_status_counts"], {"OPEN": 1, "CLOSED": 1})
            self.assertEqual(len(_TelegramHandler.messages), 2)
            self.assertIn("🟢 Bound shadow started", _TelegramHandler.messages[0])
            self.assertIn("Mode: shadow/paper", _TelegramHandler.messages[0])
            self.assertIn("fill 1", _TelegramHandler.messages[1])
            self.assertIn("Paper fills observed; not exchange-confirmed.", _TelegramHandler.messages[1])
            self.assertNotIn("BUY executed", _TelegramHandler.messages[1])


if __name__ == "__main__":
    unittest.main()
