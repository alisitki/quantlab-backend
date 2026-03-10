import json
import os
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-momentum-long-shadow-session-v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


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
        "bindable_family_ids": ["spread_reversion_v1", "momentum_v1"],
        "items": items,
    }


def bound_item(
    *,
    family_id: str,
    pack_id: str,
    strategy_id: str,
    stream: str,
    strategy_file: str,
    binding_mode: str,
    rank: int = 1,
    symbol: str = "btcusdt",
) -> dict:
    selected_cell = {
        "exchange": "binance",
        "stream": stream,
        "symbol": symbol,
        "delta_ms": 1000,
        "h_ms": 1000,
        "event_count": 1000,
        "mean_product": 0.01,
        "t_stat": 4.0,
    }
    if family_id == "spread_reversion_v1":
        selected_cell["mean_product"] = -0.01
        selected_cell["t_stat"] = -4.0
    return {
        "rank": rank,
        "pack_id": pack_id,
        "translation_status": "TRANSLATABLE",
        "strategy_id": strategy_id,
        "family_id": family_id,
        "exchange": "binance",
        "stream": stream,
        "symbols": [symbol],
        "runtime_binding_status": "BOUND_SHADOW_RUNNABLE",
        "runtime_strategy_file": strategy_file,
        "runtime_strategy_config": {
            "binding_mode": binding_mode,
            "family_id": family_id,
            "source_pack_id": pack_id,
            "source_decision_tier": "PROMOTE_STRONG",
            "exchange": "binance",
            "stream": stream,
            "symbols": [symbol],
            "source_family_report_path": f"/tmp/{pack_id}.json",
            "window": "20260101..20260101",
            "params": {"delta_ms_list": [1000], "h_ms_list": [1000], "tolerance_ms": 0},
            "selected_cell": selected_cell,
            "orderQty": 1,
        },
        "binding_reason": "",
    }


def write_fake_child_tool(path: Path, config_path: Path) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            "config = json.loads(Path(%r).read_text(encoding='utf-8'))\n"
            "counter_path = Path(config['counter_path'])\n"
            "counter_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "cycle_index = 0\n"
            "if counter_path.exists():\n"
            "    cycle_index = int(counter_path.read_text(encoding='utf-8').strip() or '0')\n"
            "cycles = list(config['cycles'])\n"
            "cycle = cycles[min(cycle_index, len(cycles) - 1)]\n"
            "counter_path.write_text(str(cycle_index + 1), encoding='utf-8')\n"
            "args = sys.argv[1:]\n"
            "def value(flag):\n"
            "    return Path(args[args.index(flag) + 1])\n"
            "bound_launch = value('--bound-launch-result-json')\n"
            "telegram_result = value('--telegram-result-json')\n"
            "watchlist_json = value('--generated-watchlist-json')\n"
            "child_launch = value('--child-launch-result-json')\n"
            "child_batch = value('--child-batch-result-json')\n"
            "stdout_log = value('--batch-stdout-log')\n"
            "stderr_log = value('--batch-stderr-log')\n"
            "audit_base = Path(args[args.index('--audit-base-dir') + 1])\n"
            "out_dir = Path(args[args.index('--out-dir') + 1])\n"
            "for path in [bound_launch, telegram_result, watchlist_json, child_launch, child_batch, stdout_log, stderr_log]:\n"
            "    path.parent.mkdir(parents=True, exist_ok=True)\n"
            "audit_base.mkdir(parents=True, exist_ok=True)\n"
            "out_dir.mkdir(parents=True, exist_ok=True)\n"
            "selected_strategy_id = args[args.index('--strategy-id') + 1] if '--strategy-id' in args else cycle['selected_strategy_id']\n"
            "selected_pack_id = cycle['selected_pack_id']\n"
            "bound_payload = {\n"
            "  'schema_version': 'shadow_bound_launch_v0',\n"
            "  'launch_status': cycle['launch_status'],\n"
            "  'valid_run': cycle['launch_status'] != 'INVALID',\n"
            "  'invalid_reason': cycle.get('invalid_reason', ''),\n"
            "  'required_artifacts_ok': bool(cycle.get('required_artifacts_ok', True)),\n"
            "  'selected_pack_id': selected_pack_id,\n"
            "  'selected_strategy_id': selected_strategy_id,\n"
            "  'selected_live_run_id': cycle['selected_live_run_id'],\n"
            "  'summary_processed_event_count': cycle.get('summary_processed_event_count', 0),\n"
            "  'matched_execution_event_count': cycle.get('matched_execution_event_count', 0),\n"
            "  'matched_trade_count': cycle.get('matched_trade_count', 0),\n"
            "}\n"
            "bound_launch.write_text(json.dumps(bound_payload) + '\\n', encoding='utf-8')\n"
            "telegram_payload = {\n"
            "  'schema_version': 'shadow_bound_launch_telegram_v0',\n"
            "  'messages_attempted': cycle.get('child_messages_attempted', 2),\n"
            "  'messages_sent': cycle.get('child_messages_sent', 2),\n"
            "  'error_count': cycle.get('child_error_count', 0),\n"
            "  'event_types_attempted': ['launch_started', 'launch_finished_valid_with_execution_activity'],\n"
            "  'event_types_sent': ['launch_started', 'launch_finished_valid_with_execution_activity'],\n"
            "}\n"
            "telegram_result.write_text(json.dumps(telegram_payload) + '\\n', encoding='utf-8')\n"
            "watchlist_json.write_text(json.dumps({'schema_version': 'shadow_bound_launch_watchlist_v0'}) + '\\n', encoding='utf-8')\n"
            "child_launch.write_text(json.dumps({'schema_version': 'shadow_long_shadow_launch_v0'}) + '\\n', encoding='utf-8')\n"
            "child_batch.write_text(json.dumps({'schema_version': 'shadow_observation_batch_result_v0'}) + '\\n', encoding='utf-8')\n"
            "stdout_log.write_text('fake stdout\\n', encoding='utf-8')\n"
            "stderr_log.write_text('', encoding='utf-8')\n"
            "raise SystemExit(int(cycle.get('exit_code', 0)))\n"
        )
        % str(config_path),
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


class RunMomentumLongShadowSessionV0Tests(unittest.TestCase):
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

    def _write_futures_payload(self, path: Path, *, pack_id: str, live_run_id: str, funding_cost_quote: float = 0.0) -> None:
        payload = {
            "schema_version": "shadow_futures_paper_ledger_v1",
            "generated_ts_utc": "2026-03-09T00:00:00Z",
            "run_count": 1,
            "fill_backed_run_count": 1,
            "net_fee_backed_run_count": 1,
            "funding_cost_backed_run_count": 1 if funding_cost_quote != 0.0 else 0,
            "profitability_interpretable_run_count": 1,
            "items": [
                {
                    "selected_pack_id": pack_id,
                    "live_run_id": live_run_id,
                    "paper_run_status": "FILL_BACKED_POSITION_OPEN",
                    "fill_event_count": 1,
                    "final_position_direction": "LONG",
                    "final_position_qty": 1.0,
                    "funding_cost_quote": funding_cost_quote,
                    "funding_support_status": "FUNDING_COST_BACKED" if funding_cost_quote != 0.0 else "NO_FUNDING_WINDOW_CROSSED",
                    "funding_alignment_status": "ALL_APPLIED_WINDOWS_MARK_PRICE_BACKED" if funding_cost_quote != 0.0 else "NO_FUNDING_WINDOW_CROSSED",
                    "funding_applied_count": 1 if funding_cost_quote != 0.0 else 0,
                    "profitability_status": "NET_MARK_TO_MARKET_AFTER_FEES_FUNDING_AND_EXIT_ESTIMATE",
                    "mark_to_market_pnl_quote_net_after_funding": 12.5,
                    "mark_to_market_pnl_quote_net_after_funding_and_exit_estimate": 11.5,
                    "action_sequence": [
                        {
                            "action": "LONG_OPEN",
                            "side": "BUY",
                            "qty": 1.0,
                            "fill_price": 100.0,
                        }
                    ],
                }
            ],
        }
        write_json(path, payload)

    def test_max_cycles_one_aggregates_valid_cycle_and_paper_activity(self):
        with tempfile.TemporaryDirectory(prefix="momentum_session_one_cycle_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            child_tool = root / "fake_child.py"
            child_config = root / "child_config.json"
            session_json = root / "session.json"
            session_artifacts_dir = root / "artifacts"
            futures_json = root / "shadow_futures_paper_ledger_v1.json"
            counter_path = root / "child_counter.txt"

            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="spread_reversion_v1",
                            pack_id="pack_spread",
                            strategy_id="strategy_spread",
                            stream="bbo",
                            strategy_file="core/strategy/strategies/SpreadReversionV1Strategy.js",
                            binding_mode="OBSERVE_ONLY",
                            rank=1,
                            symbol="bnbusdt",
                        ),
                        bound_item(
                            family_id="momentum_v1",
                            pack_id="pack_momentum",
                            strategy_id="strategy_momentum",
                            stream="trade",
                            strategy_file="core/strategy/strategies/MomentumV1Strategy.js",
                            binding_mode="PAPER_DIRECTIONAL_V1",
                            rank=2,
                            symbol="btcusdt",
                        ),
                    ]
                ),
            )
            write_json(
                child_config,
                {
                    "counter_path": str(counter_path),
                    "cycles": [
                        {
                            "selected_pack_id": "pack_momentum",
                            "selected_strategy_id": "strategy_momentum",
                            "selected_live_run_id": "live_pack_momentum",
                            "launch_status": "VALID_WITH_EXECUTION_ACTIVITY",
                            "required_artifacts_ok": True,
                            "summary_processed_event_count": 123,
                            "matched_execution_event_count": 7,
                            "matched_trade_count": 1,
                            "child_messages_attempted": 2,
                            "child_messages_sent": 2,
                            "child_error_count": 0,
                            "exit_code": 0,
                        }
                    ],
                },
            )
            write_fake_child_tool(child_tool, child_config)
            self._write_futures_payload(futures_json, pack_id="pack_momentum", live_run_id="live_pack_momentum")

            res = self._run(
                "--telegram-dry-run",
                "--binding-artifact",
                str(binding_json),
                "--child-tool",
                str(child_tool),
                "--session-json",
                str(session_json),
                "--session-artifacts-dir",
                str(session_artifacts_dir),
                "--futures-paper-ledger-json",
                str(futures_json),
                "--max-cycles",
                "1",
                "--cooldown-sec",
                "0",
                "--failure-cooldown-sec",
                "0",
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["session_status"], "COMPLETED_LIMIT")
            self.assertEqual(payload["selected_family_id"], "momentum_v1")
            self.assertEqual(payload["selected_pack_id"], "pack_momentum")
            self.assertEqual(payload["cycle_count"], 1)
            self.assertEqual(payload["valid_cycle_count"], 1)
            self.assertEqual(payload["invalid_cycle_count"], 0)
            self.assertEqual(payload["valid_with_execution_activity_count"], 1)
            self.assertEqual(payload["total_processed_event_count"], 123)
            self.assertEqual(payload["total_matched_execution_event_count"], 7)
            self.assertEqual(payload["total_matched_trade_count"], 1)
            self.assertEqual(payload["total_fill_backed_run_count"], 1)
            self.assertEqual(payload["funding_aware_cycle_count"], 1)
            self.assertEqual(payload["latest_cycle_status"], "VALID_WITH_EXECUTION_ACTIVITY")
            self.assertEqual(payload["telegram_messages_attempted"], 6)
            self.assertEqual(payload["telegram_messages_sent"], 6)
            self.assertEqual(payload["telegram_error_count"], 0)
            self.assertIn("session_started", payload["session_event_types_attempted"])
            self.assertIn("cycle_paper_activity", payload["session_event_types_attempted"])
            self.assertIn("cycle_profitability", payload["session_event_types_attempted"])
            self.assertIn("session_summary", payload["session_event_types_attempted"])

    def test_non_momentum_selector_fails_honestly(self):
        with tempfile.TemporaryDirectory(prefix="momentum_session_wrong_family_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            session_json = root / "session.json"
            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="spread_reversion_v1",
                            pack_id="pack_spread",
                            strategy_id="strategy_spread",
                            stream="bbo",
                            strategy_file="core/strategy/strategies/SpreadReversionV1Strategy.js",
                            binding_mode="OBSERVE_ONLY",
                            rank=1,
                            symbol="bnbusdt",
                        )
                    ]
                ),
            )

            res = self._run(
                "--telegram-dry-run",
                "--binding-artifact",
                str(binding_json),
                "--pack-id",
                "pack_spread",
                "--session-json",
                str(session_json),
            )

            self.assertNotEqual(res.returncode, 0)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["session_status"], "FAILED")
            self.assertEqual(payload["stop_reason"], "selected_row_wrong_family:spread_reversion_v1")

    def test_stop_file_stops_before_first_cycle(self):
        with tempfile.TemporaryDirectory(prefix="momentum_session_stop_file_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            child_tool = root / "fake_child.py"
            child_config = root / "child_config.json"
            session_json = root / "session.json"
            stop_file = root / "stop.now"
            futures_json = root / "shadow_futures_paper_ledger_v1.json"
            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="momentum_v1",
                            pack_id="pack_momentum",
                            strategy_id="strategy_momentum",
                            stream="trade",
                            strategy_file="core/strategy/strategies/MomentumV1Strategy.js",
                            binding_mode="PAPER_DIRECTIONAL_V1",
                            rank=1,
                            symbol="btcusdt",
                        )
                    ]
                ),
            )
            write_json(child_config, {"counter_path": str(root / "counter.txt"), "cycles": []})
            write_fake_child_tool(child_tool, child_config)
            self._write_futures_payload(futures_json, pack_id="pack_momentum", live_run_id="live_pack_momentum")
            stop_file.write_text("stop\n", encoding="utf-8")

            res = self._run(
                "--telegram-dry-run",
                "--binding-artifact",
                str(binding_json),
                "--child-tool",
                str(child_tool),
                "--session-json",
                str(session_json),
                "--futures-paper-ledger-json",
                str(futures_json),
                "--stop-file",
                str(stop_file),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["session_status"], "STOPPED_BY_USER")
            self.assertEqual(payload["cycle_count"], 0)
            self.assertTrue(str(payload["stop_reason"]).startswith("stop_file:"))

    def test_live_session_messages_use_fake_telegram_server(self):
        with tempfile.TemporaryDirectory(prefix="momentum_session_live_tg_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            child_tool = root / "fake_child.py"
            child_config = root / "child_config.json"
            session_json = root / "session.json"
            session_artifacts_dir = root / "artifacts"
            futures_json = root / "shadow_futures_paper_ledger_v1.json"
            counter_path = root / "child_counter.txt"
            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(
                            family_id="momentum_v1",
                            pack_id="pack_momentum",
                            strategy_id="strategy_momentum",
                            stream="trade",
                            strategy_file="core/strategy/strategies/MomentumV1Strategy.js",
                            binding_mode="PAPER_DIRECTIONAL_V1",
                            rank=1,
                            symbol="btcusdt",
                        )
                    ]
                ),
            )
            write_json(
                child_config,
                {
                    "counter_path": str(counter_path),
                    "cycles": [
                        {
                            "selected_pack_id": "pack_momentum",
                            "selected_strategy_id": "strategy_momentum",
                            "selected_live_run_id": "live_pack_momentum",
                            "launch_status": "VALID_NO_EXECUTION_ACTIVITY",
                            "required_artifacts_ok": True,
                            "summary_processed_event_count": 55,
                            "matched_execution_event_count": 0,
                            "matched_trade_count": 0,
                            "child_messages_attempted": 2,
                            "child_messages_sent": 2,
                            "child_error_count": 0,
                            "exit_code": 0,
                        }
                    ],
                },
            )
            write_fake_child_tool(child_tool, child_config)
            self._write_futures_payload(futures_json, pack_id="pack_momentum", live_run_id="live_pack_momentum")

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
                    "--binding-artifact",
                    str(binding_json),
                    "--child-tool",
                    str(child_tool),
                    "--session-json",
                    str(session_json),
                    "--session-artifacts-dir",
                    str(session_artifacts_dir),
                    "--futures-paper-ledger-json",
                    str(futures_json),
                    "--max-cycles",
                    "1",
                    "--cooldown-sec",
                    "0",
                    "--failure-cooldown-sec",
                    "0",
                    "--telegram-api-base-url",
                    f"http://127.0.0.1:{server.server_port}",
                    env=env,
                )
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(session_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["telegram_send_mode"], "LIVE")
            self.assertGreaterEqual(payload["telegram_messages_sent"], 3)
            joined = "\n".join(_TelegramHandler.messages)
            self.assertIn("🟢 Momentum session started", joined)
            self.assertIn("Mode: shadow/paper", joined)
            self.assertIn("Paper fill-backed", joined)


if __name__ == "__main__":
    unittest.main()
