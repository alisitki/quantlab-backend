import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-bound-shadow-launch-v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def binding_artifact(items: list[dict]) -> dict:
    return {
        "schema_version": "candidate_strategy_runtime_binding_v0",
        "generated_ts_utc": "2026-03-08T19:00:00Z",
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


def bound_item(*, rank: int, pack_id: str, strategy_id: str, symbol: str) -> dict:
    return {
        "rank": rank,
        "pack_id": pack_id,
        "translation_status": "TRANSLATABLE",
        "strategy_id": strategy_id,
        "family_id": "spread_reversion_v1",
        "exchange": "bybit",
        "stream": "bbo",
        "symbols": [symbol],
        "runtime_binding_status": "BOUND_SHADOW_RUNNABLE",
        "runtime_strategy_file": "core/strategy/strategies/SpreadReversionV1Strategy.js",
        "runtime_strategy_config": {
            "binding_mode": "OBSERVE_ONLY",
            "family_id": "spread_reversion_v1",
            "source_pack_id": pack_id,
            "source_decision_tier": "PROMOTE_STRONG",
            "exchange": "bybit",
            "stream": "bbo",
            "symbols": [symbol],
            "source_family_report_path": f"/tmp/{pack_id}.json",
            "window": "20260123..20260123",
            "params": {
                "delta_ms_list": [1000, 5000],
                "h_ms_list": [1000, 5000],
                "tolerance_ms": 0,
            },
            "selected_cell": {
                "exchange": "bybit",
                "stream": "bbo",
                "symbol": symbol,
                "delta_ms": 5000,
                "h_ms": 5000,
                "mean_product": -0.001,
                "t_stat": -9.0,
            },
        },
        "binding_reason": "",
    }


def unbound_item(*, rank: int, pack_id: str) -> dict:
    return {
        "rank": rank,
        "pack_id": pack_id,
        "translation_status": "TRANSLATABLE",
        "strategy_id": f"candidate_strategy::spread_reversion_v1::{pack_id}::bnbusdt",
        "family_id": "spread_reversion_v1",
        "exchange": "bybit",
        "stream": "bbo",
        "symbols": ["bnbusdt"],
        "runtime_binding_status": "UNBOUND_NO_RUNTIME_IMPL",
        "runtime_strategy_file": None,
        "runtime_strategy_config": None,
        "binding_reason": "NO_RUNTIME_BINDING_FOR_FAMILY:spread_reversion_v1",
    }


def write_fake_launch_tool(path: Path) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "watchlist_path = Path(args[args.index('--watchlist') + 1])\n"
            "strategy = args[args.index('--strategy') + 1]\n"
            "strategy_config = json.loads(args[args.index('--strategy-config-json') + 1])\n"
            "launch_result_path = Path(args[args.index('--launch-result-json') + 1])\n"
            "batch_result_path = Path(args[args.index('--batch-result-json') + 1])\n"
            "watchlist = json.loads(watchlist_path.read_text(encoding='utf-8'))\n"
            "selected = watchlist['items'][0]\n"
            "launch_payload = {\n"
            "  'schema_version': 'shadow_long_shadow_launch_v0',\n"
            "  'launch_status': 'VALID_NO_EXECUTION_ACTIVITY',\n"
            "  'valid_run': True,\n"
            "  'invalid_reason': '',\n"
            "  'required_artifacts_ok': True,\n"
            "  'selected_pack_id': selected['pack_id'],\n"
            "  'selected_live_run_id': 'live_' + selected['pack_id'],\n"
            "  'matched_execution_event_count': 0,\n"
            "  'matched_trade_count': 0,\n"
            "  'summary_json_path': '/tmp/summary.json',\n"
            "  'stdout_log_path': '/tmp/stdout.log',\n"
            "  'stderr_log_path': '/tmp/stderr.log',\n"
            "  'audit_spool_dir': '/tmp/audit',\n"
            "  'received_strategy': strategy,\n"
            "  'received_strategy_config': strategy_config,\n"
            "  'received_watchlist': watchlist,\n"
            "}\n"
            "launch_result_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "launch_result_path.write_text(json.dumps(launch_payload) + '\\n', encoding='utf-8')\n"
            "batch_result_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "batch_result_path.write_text(json.dumps({'schema_version': 'shadow_observation_batch_result_v0'}) + '\\n', encoding='utf-8')\n"
            "print('fake_bound_launch_done')\n"
            "raise SystemExit(0)\n"
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


class RunBoundShadowLaunchV0Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_default_selector_picks_first_bound_row_and_maps_runtime_fields(self):
        with tempfile.TemporaryDirectory(prefix="bound_shadow_launch_default_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            launch_tool = root / "fake_launch.py"
            result_json = root / "result.json"
            generated_watchlist_json = root / "watchlist.json"
            child_launch_result_json = root / "child_launch.json"
            child_batch_result_json = root / "child_batch.json"
            write_json(
                binding_json,
                binding_artifact(
                    [
                        unbound_item(rank=1, pack_id="pack_unbound"),
                        bound_item(rank=2, pack_id="pack_bound_a", strategy_id="strategy_a", symbol="bnbusdt"),
                        bound_item(rank=3, pack_id="pack_bound_b", strategy_id="strategy_b", symbol="ethusdt"),
                    ]
                ),
            )
            write_fake_launch_tool(launch_tool)

            res = self._run(
                "--binding-artifact",
                str(binding_json),
                "--launch-tool",
                str(launch_tool),
                "--result-json",
                str(result_json),
                "--generated-watchlist-json",
                str(generated_watchlist_json),
                "--child-launch-result-json",
                str(child_launch_result_json),
                "--child-batch-result-json",
                str(child_batch_result_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["selected_pack_id"], "pack_bound_a")
            self.assertEqual(payload["selected_strategy_id"], "strategy_a")
            self.assertEqual(payload["runtime_strategy_file"], "core/strategy/strategies/SpreadReversionV1Strategy.js")
            self.assertEqual(payload["launch_status"], "VALID_NO_EXECUTION_ACTIVITY")
            child_payload = json.loads(child_launch_result_json.read_text(encoding="utf-8"))
            self.assertEqual(child_payload["received_strategy"], "core/strategy/strategies/SpreadReversionV1Strategy.js")
            self.assertEqual(child_payload["received_strategy_config"]["source_pack_id"], "pack_bound_a")
            self.assertEqual(child_payload["received_watchlist"]["items"][0]["selection_slot"], "bybit/bbo")

    def test_strategy_id_selector_picks_exact_bound_row(self):
        with tempfile.TemporaryDirectory(prefix="bound_shadow_launch_strategy_id_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            launch_tool = root / "fake_launch.py"
            result_json = root / "result.json"
            generated_watchlist_json = root / "watchlist.json"
            child_launch_result_json = root / "child_launch.json"
            child_batch_result_json = root / "child_batch.json"
            write_json(
                binding_json,
                binding_artifact(
                    [
                        bound_item(rank=1, pack_id="pack_bound_a", strategy_id="strategy_a", symbol="bnbusdt"),
                        bound_item(rank=2, pack_id="pack_bound_b", strategy_id="strategy_b", symbol="ethusdt"),
                    ]
                ),
            )
            write_fake_launch_tool(launch_tool)

            res = self._run(
                "--binding-artifact",
                str(binding_json),
                "--launch-tool",
                str(launch_tool),
                "--strategy-id",
                "strategy_b",
                "--result-json",
                str(result_json),
                "--generated-watchlist-json",
                str(generated_watchlist_json),
                "--child-launch-result-json",
                str(child_launch_result_json),
                "--child-batch-result-json",
                str(child_batch_result_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["selection_mode"], "STRATEGY_ID")
            self.assertEqual(payload["selected_pack_id"], "pack_bound_b")
            self.assertEqual(payload["selected_strategy_id"], "strategy_b")

    def test_unbound_selector_fails_honestly(self):
        with tempfile.TemporaryDirectory(prefix="bound_shadow_launch_unbound_") as td:
            root = Path(td)
            binding_json = root / "binding.json"
            launch_tool = root / "fake_launch.py"
            result_json = root / "result.json"
            write_json(binding_json, binding_artifact([unbound_item(rank=1, pack_id="pack_unbound")]))
            write_fake_launch_tool(launch_tool)

            res = self._run(
                "--binding-artifact",
                str(binding_json),
                "--launch-tool",
                str(launch_tool),
                "--pack-id",
                "pack_unbound",
                "--result-json",
                str(result_json),
            )

            self.assertNotEqual(res.returncode, 0)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["launch_status"], "INVALID")
            self.assertEqual(payload["invalid_reason"], "selected_row_not_bound:UNBOUND_NO_RUNTIME_IMPL")
