import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase6_strategy_runtime_binding_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def base_contract(items: list[dict]) -> dict:
    return {
        "schema_version": "candidate_strategy_contract_v0",
        "generated_ts_utc": "2026-03-08T15:00:00Z",
        "source_candidate_review_tsv": "/tmp/candidate_review.tsv",
        "source_row_count": len(items),
        "translatable_count": sum(1 for item in items if item.get("translation_status") == "TRANSLATABLE"),
        "not_translatable_yet_count": 0,
        "insufficient_contract_count": 0,
        "unsupported_family_count": 0,
        "items": items,
    }


def empty_binding_map() -> dict:
    return {
        "schema_version": "family_shadow_runtime_binding_map_v0",
        "generated_ts_utc": "2026-03-08T15:00:00Z",
        "bindings": {},
    }


def translated_item() -> dict:
    return {
        "rank": 1,
        "pack_id": "pack_a",
        "source_pack_id": "pack_a",
        "contract_row_id": "candidate_contract::pack_a::bnbusdt",
        "selected_symbol": "bnbusdt",
        "pack_path": "/tmp/pack_a",
        "decision_tier": "PROMOTE_STRONG",
        "translation_status": "TRANSLATABLE",
        "reject_reason": "",
        "strategy_spec": {
            "strategy_spec_version": "candidate_strategy_spec_v0",
            "strategy_id": "candidate_strategy::spread_reversion_v1::pack_a::bnbusdt",
            "source_pack_id": "pack_a",
            "source_contract_row_id": "candidate_contract::pack_a::bnbusdt",
            "source_decision_tier": "PROMOTE_STRONG",
            "source_selected_symbol": "bnbusdt",
            "family_id": "spread_reversion_v1",
            "exchange": "bybit",
            "stream": "bbo",
            "symbols": ["bnbusdt"],
            "activation_mode": "SPEC_ONLY",
            "runtime_binding_status": "UNBOUND",
            "source_family_report_path": "/tmp/family_spread_reversion_report.json",
            "strategy_params": {
                "window": "20260123..20260123",
                "params": {"delta_ms_list": [1000]},
                "selected_cell": {"symbol": "bnbusdt"},
            },
        },
    }


def rejected_item() -> dict:
    return {
        "rank": 2,
        "pack_id": "pack_b",
        "source_pack_id": "pack_b",
        "contract_row_id": "candidate_contract::pack_b::btcusdt",
        "selected_symbol": "btcusdt",
        "pack_path": "/tmp/pack_b",
        "decision_tier": "PROMOTE",
        "translation_status": "NOT_TRANSLATABLE_YET",
        "reject_reason": "MULTI_SYMBOL_PACK_UNSUPPORTED",
        "strategy_spec": None,
    }


def directional_item() -> dict:
    return {
        "rank": 3,
        "pack_id": "pack_c",
        "source_pack_id": "pack_c",
        "contract_row_id": "candidate_contract::pack_c::btcusdt",
        "selected_symbol": "btcusdt",
        "pack_path": "/tmp/pack_c",
        "decision_tier": "PROMOTE_STRONG",
        "translation_status": "TRANSLATABLE",
        "reject_reason": "",
        "strategy_spec": {
            "strategy_spec_version": "candidate_strategy_spec_v0",
            "strategy_id": "candidate_strategy::momentum_v1::pack_c::btcusdt",
            "source_pack_id": "pack_c",
            "source_contract_row_id": "candidate_contract::pack_c::btcusdt",
            "source_decision_tier": "PROMOTE_STRONG",
            "source_selected_symbol": "btcusdt",
            "family_id": "momentum_v1",
            "exchange": "binance",
            "stream": "trade",
            "symbols": ["btcusdt"],
            "activation_mode": "SPEC_ONLY",
            "runtime_binding_status": "UNBOUND",
            "source_family_report_path": "/tmp/family_momentum_report.json",
            "strategy_params": {
                "window": "20260123..20260123",
                "params": {"delta_ms_list": [1000]},
                "selected_cell": {"symbol": "btcusdt"},
            },
        },
    }


class CandidateStrategyRuntimeBindingV0Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_empty_input_generates_empty_payload(self):
        with tempfile.TemporaryDirectory(prefix="candidate_runtime_binding_empty_") as td:
            root = Path(td)
            contract_json = root / "contract.json"
            binding_map_json = root / "binding_map.json"
            out_json = root / "binding.json"
            write_json(contract_json, base_contract([]))
            write_json(binding_map_json, empty_binding_map())

            res = self._run(
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--binding-map-json",
                str(binding_map_json),
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["source_row_count"], 0)
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["bound_shadow_runnable_count"], 0)
            self.assertEqual(payload["bound_directional_count"], 0)
            self.assertEqual(payload["bound_observe_only_count"], 0)

    def test_translatable_spec_without_map_is_unbound_no_runtime_impl(self):
        with tempfile.TemporaryDirectory(prefix="candidate_runtime_binding_nomap_") as td:
            root = Path(td)
            contract_json = root / "contract.json"
            binding_map_json = root / "binding_map.json"
            out_json = root / "binding.json"
            write_json(contract_json, base_contract([translated_item()]))
            write_json(binding_map_json, empty_binding_map())

            res = self._run(
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--binding-map-json",
                str(binding_map_json),
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["runtime_binding_status"], "UNBOUND_NO_RUNTIME_IMPL")
            self.assertEqual(item["binding_reason"], "NO_RUNTIME_BINDING_FOR_FAMILY:spread_reversion_v1")

    def test_rejected_translation_stays_unbound_translation_rejected(self):
        with tempfile.TemporaryDirectory(prefix="candidate_runtime_binding_rejected_") as td:
            root = Path(td)
            contract_json = root / "contract.json"
            binding_map_json = root / "binding_map.json"
            out_json = root / "binding.json"
            write_json(contract_json, base_contract([rejected_item()]))
            write_json(binding_map_json, empty_binding_map())

            res = self._run(
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--binding-map-json",
                str(binding_map_json),
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["runtime_binding_status"], "UNBOUND_TRANSLATION_REJECTED")
            self.assertEqual(
                item["binding_reason"],
                "TRANSLATION_STATUS:NOT_TRANSLATABLE_YET:MULTI_SYMBOL_PACK_UNSUPPORTED",
            )

    def test_valid_binding_map_can_bind_temp_strategy(self):
        with tempfile.TemporaryDirectory(prefix="candidate_runtime_binding_bound_") as td:
            root = Path(td)
            contract_json = root / "contract.json"
            binding_map_json = root / "binding_map.json"
            out_json = root / "binding.json"
            strategy_file = root / "SpreadRuntimeStrategy.js"
            strategy_file.write_text(
                "export default class SpreadRuntimeStrategy { async onEvent() {} }\n",
                encoding="utf-8",
            )
            write_json(contract_json, base_contract([translated_item()]))
            write_json(
                binding_map_json,
                {
                    "schema_version": "family_shadow_runtime_binding_map_v0",
                    "generated_ts_utc": "2026-03-08T15:00:00Z",
                    "bindings": {
                        "spread_reversion_v1": {
                            "strategy_file": str(strategy_file),
                            "strategy_config": {"lookback_ms": 5000},
                            "supported_streams": ["bbo"],
                            "supported_exchanges": ["bybit"],
                        }
                    },
                },
            )

            res = self._run(
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--binding-map-json",
                str(binding_map_json),
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["runtime_binding_status"], "BOUND_SHADOW_RUNNABLE")
            self.assertEqual(item["runtime_strategy_file"], str(strategy_file))
            self.assertEqual(item["shadow_tradeability_class"], "DIRECTIONAL")
            self.assertEqual(
                item["runtime_strategy_config"],
                {
                    "lookback_ms": 5000,
                    "family_id": "spread_reversion_v1",
                    "source_pack_id": "pack_a",
                    "source_decision_tier": "PROMOTE_STRONG",
                    "exchange": "bybit",
                    "stream": "bbo",
                    "symbols": ["bnbusdt"],
                    "source_family_report_path": "/tmp/family_spread_reversion_report.json",
                    "window": "20260123..20260123",
                    "params": {"delta_ms_list": [1000]},
                    "selected_cell": {"symbol": "bnbusdt"},
                },
            )

    def test_payload_reports_directional_and_observe_only_bound_counts(self):
        with tempfile.TemporaryDirectory(prefix="candidate_runtime_binding_tradeability_") as td:
            root = Path(td)
            contract_json = root / "contract.json"
            binding_map_json = root / "binding_map.json"
            out_json = root / "binding.json"
            spread_file = root / "SpreadRuntimeStrategy.js"
            momentum_file = root / "MomentumRuntimeStrategy.js"
            spread_file.write_text("export default class SpreadRuntimeStrategy { async onEvent() {} }\n", encoding="utf-8")
            momentum_file.write_text("export default class MomentumRuntimeStrategy { async onEvent() {} }\n", encoding="utf-8")
            write_json(contract_json, base_contract([translated_item(), directional_item()]))
            write_json(
                binding_map_json,
                {
                    "schema_version": "family_shadow_runtime_binding_map_v0",
                    "generated_ts_utc": "2026-03-08T15:00:00Z",
                    "bindings": {
                        "spread_reversion_v1": {
                            "strategy_file": str(spread_file),
                            "strategy_config": {"binding_mode": "OBSERVE_ONLY"},
                            "supported_streams": ["bbo"],
                            "supported_exchanges": ["bybit"],
                        },
                        "momentum_v1": {
                            "strategy_file": str(momentum_file),
                            "strategy_config": {"binding_mode": "PAPER_DIRECTIONAL_V1", "orderQty": 1},
                            "supported_streams": ["trade"],
                            "supported_exchanges": ["binance"],
                        },
                    },
                },
            )

            res = self._run(
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--binding-map-json",
                str(binding_map_json),
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["bound_shadow_runnable_count"], 2)
            self.assertEqual(payload["bound_directional_count"], 1)
            self.assertEqual(payload["bound_observe_only_count"], 1)
            by_strategy = {item["strategy_id"]: item for item in payload["items"]}
            self.assertEqual(
                by_strategy["candidate_strategy::spread_reversion_v1::pack_a::bnbusdt"]["shadow_tradeability_class"],
                "OBSERVE_ONLY",
            )
            self.assertEqual(
                by_strategy["candidate_strategy::momentum_v1::pack_c::btcusdt"]["shadow_tradeability_class"],
                "DIRECTIONAL",
            )

    def test_stream_mismatch_becomes_unbound_config_gap(self):
        with tempfile.TemporaryDirectory(prefix="candidate_runtime_binding_stream_gap_") as td:
            root = Path(td)
            contract_json = root / "contract.json"
            binding_map_json = root / "binding_map.json"
            out_json = root / "binding.json"
            strategy_file = root / "SpreadRuntimeStrategy.js"
            strategy_file.write_text(
                "export default class SpreadRuntimeStrategy { async onEvent() {} }\n",
                encoding="utf-8",
            )
            write_json(contract_json, base_contract([translated_item()]))
            write_json(
                binding_map_json,
                {
                    "schema_version": "family_shadow_runtime_binding_map_v0",
                    "generated_ts_utc": "2026-03-08T15:00:00Z",
                    "bindings": {
                        "spread_reversion_v1": {
                            "strategy_file": str(strategy_file),
                            "strategy_config": {},
                            "supported_streams": ["trade"],
                            "supported_exchanges": ["bybit"],
                        }
                    },
                },
            )

            res = self._run(
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--binding-map-json",
                str(binding_map_json),
                "--out-json",
                str(out_json),
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            item = payload["items"][0]
            self.assertEqual(item["runtime_binding_status"], "UNBOUND_CONFIG_GAP")
            self.assertEqual(item["binding_reason"], "SPEC_STREAM_UNSUPPORTED")
