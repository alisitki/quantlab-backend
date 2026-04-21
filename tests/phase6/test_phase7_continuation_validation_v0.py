import importlib.util
import json
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO / "tools" / "phase7_continuation_validation_v0.py"
SPEC = importlib.util.spec_from_file_location("phase7_continuation_validation_v0", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable_to_load_module:{MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Phase7ContinuationValidationV0Tests(unittest.TestCase):
    def test_selected_targets_matches_expected_continuation_symbols(self):
        medium_result = json.loads((REPO / "tools" / "phase7_medium_shadow_result_v0.json").read_text(encoding="utf-8"))
        binding = json.loads(
            (REPO / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json").read_text(
                encoding="utf-8"
            )
        )

        targets = MODULE.selected_targets(medium_result, binding)

        self.assertEqual([target["symbol"] for target in targets], ["linkusdt", "avaxusdt"])
        self.assertTrue(all(target["binding_row"]["runtime_binding_status"] == "BOUND_SHADOW_RUNNABLE" for target in targets))

    def test_runtime_config_for_run_preserves_paper_directional_binding(self):
        binding = json.loads(
            (REPO / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json").read_text(
                encoding="utf-8"
            )
        )
        source_row = next(
            row
            for row in binding["items"]
            if row["strategy_id"].endswith("::linkusdt")
        )

        original_config = json.loads(json.dumps(source_row["runtime_strategy_config"]))
        override = MODULE.runtime_config_for_run(source_row, "paper_directional")

        self.assertEqual(override, original_config)
        self.assertEqual(source_row["runtime_strategy_config"], original_config)

    def test_runtime_config_for_run_supports_observe_only_without_zeroing_order_qty(self):
        binding = json.loads(
            (REPO / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json").read_text(
                encoding="utf-8"
            )
        )
        source_row = next(
            row
            for row in binding["items"]
            if row["strategy_id"].endswith("::linkusdt")
        )

        override = MODULE.runtime_config_for_run(source_row, "observe_only")

        self.assertEqual(override["binding_mode"], "OBSERVE_ONLY")
        self.assertEqual(override["orderQty"], source_row["runtime_strategy_config"]["orderQty"])

    def test_compare_rows_prefers_stronger_candidate_without_dropping_both_survivors(self):
        rows = [
            {
                "strategy_id": "link",
                "rank": 16,
                "symbol": "linkusdt",
                "verdict": "WEAK_CONTINUE",
                "metrics": {
                    "risk_reject_count": 0,
                    "fill_count": 0,
                    "decision_count": 12,
                    "processed_event_count": 1000,
                    "trade_transitions_per_1k_events": 0.0,
                },
            },
            {
                "strategy_id": "avax",
                "rank": 24,
                "symbol": "avaxusdt",
                "verdict": "WEAK_CONTINUE",
                "metrics": {
                    "risk_reject_count": 0,
                    "fill_count": 0,
                    "decision_count": 20,
                    "processed_event_count": 1200,
                    "trade_transitions_per_1k_events": 0.0,
                },
            },
        ]

        comparison = MODULE.compare_rows(rows)

        self.assertEqual(comparison["stronger_candidate"]["symbol"], "avaxusdt")
        self.assertEqual(comparison["weaker_candidate"]["symbol"], "linkusdt")
        self.assertTrue(comparison["both_survive"])
        self.assertEqual(comparison["final_recommendation"], "BOTH_ADVANCE")

    def test_compare_rows_single_survivor_maps_to_symbol_specific_final_decision(self):
        rows = [
            {
                "strategy_id": "link",
                "rank": 16,
                "symbol": "linkusdt",
                "verdict": "DROP",
                "metrics": {
                    "risk_reject_count": 0,
                    "fill_count": 0,
                    "decision_count": 0,
                    "processed_event_count": 1000,
                    "trade_transitions_per_1k_events": 0.0,
                },
            },
            {
                "strategy_id": "avax",
                "rank": 24,
                "symbol": "avaxusdt",
                "verdict": "WEAK_CONTINUE",
                "metrics": {
                    "risk_reject_count": 0,
                    "fill_count": 0,
                    "decision_count": 5,
                    "processed_event_count": 1200,
                    "trade_transitions_per_1k_events": 0.0,
                },
            },
        ]

        comparison = MODULE.compare_rows(rows)

        self.assertEqual(comparison["final_recommendation"], "AVAX_ONLY")


if __name__ == "__main__":
    unittest.main()
