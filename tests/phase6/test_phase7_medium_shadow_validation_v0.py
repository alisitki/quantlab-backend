import importlib.util
import json
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO / "tools" / "phase7_medium_shadow_validation_v0.py"
SPEC = importlib.util.spec_from_file_location("phase7_medium_shadow_validation_v0", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable_to_load_module:{MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Phase7MediumShadowValidationV0Tests(unittest.TestCase):
    def test_selected_targets_matches_expected_medium_symbols(self):
        shortlist = json.loads((REPO / "tools" / "phase6_state" / "shadow_shortlist_v0.json").read_text(encoding="utf-8"))
        expectation = json.loads(
            (REPO / "tools" / "phase7_expectation_audit_output" / "expectation_audit_report_v0.json").read_text(
                encoding="utf-8"
            )
        )
        window_plan = json.loads(
            (REPO / "tools" / "phase7_expectation_audit_output" / "shadow_window_plan_v0.json").read_text(
                encoding="utf-8"
            )
        )
        binding = json.loads(
            (REPO / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json").read_text(
                encoding="utf-8"
            )
        )

        targets = MODULE.selected_targets(shortlist, expectation, window_plan, binding)

        self.assertEqual([target["symbol"] for target in targets], ["xrpusdt", "linkusdt", "ethusdt", "avaxusdt"])
        self.assertTrue(all(target["binding_row"]["runtime_binding_status"] == "BOUND_SHADOW_RUNNABLE" for target in targets))

    def test_verdict_from_metrics_maps_expected_lane(self):
        continue_metrics = {
            "verify_soft_live_pass": True,
            "processed_event_count": 100,
            "decision_count": 4,
            "fill_count": 2,
            "trade_transitions_per_1k_events": 1.0,
        }
        weak_metrics = {
            "verify_soft_live_pass": True,
            "processed_event_count": 100,
            "decision_count": 2,
            "fill_count": 0,
            "trade_transitions_per_1k_events": 1.0,
        }
        no_signal_metrics = {
            "verify_soft_live_pass": True,
            "processed_event_count": 100,
            "decision_count": 0,
            "fill_count": 0,
            "trade_transitions_per_1k_events": 0.0,
        }
        churn_metrics = {
            "verify_soft_live_pass": True,
            "processed_event_count": 100,
            "decision_count": 10,
            "fill_count": 1,
            "trade_transitions_per_1k_events": 3.0,
        }

        self.assertEqual(MODULE.verdict_from_metrics(continue_metrics, False, 2.5)[0], "CONTINUE")
        self.assertEqual(MODULE.verdict_from_metrics(weak_metrics, False, 2.5)[0], "WEAK")
        self.assertEqual(MODULE.verdict_from_metrics(no_signal_metrics, False, 2.5)[0], "NO_SIGNAL")
        self.assertEqual(MODULE.verdict_from_metrics(churn_metrics, False, 2.5)[0], "FAIL_CHURN")
        self.assertEqual(MODULE.verdict_from_metrics({}, True, 2.5)[0], "INVALID_RUN")

    def test_apply_reduction_picks_top_two_by_priority_then_tiebreaks(self):
        rows = [
            {
                "strategy_id": "s3",
                "rank": 3,
                "family_id": "return_reversal_v1",
                "symbol": "ethusdt",
                "exchange": "binance",
                "verdict": "WEAK",
                "verdict_reason": "weak",
                "metrics": {
                    "risk_reject_count": 0,
                    "fill_count": 0,
                    "decision_count": 2,
                    "trade_transitions_per_1k_events": 0.7,
                },
                "eliminated": True,
                "elimination_reason": "weak",
            },
            {
                "strategy_id": "s2",
                "rank": 2,
                "family_id": "return_reversal_v1",
                "symbol": "linkusdt",
                "exchange": "binance",
                "verdict": "CONTINUE",
                "verdict_reason": "continue",
                "metrics": {
                    "risk_reject_count": 1,
                    "fill_count": 3,
                    "decision_count": 4,
                    "trade_transitions_per_1k_events": 0.9,
                },
                "eliminated": True,
                "elimination_reason": "continue",
            },
            {
                "strategy_id": "s1",
                "rank": 1,
                "family_id": "return_reversal_v1",
                "symbol": "xrpusdt",
                "exchange": "binance",
                "verdict": "CONTINUE",
                "verdict_reason": "continue",
                "metrics": {
                    "risk_reject_count": 0,
                    "fill_count": 1,
                    "decision_count": 5,
                    "trade_transitions_per_1k_events": 0.5,
                },
                "eliminated": True,
                "elimination_reason": "continue",
            },
        ]

        selected = MODULE.apply_reduction(rows, 2)

        self.assertEqual([row["strategy_id"] for row in selected], ["s1", "s2"])
        self.assertTrue(rows[0]["eliminated"])
        self.assertEqual(rows[0]["elimination_reason"], "lower_priority_than_top2")
        self.assertFalse(rows[1]["eliminated"])
        self.assertFalse(rows[2]["eliminated"])


if __name__ == "__main__":
    unittest.main()
