import argparse
import importlib.util
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO / "tools" / "phase7_profitability_analysis_v0.py"
SPEC = importlib.util.spec_from_file_location("phase7_profitability_analysis_v0", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable_to_load_module:{MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Phase7ProfitabilityAnalysisV0Tests(unittest.TestCase):
    def _build_current_result(self):
        args = argparse.Namespace(
            continuation_result_json=str(REPO / "tools" / "phase7_continuation_validation_result_v1.json"),
            canonical_truth_registry_json=str(REPO / "tools" / "system_state" / "canonical_truth_registry_v0.json"),
            output_json=str(REPO / "tools" / "phase7_profitability_analysis_v0.json"),
            report_md=str(
                REPO / "tools" / "phase7_profitability_analysis_output" / "phase7_profitability_analysis_report_v0.md"
            ),
            generated_ts="2026-04-06T00:00:00Z",
        )
        return MODULE.build_result(args)

    def test_current_artifacts_drop_both_when_gross_is_negative_before_fees(self):
        result = self._build_current_result()

        self.assertEqual(result["final_decision"], "NEITHER_CONTINUE")
        rows = {row["symbol"]: row for row in result["per_strategy"]}
        self.assertEqual(set(rows), {"linkusdt", "avaxusdt"})

        for row in rows.values():
            self.assertEqual(row["verdict"], "DROP")
            self.assertLess(row["gross_performance"]["total_gross_pnl_quote"], 0)
            self.assertEqual(row["edge_strength"]["edge_statement"], "no real edge")

    def test_current_artifacts_identify_fees_as_burden_but_not_sole_cause(self):
        result = self._build_current_result()
        rows = {row["symbol"]: row for row in result["per_strategy"]}

        self.assertEqual(rows["linkusdt"]["fee_burden"]["total_fee_quote"], 0.1455256)
        self.assertEqual(rows["avaxusdt"]["fee_burden"]["total_fee_quote"], 0.4459356)
        self.assertEqual(rows["linkusdt"]["funding_impact"]["funding_cost_quote"], 0.0)
        self.assertEqual(rows["avaxusdt"]["funding_impact"]["funding_cost_quote"], 0.0)
        self.assertIn("gross pnl is not positive", rows["linkusdt"]["fee_burden"]["fee_explanation"])
        self.assertIn("gross pnl is not positive", rows["avaxusdt"]["fee_burden"]["fee_explanation"])


if __name__ == "__main__":
    unittest.main()
