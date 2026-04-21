import argparse
import importlib.util
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO / "tools" / "hypothesis_family_audit_v0.py"
SPEC = importlib.util.spec_from_file_location("hypothesis_family_audit_v0", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable_to_load_module:{MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class HypothesisFamilyAuditV0Tests(unittest.TestCase):
    def _args(self):
        return argparse.Namespace(
            candidate_review_json=str(REPO / "tools" / "phase6_state" / "candidate_review_v2.json"),
            promotion_index_json=str(REPO / "tools" / "phase6_state" / "promotion_index.json"),
            runtime_binding_json=str(REPO / "tools" / "phase6_state" / "candidate_strategy_runtime_binding_v0.json"),
            medium_shadow_result_json=str(REPO / "tools" / "phase7_medium_shadow_result_v0.json"),
            continuation_result_json=str(REPO / "tools" / "phase7_continuation_validation_result_v1.json"),
            profitability_result_json=str(REPO / "tools" / "phase7_profitability_analysis_v0.json"),
            canonical_truth_registry_json=str(REPO / "tools" / "system_state" / "canonical_truth_registry_v0.json"),
            audit_json=str(REPO / "tools" / "hypothesis_family_audit_v0.json"),
            policy_json=str(REPO / "tools" / "hypothesis_family_nightly_policy_v0.json"),
            report_md=str(REPO / "tools" / "hypothesis_family_audit_output" / "hypothesis_family_audit_report_v0.md"),
            generated_ts="2026-04-06T00:00:00Z",
        )

    def test_current_family_policy_preserves_controlled_discovery_while_reducing_noise(self):
        audit, policy = MODULE.build_outputs(self._args())
        by_family = {row["family_id"]: row for row in policy["policy_table"]}

        self.assertEqual(by_family["momentum_v1"]["role"], "TRADING")
        self.assertEqual(by_family["momentum_v1"]["status"], "WEAK")
        self.assertEqual(by_family["momentum_v1"]["nightly_mode"], "REDUCED_NIGHTLY")

        self.assertEqual(by_family["return_reversal_v1"]["role"], "TRADING")
        self.assertEqual(by_family["return_reversal_v1"]["status"], "FAILED")
        self.assertEqual(by_family["return_reversal_v1"]["nightly_mode"], "REDUCED_NIGHTLY")

        self.assertEqual(by_family["spread_reversion_v1"]["role"], "CONTEXT")
        self.assertEqual(by_family["spread_reversion_v1"]["nightly_mode"], "REDUCED_NIGHTLY")

        self.assertEqual(audit["safety_check"]["active_full_nightly_trading_family_count"], 0)
        self.assertTrue(audit["safety_check"]["passes_controlled_discovery_guard"])
        self.assertEqual(policy["cost_aware_prefilter"]["status"], "POLICY_DEFINED_NOT_APPLIED_BY_THIS_SCRIPT")
        self.assertIn("gross_pnl <= 0", policy["cost_aware_prefilter"]["rule"])
        self.assertIn("candidate_review_rows_without_selected_family", audit["unclassified_records"])


if __name__ == "__main__":
    unittest.main()
