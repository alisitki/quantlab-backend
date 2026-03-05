import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "tools" / "run-multi-hypothesis.js"


class RunMultiHypothesisGlueContractTests(unittest.TestCase):
    def test_family_table_contains_expected_order_and_ids(self) -> None:
        text = RUNNER.read_text(encoding="utf-8")
        m = re.search(r"const FAMILY_TABLE = Object\.freeze\(\[(.*?)\]\);", text, flags=re.S)
        self.assertIsNotNone(m, "FAMILY_TABLE block must exist")
        table_block = m.group(1)
        family_ids = re.findall(r'familyId:\s*"([^"]+)"', table_block)
        self.assertEqual(
            family_ids,
            [
                "family_a_patternscanner",
                "family_b_simple_momentum",
                "return_reversal_v1",
                "momentum_v1",
                "volatility_clustering_v1",
                "spread_reversion_v1",
                "volume_vol_link_v1",
                "jump_reversion_v1",
            ],
        )
        self.assertIn('evidenceEligible: true', table_block)
        self.assertIn('evidenceEligible: false', table_block)

    def test_main_orchestration_is_table_driven(self) -> None:
        text = RUNNER.read_text(encoding="utf-8")
        self.assertGreaterEqual(text.count("for (const familyMeta of FAMILY_TABLE)"), 2)
        self.assertIn("const familyRowsById = new Map();", text)
        self.assertNotIn(
            "for (const familyRow of [rrRow, momRow, vcRow, srRow, vvlRow, jrRow])",
            text,
        )

    def test_compare_basis_and_skipped_hash_contract(self) -> None:
        text = RUNNER.read_text(encoding="utf-8")
        self.assertIn("function compareBasisFromHeader(headerArr)", text)
        self.assertIn('const SKIPPED_HASH_PLACEHOLDER = "-";', text)
        self.assertIsNone(re.search(r'const compareBasis = "exchange,', text))

        for header_name in ("RR_HEADER", "MOM_HEADER", "VC_HEADER", "SR_HEADER", "VVL_HEADER", "JR_HEADER"):
            self.assertIn(f"compareBasisFromHeader({header_name})", text)

        self.assertIn('status.startsWith("SKIPPED_")', text)
        self.assertGreaterEqual(text.count("let primaryHash = SKIPPED_HASH_PLACEHOLDER;"), 6)
        self.assertGreaterEqual(text.count("let replayHash = SKIPPED_HASH_PLACEHOLDER;"), 6)


if __name__ == "__main__":
    unittest.main()
