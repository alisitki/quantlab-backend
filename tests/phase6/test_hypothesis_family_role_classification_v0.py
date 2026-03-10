from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TOOL = REPO_ROOT / "tools" / "phase6_hypothesis_family_role_classification_v0.py"


class HypothesisFamilyRoleClassificationV0Test(unittest.TestCase):
    def run_tool(self, tmp: Path, readme_text: str, reports: dict[str, list[tuple[str, dict]]], contract_items, binding_items):
        hypotheses_dir = tmp / "tools" / "hypotheses"
        hypotheses_dir.mkdir(parents=True)
        (hypotheses_dir / "README.md").write_text(readme_text, encoding="utf-8")
        (hypotheses_dir / "family_a_patternscanner.md").write_text(
            "# x\n- family_id: `family_a_patternscanner`\n", encoding="utf-8"
        )
        (hypotheses_dir / "family_b_simple_momentum.md").write_text(
            "# x\n- family_id: `family_b_simple_momentum`\n", encoding="utf-8"
        )
        (hypotheses_dir / "simple_momentum_family.py").write_text(
            'REPORT = {"family_id": "family_b_simple_momentum"}\n', encoding="utf-8"
        )
        (hypotheses_dir / "spread_reversion_v1.py").write_text(
            'REPORT = {"family_id": "spread_reversion_v1"}\n', encoding="utf-8"
        )
        (hypotheses_dir / "momentum_v1.py").write_text(
            'REPORT = {"family_id": "momentum_v1"}\n', encoding="utf-8"
        )
        (hypotheses_dir / "latency_leadlag_v1.py").write_text(
            'REPORT = {"family_id": "latency_leadlag_v1"}\n', encoding="utf-8"
        )

        packs_root = tmp / "packs"
        rows = []
        for idx, (pack_name, pack_reports) in enumerate(reports.items(), start=1):
            pack_path = packs_root / pack_name
            for relpath, payload in pack_reports:
                full = pack_path / relpath
                full.parent.mkdir(parents=True, exist_ok=True)
                full.write_text(json.dumps(payload), encoding="utf-8")
            rows.append(
                {
                    "rank": str(idx),
                    "decision_tier": "PROMOTE",
                    "pack_id": pack_name,
                    "pack_path": str(pack_path),
                }
            )

        review_tsv = tmp / "candidate_review.tsv"
        with review_tsv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["rank", "decision_tier", "pack_id", "pack_path"], delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

        contract_json = tmp / "candidate_strategy_contract_v0.json"
        contract_json.write_text(
            json.dumps(
                {
                    "schema_version": "candidate_strategy_contract_v0",
                    "items": contract_items,
                }
            ),
            encoding="utf-8",
        )

        binding_json = tmp / "candidate_strategy_runtime_binding_v0.json"
        binding_json.write_text(
            json.dumps(
                {
                    "schema_version": "candidate_strategy_runtime_binding_v0",
                    "items": binding_items,
                }
            ),
            encoding="utf-8",
        )

        out_json = tmp / "out.json"
        subprocess.run(
            [
                sys.executable,
                str(TOOL),
                "--hypotheses-readme",
                str(hypotheses_dir / "README.md"),
                "--hypotheses-dir",
                str(hypotheses_dir),
                "--candidate-review-tsv",
                str(review_tsv),
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--candidate-strategy-runtime-binding-json",
                str(binding_json),
                "--out-json",
                str(out_json),
            ],
            check=True,
            cwd=REPO_ROOT,
        )
        return json.loads(out_json.read_text(encoding="utf-8"))

    def test_classifies_roles_and_current_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            payload = self.run_tool(
                tmp=tmp,
                readme_text=(
                    "# Multi-Hypothesis Contract v0\n\n"
                    "- families:\n"
                    "  - `family_a_patternscanner`\n"
                    "  - `family_b_simple_momentum`\n"
                    "  - `spread_reversion_v1`\n"
                    "  - `momentum_v1`\n"
                ),
                reports={
                    "pack_a": [
                        (
                            "runs/bnbusdt/artifacts/multi_hypothesis/family_A_report.json",
                            {"family_id": "family_a_patternscanner"},
                        ),
                        (
                            "runs/bnbusdt/artifacts/multi_hypothesis/family_spread_reversion_report.json",
                            {"family_id": "spread_reversion_v1"},
                        ),
                    ],
                    "pack_b": [
                        (
                            "runs/ethusdt/artifacts/multi_hypothesis/family_B_report.json",
                            {"family_id": "family_b_simple_momentum"},
                        ),
                        (
                            "runs/ethusdt/artifacts/multi_hypothesis/family_momentum_report.json",
                            {"family_id": "momentum_v1"},
                        ),
                    ],
                },
                contract_items=[
                    {
                        "translation_status": "TRANSLATABLE",
                        "strategy_spec": {"family_id": "spread_reversion_v1"},
                    }
                ],
                binding_items=[
                    {
                        "family_id": "spread_reversion_v1",
                        "runtime_binding_status": "BOUND_SHADOW_RUNNABLE",
                    }
                ],
            )

            self.assertEqual(payload["current_multi_hypothesis_family_count"], 4)
            self.assertEqual(payload["repo_extra_hypothesis_ids"], ["latency_leadlag_v1"])
            items = {item["family_id"]: item for item in payload["items"]}
            self.assertEqual(items["family_a_patternscanner"]["role"], "DISCOVERY_RESEARCH")
            self.assertTrue(items["family_a_patternscanner"]["not_ready_now"])
            self.assertEqual(items["family_b_simple_momentum"]["role"], "PRIMARY_DIRECTIONAL")
            self.assertEqual(items["spread_reversion_v1"]["role"], "CONTEXT_GUARD")
            self.assertTrue(items["spread_reversion_v1"]["strategy_translatable_now"])
            self.assertTrue(items["spread_reversion_v1"]["runtime_bindable_now"])
            self.assertFalse(items["spread_reversion_v1"]["paper_execution_ready_now"])
            self.assertTrue(items["spread_reversion_v1"]["support_only_now"])
            self.assertEqual(items["momentum_v1"]["strategy_translation_eligibility"], "PRIMARY_DIRECTIONAL_NOT_IN_TRANSLATOR_SCOPE")

    def test_requires_known_family_classification(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            hypotheses_dir = tmp / "tools" / "hypotheses"
            hypotheses_dir.mkdir(parents=True)
            (hypotheses_dir / "README.md").write_text("# x\n- `unknown_family`\n", encoding="utf-8")
            (hypotheses_dir / "unknown_family.py").write_text('REPORT={"family_id":"unknown_family"}\n', encoding="utf-8")
            review = tmp / "candidate_review.tsv"
            with review.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["rank", "decision_tier", "pack_id", "pack_path"], delimiter="\t")
                writer.writeheader()
            contract = tmp / "candidate_strategy_contract_v0.json"
            contract.write_text(json.dumps({"schema_version": "candidate_strategy_contract_v0", "items": []}), encoding="utf-8")
            binding = tmp / "candidate_strategy_runtime_binding_v0.json"
            binding.write_text(json.dumps({"schema_version": "candidate_strategy_runtime_binding_v0", "items": []}), encoding="utf-8")
            out = tmp / "out.json"
            proc = subprocess.run(
                [
                    sys.executable,
                    str(TOOL),
                    "--hypotheses-readme",
                    str(hypotheses_dir / "README.md"),
                    "--hypotheses-dir",
                    str(hypotheses_dir),
                    "--candidate-review-tsv",
                    str(review),
                    "--candidate-strategy-contract-json",
                    str(contract),
                    "--candidate-strategy-runtime-binding-json",
                    str(binding),
                    "--out-json",
                    str(out),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("missing classification for families: unknown_family", proc.stdout)

    def test_empty_candidate_rows_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            payload = self.run_tool(
                tmp=tmp,
                readme_text=(
                    "# Multi-Hypothesis Contract v0\n\n"
                    "- families:\n"
                    "  - `spread_reversion_v1`\n"
                    "  - `volatility_clustering_v1`\n"
                ),
                reports={},
                contract_items=[],
                binding_items=[],
            )
            items = {item["family_id"]: item for item in payload["items"]}
            self.assertEqual(items["spread_reversion_v1"]["candidate_report_count"], 0)
            self.assertFalse(items["spread_reversion_v1"]["strategy_translatable_now"])
            self.assertFalse(items["volatility_clustering_v1"]["runtime_bindable_now"])
            self.assertTrue(items["volatility_clustering_v1"]["support_only_now"])


if __name__ == "__main__":
    unittest.main()
