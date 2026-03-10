from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TOOL = REPO_ROOT / "tools" / "phase6_primary_directional_family_selection_v0.py"


class PrimaryDirectionalFamilySelectionV0Test(unittest.TestCase):
    def write_candidate_review(self, path: Path, rows: list[dict[str, str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["rank", "decision_tier", "pack_id", "pack_path"], delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

    def run_tool(self, tmp: Path, role_items, rows, reports_by_pack, contract_items, binding_items):
        role_json = tmp / "role.json"
        role_json.write_text(
            json.dumps(
                {
                    "schema_version": "hypothesis_family_role_classification_v0",
                    "items": role_items,
                }
            ),
            encoding="utf-8",
        )
        review_tsv = tmp / "candidate_review.tsv"
        self.write_candidate_review(review_tsv, rows)
        for pack_name, reports in reports_by_pack.items():
            pack_dir = tmp / pack_name
            for relpath, payload in reports:
                full = pack_dir / relpath
                full.parent.mkdir(parents=True, exist_ok=True)
                full.write_text(json.dumps(payload), encoding="utf-8")
            plan = {"selected_symbols": ["sym1"] if "single" in pack_name else ["sym1", "sym2"]}
            (pack_dir / "campaign_plan.json").write_text(json.dumps(plan), encoding="utf-8")

        contract_json = tmp / "contract.json"
        contract_json.write_text(
            json.dumps({"schema_version": "candidate_strategy_contract_v0", "items": contract_items}),
            encoding="utf-8",
        )
        binding_json = tmp / "binding.json"
        binding_json.write_text(
            json.dumps({"schema_version": "candidate_strategy_runtime_binding_v0", "items": binding_items}),
            encoding="utf-8",
        )
        out_json = tmp / "out.json"
        subprocess.run(
            [
                sys.executable,
                str(TOOL),
                "--role-classification-json",
                str(role_json),
                "--candidate-review-tsv",
                str(review_tsv),
                "--candidate-strategy-contract-json",
                str(contract_json),
                "--candidate-strategy-runtime-binding-json",
                str(binding_json),
                "--out-json",
                str(out_json),
            ],
            cwd=REPO_ROOT,
            check=True,
        )
        return json.loads(out_json.read_text(encoding="utf-8"))

    def test_primary_directional_tie_break_uses_support_then_t_stat(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            role_items = [
                {"family_id": "return_reversal_v1", "role": "PRIMARY_DIRECTIONAL", "paper_execution_ready_now": False, "rationale": "r1", "next_path": "n1"},
                {"family_id": "momentum_v1", "role": "PRIMARY_DIRECTIONAL", "paper_execution_ready_now": False, "rationale": "m1", "next_path": "n2"},
                {"family_id": "spread_reversion_v1", "role": "CONTEXT_GUARD", "paper_execution_ready_now": False, "rationale": "s1", "next_path": "n3"},
            ]
            rows = [
                {"rank": "1", "decision_tier": "PROMOTE_STRONG", "pack_id": "pack_single_a", "pack_path": str(tmp / "pack_single_a")},
                {"rank": "2", "decision_tier": "PROMOTE", "pack_id": "pack_single_b", "pack_path": str(tmp / "pack_single_b")},
            ]
            reports = {
                "pack_single_a": [
                    ("runs/sym1/artifacts/multi_hypothesis/family_return_reversal_report.json", {"family_id": "return_reversal_v1", "result": {"selected_cell": {"event_count": 400, "t_stat": -5.0}}}),
                    ("runs/sym1/artifacts/multi_hypothesis/family_momentum_report.json", {"family_id": "momentum_v1", "result": {"selected_cell": {"event_count": 300, "t_stat": 3.0}}}),
                ],
                "pack_single_b": [
                    ("runs/sym1/artifacts/multi_hypothesis/family_return_reversal_report.json", {"family_id": "return_reversal_v1", "result": {"selected_cell": {"event_count": 500, "t_stat": -6.0}}}),
                    ("runs/sym1/artifacts/multi_hypothesis/family_momentum_report.json", {"family_id": "momentum_v1", "result": {"selected_cell": {"event_count": 200, "t_stat": 2.5}}}),
                ],
            }
            payload = self.run_tool(tmp, role_items, rows, reports, contract_items=[], binding_items=[])
            self.assertEqual(payload["selected_family_id"], "return_reversal_v1")
            order = [item["family_id"] for item in payload["scorecard"]]
            self.assertEqual(order, ["return_reversal_v1", "momentum_v1"])

    def test_non_primary_families_are_excluded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            role_items = [
                {"family_id": "momentum_v1", "role": "PRIMARY_DIRECTIONAL", "paper_execution_ready_now": False, "rationale": "m1", "next_path": "n"},
                {"family_id": "spread_reversion_v1", "role": "CONTEXT_GUARD", "paper_execution_ready_now": False, "rationale": "s1", "next_path": "n"},
                {"family_id": "family_a_patternscanner", "role": "DISCOVERY_RESEARCH", "paper_execution_ready_now": False, "rationale": "a1", "next_path": "n"},
            ]
            rows = [{"rank": "1", "decision_tier": "PROMOTE", "pack_id": "pack_single_a", "pack_path": str(tmp / "pack_single_a")}]
            reports = {
                "pack_single_a": [
                    ("runs/sym1/artifacts/multi_hypothesis/family_momentum_report.json", {"family_id": "momentum_v1", "result": {"selected_cell": {"event_count": 210, "t_stat": 2.1}}}),
                    ("runs/sym1/artifacts/multi_hypothesis/family_spread_reversion_report.json", {"family_id": "spread_reversion_v1", "result": {"selected_cell": {"event_count": 210, "t_stat": -2.1}}}),
                ]
            }
            payload = self.run_tool(tmp, role_items, rows, reports, contract_items=[], binding_items=[])
            self.assertEqual(payload["eligible_families"], ["momentum_v1"])
            self.assertEqual([item["family_id"] for item in payload["scorecard"]], ["momentum_v1"])

    def test_counts_translation_and_binding(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            role_items = [
                {"family_id": "jump_reversion_v1", "role": "PRIMARY_DIRECTIONAL", "paper_execution_ready_now": False, "rationale": "j1", "next_path": "n"},
            ]
            rows = [{"rank": "1", "decision_tier": "PROMOTE_STRONG", "pack_id": "pack_single_a", "pack_path": str(tmp / "pack_single_a")}]
            reports = {
                "pack_single_a": [
                    ("runs/sym1/artifacts/multi_hypothesis/family_jump_reversion_report.json", {"family_id": "jump_reversion_v1", "result": {"selected_cell": {"jump_count": 250, "t_stat": 2.7}}}),
                ]
            }
            payload = self.run_tool(
                tmp,
                role_items,
                rows,
                reports,
                contract_items=[{"translation_status": "TRANSLATABLE", "strategy_spec": {"family_id": "jump_reversion_v1"}}],
                binding_items=[{"family_id": "jump_reversion_v1", "runtime_binding_status": "BOUND_SHADOW_RUNNABLE"}],
            )
            item = payload["scorecard"][0]
            self.assertEqual(item["strategy_translatable_now_count"], 1)
            self.assertEqual(item["runtime_bindable_now_count"], 1)
            self.assertEqual(item["selected_support_metric_name"], "jump_count")


if __name__ == "__main__":
    unittest.main()
