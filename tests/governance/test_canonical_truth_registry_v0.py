from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_TOOL = REPO_ROOT / "tools" / "refresh-governance-registry-v0.py"
REGISTRY_JSON = REPO_ROOT / "tools" / "system_state" / "canonical_truth_registry_v0.json"

REQUIRED_CONCEPTS = {
    "family_role_classification",
    "primary_directional_selection",
    "candidate_review",
    "candidate_strategy_contract",
    "runtime_binding",
    "active_shadow_subset",
    "continuous_session_state",
    "replay_truth",
    "strategyd_truth",
    "scheduler_truth",
}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class CanonicalTruthRegistryV0Tests(unittest.TestCase):
    def test_registry_tool_emits_required_concepts_and_active_paths(self):
        with tempfile.TemporaryDirectory(prefix="governance_registry_") as td:
            out_json = Path(td) / "canonical_truth_registry_v0.json"
            proc = subprocess.run(
                [sys.executable, str(REGISTRY_TOOL), "--out-json", str(out_json)],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            payload = read_json(out_json)
            self.assertEqual(payload["schema_version"], "canonical_truth_registry_v0")
            self.assertEqual({item["concept"] for item in payload["concepts"]}, REQUIRED_CONCEPTS)
            self.assertIn("current_critical_path", payload["active_paths"])
            self.assertIn("active_tool_path", payload["active_paths"])
            self.assertIn("active_service_path", payload["active_paths"])
            self.assertIn("not_in_current_critical_path", payload["active_paths"])

    def test_committed_registry_and_demoted_surfaces_are_self_describing(self):
        registry = read_json(REGISTRY_JSON)
        self.assertEqual(registry["schema_version"], "canonical_truth_registry_v0")
        self.assertEqual({item["concept"] for item in registry["concepts"]}, REQUIRED_CONCEPTS)

        role = read_json(REPO_ROOT / "tools" / "phase6_state" / "hypothesis_family_role_classification_v0.json")
        primary = read_json(REPO_ROOT / "tools" / "phase6_state" / "primary_directional_family_selection_v0.json")
        watchlist = read_json(REPO_ROOT / "tools" / "shadow_state" / "shadow_watchlist_v0.json")
        bound_watchlist = read_json(REPO_ROOT / "tools" / "shadow_state" / "shadow_bound_launch_watchlist_v0.json")

        self.assertEqual(role["governance"]["surface_role"], "DERIVED_SUMMARY")
        self.assertEqual(primary["governance"]["surface_role"], "DERIVED_SELECTION_SUMMARY")
        self.assertEqual(watchlist["governance"]["surface_role"], "ACTIVE_SHADOW_SUBSET")
        self.assertEqual(bound_watchlist["governance"]["surface_role"], "ONE_SHOT_BOUND_LAUNCH_SELECTION")


if __name__ == "__main__":
    unittest.main()
