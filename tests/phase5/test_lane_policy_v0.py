import json
import tempfile
import unittest
from pathlib import Path

from tools.phase5_big_hunt_scheduler_v1 import apply_runtime_lane_policy, build_v0_command
from tools.phase5_lane_policy_v0 import load_lane_policy, resolve_lane_policy


class LanePolicyV0Tests(unittest.TestCase):
    def _write_policy(self, root: Path) -> Path:
        path = root / "lane_policy_v0.json"
        path.write_text(
            json.dumps(
                {
                    "default": {
                        "max_symbols": 20,
                        "per_run_timeout_min": 12,
                        "max_wall_min": 120,
                    },
                    "overrides": {
                        "binance/bbo": {
                            "max_symbols": 4,
                            "per_run_timeout_min": 25,
                            "max_wall_min": 180,
                        }
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return path

    def test_default_resolution(self):
        with tempfile.TemporaryDirectory(prefix="lane_policy_default_") as td:
            policy = load_lane_policy(self._write_policy(Path(td)))
        resolved = resolve_lane_policy("binance", "trade", policy)
        self.assertEqual(
            resolved,
            {"max_symbols": 20, "per_run_timeout_min": 12, "max_wall_min": 120},
        )

    def test_binance_bbo_override_resolution(self):
        with tempfile.TemporaryDirectory(prefix="lane_policy_bbo_") as td:
            policy = load_lane_policy(self._write_policy(Path(td)))
        resolved = resolve_lane_policy("binance", "bbo", policy)
        self.assertEqual(
            resolved,
            {"max_symbols": 4, "per_run_timeout_min": 25, "max_wall_min": 180},
        )

    def test_unknown_lane_uses_default(self):
        with tempfile.TemporaryDirectory(prefix="lane_policy_unknown_") as td:
            policy = load_lane_policy(self._write_policy(Path(td)))
        resolved = resolve_lane_policy("okx", "book", policy)
        self.assertEqual(
            resolved,
            {"max_symbols": 20, "per_run_timeout_min": 12, "max_wall_min": 120},
        )

    def test_scheduler_command_assembly_uses_override_values(self):
        with tempfile.TemporaryDirectory(prefix="lane_policy_cmd_") as td:
            policy = load_lane_policy(self._write_policy(Path(td)))
        plan = {
            "exchange": "binance",
            "stream": "bbo",
            "start": "20260105",
            "end": "20260105",
            "object_keys_tsv": "/tmp/object_keys.tsv",
            "max_symbols": 20,
            "per_run_timeout_min": 12,
            "max_wall_min": 120,
        }
        effective, meta = apply_runtime_lane_policy(plan, policy)
        cmd = build_v0_command(effective, "run-1")
        self.assertEqual(meta["lane_policy_applied"], "OVERRIDE")
        self.assertEqual(cmd[cmd.index("--max-symbols") + 1], "4")
        self.assertEqual(cmd[cmd.index("--per-run-timeout-min") + 1], "25")
        self.assertEqual(cmd[cmd.index("--max-wall-min") + 1], "180")


if __name__ == "__main__":
    unittest.main()
