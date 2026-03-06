import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tools.phase5_big_hunt_plan_v2 import run_plan_generation


def write_inventory(path: Path, partitions: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {"partitions": partitions}
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class BigHuntPlanV2AllInventoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="bighunt_plan_v2_all_inventory_"))
        self.state_dir = self.tmpdir / "phase5_state"
        self.inventory_json = self.tmpdir / "inventory_state.json"
        write_inventory(
            self.inventory_json,
            {
                # binance/trade: two windows (0101..0102, 0102..0103), coverage exists.
                "binance/trade/btcusdt/20260101": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260102": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260103": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/ethusdt/20260102": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/ethusdt/20260103": {"status": "success", "day_quality_post": "GOOD"},
                # binance/bbo: one window.
                "binance/bbo/btcusdt/20260101": {"status": "success", "day_quality_post": "GOOD"},
                "binance/bbo/btcusdt/20260102": {"status": "success", "day_quality_post": "GOOD"},
                # bybit/trade: one candidate window but zero full coverage (A has day1 only, B has day2 only).
                "bybit/trade/adausdt/20260101": {"status": "success", "day_quality_post": "GOOD"},
                "bybit/trade/xrpusdt/20260102": {"status": "success", "day_quality_post": "GOOD"},
                # bybit/bbo: two windows with coverage.
                "bybit/bbo/solusdt/20260101": {"status": "success", "day_quality_post": "GOOD"},
                "bybit/bbo/solusdt/20260102": {"status": "success", "day_quality_post": "GOOD"},
                "bybit/bbo/solusdt/20260103": {"status": "success", "day_quality_post": "GOOD"},
            },
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _args(self, *, dry_run: bool, max_windows_per_pair: int = 0) -> SimpleNamespace:
        return SimpleNamespace(
            exchange="",
            stream="",
            exchanges="binance,bybit",
            streams="trade,bbo",
            window_days=2,
            lookback_days=30,
            all_dates=True,
            max_windows=0,
            max_windows_per_pair=max_windows_per_pair,
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            state_dir=str(self.state_dir),
            inventory_state_json=str(self.inventory_json),
            inventory_bucket="quantlab-compact",
            inventory_key="compacted/_state.json",
            inventory_s3_tool="/tmp/s3_compact_tool.py",
            object_keys_tsv_ref="state_selection/object_keys_selected.tsv",
            require_quality_pass=True,
            dry_run=dry_run,
        )

    def test_multi_pair_all_dates_deterministic(self):
        out1 = run_plan_generation(self._args(dry_run=True), repo=self.tmpdir)
        out2 = run_plan_generation(self._args(dry_run=True), repo=self.tmpdir)
        self.assertEqual(out1["pairs_considered"], 4)
        self.assertEqual(out1["windows_total"], 6)
        self.assertEqual(out1["skipped_no_coverage"], 1)
        self.assertEqual(out1["would_add_count"], 5)
        self.assertEqual(out1["added_count"], 0)
        self.assertEqual(out1["windows_csv"], out2["windows_csv"])
        self.assertEqual(out1["would_add_plan_ids"], out2["would_add_plan_ids"])

    def test_idempotent_enqueue(self):
        out1 = run_plan_generation(self._args(dry_run=False), repo=self.tmpdir)
        out2 = run_plan_generation(self._args(dry_run=False), repo=self.tmpdir)
        self.assertEqual(out1["added_count"], 5)
        self.assertEqual(out2["added_count"], 0)
        self.assertEqual(out2["skipped_existing_count"], 5)
        self.assertEqual(out2["skipped_done_count"], 0)

    def test_max_windows_per_pair_cap(self):
        out = run_plan_generation(self._args(dry_run=True, max_windows_per_pair=1), repo=self.tmpdir)
        self.assertEqual(out["pairs_considered"], 4)
        self.assertEqual(out["windows_total"], 4)
        self.assertEqual(out["skipped_no_coverage"], 1)
        self.assertEqual(out["would_add_count"], 3)


if __name__ == "__main__":
    unittest.main()
