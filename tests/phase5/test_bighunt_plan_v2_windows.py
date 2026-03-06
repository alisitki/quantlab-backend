import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tools.phase5_big_hunt_plan_v2 import (
    build_plan_record,
    discover_windows,
    enqueue_windows_from_inventory,
    run_plan_generation,
)
from tools.phase5_bighunt_state_v1 import (
    append_queue_record,
    canonical_plan_id,
    ensure_state_files,
    load_queue_records,
)
from tools.phase5_state_selection_v1 import load_inventory


def write_inventory(path: Path, partitions: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {"partitions": partitions}
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class BigHuntPlanV2WindowsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="bighunt_plan_v2_"))
        self.state_dir = self.tmpdir / "phase5_state"
        self.inventory_json = self.tmpdir / "inventory_state.json"
        write_inventory(
            self.inventory_json,
            {
                "binance/trade/btcusdt/20260124": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260125": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260126": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260127": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260128": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260129": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260130": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/ethusdt/20260129": {"status": "success", "day_quality_post": "DEGRADED"},
                "binance/trade/ethusdt/20260130": {"status": "success", "day_quality_post": "DEGRADED"},
                "binance/trade/solusdt/20260130": {"status": "fail", "day_quality_post": "GOOD"},
                "binance/trade/xrpusdt/20260128": {"status": "success", "day_quality_post": "BAD"},
            },
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_window_discovery_is_deterministic(self):
        rows = load_inventory(self.inventory_json, bucket="quantlab-compact")
        w1 = discover_windows(
            rows=rows,
            exchange="binance",
            stream="trade",
            window_days=2,
            lookback_days=4,
            max_windows=50,
            require_quality_pass=True,
        )
        w2 = discover_windows(
            rows=rows,
            exchange="binance",
            stream="trade",
            window_days=2,
            lookback_days=4,
            max_windows=50,
            require_quality_pass=True,
        )
        self.assertEqual(w1, w2)
        self.assertEqual(
            w1,
            [("20260127", "20260128"), ("20260128", "20260129"), ("20260129", "20260130")],
        )

    def test_done_window_skipped_and_idempotent(self):
        queue_path, _index_path = ensure_state_files(self.state_dir)
        plan_id_done = canonical_plan_id(
            exchange="binance",
            stream="trade",
            start="20260127",
            end="20260128",
            object_keys_tsv="state_selection/object_keys_selected.tsv",
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
        )
        done_rec = build_plan_record(
            plan_id=plan_id_done,
            exchange="binance",
            stream="trade",
            start="20260127",
            end="20260128",
            object_keys_tsv_ref="state_selection/object_keys_selected.tsv",
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
        )
        done_rec["status"] = "DONE"
        done_rec["tries"] = 1
        append_queue_record(queue_path, done_rec)

        out1 = enqueue_windows_from_inventory(
            state_dir=self.state_dir,
            object_keys_tsv_ref="state_selection/object_keys_selected.tsv",
            exchange="binance",
            stream="trade",
            windows=[("20260127", "20260128"), ("20260128", "20260129")],
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            dry_run=False,
        )
        self.assertEqual(out1["skipped_done_count"], 1)
        self.assertEqual(out1["added_count"], 1)

        out2 = enqueue_windows_from_inventory(
            state_dir=self.state_dir,
            object_keys_tsv_ref="state_selection/object_keys_selected.tsv",
            exchange="binance",
            stream="trade",
            windows=[("20260127", "20260128"), ("20260128", "20260129")],
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            dry_run=False,
        )
        self.assertEqual(out2["added_count"], 0)
        self.assertEqual(out2["skipped_done_count"], 1)
        self.assertEqual(out2["skipped_existing_count"], 1)
        self.assertEqual(len(load_queue_records(queue_path)), 2)

    def test_caps_respected_in_dry_run(self):
        args = SimpleNamespace(
            exchange="binance",
            stream="trade",
            exchanges="",
            streams="",
            window_days=2,
            lookback_days=3,
            all_dates=False,
            max_windows=1,
            max_windows_per_pair=0,
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
            dry_run=True,
        )
        out = run_plan_generation(args, repo=self.tmpdir)
        self.assertTrue(out["dry_run"])
        self.assertEqual(out["windows_considered"], 1)
        self.assertEqual(out["windows_csv"], "20260129..20260130")
        self.assertEqual(out["added_count"], 0)
        self.assertEqual(out["would_add_count"], 1)


if __name__ == "__main__":
    unittest.main()
