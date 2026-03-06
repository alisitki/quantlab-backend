import csv
import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from tools.phase5_big_hunt_plan_v1 import generate_plans
from tools.phase5_big_hunt_scheduler_v1 import (
    is_in_active_window,
    parse_args as parse_scheduler_args,
    run_scheduler,
)


def write_tsv(path: Path, header, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def make_object_keys_tsv(path: Path) -> None:
    write_tsv(
        path,
        ["label", "partition_key", "date", "data_key", "meta_key", "bucket"],
        [
            [
                "day1",
                "binance/trade/btcusdt/20260126",
                "20260126",
                "exchange=binance/stream=trade/symbol=btcusdt/date=20260126/data.parquet",
                "",
                "quantlab-compact",
            ],
            [
                "day2",
                "binance/trade/btcusdt/20260127",
                "20260127",
                "exchange=binance/stream=trade/symbol=btcusdt/date=20260127/data.parquet",
                "",
                "quantlab-compact",
            ],
        ],
    )


def make_inventory_state_json(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {
        "partitions": {
            "binance/trade/btcusdt/20260126": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/btcusdt/20260127": {"status": "success", "day_quality_post": "GOOD"},
        }
    }
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class SchedulerActiveWindowGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="sched_active_window_"))
        self.state_dir = self.tmpdir / "phase5_state"
        self.object_keys = self.tmpdir / "object_keys_selected.tsv"
        self.inventory_state = self.tmpdir / "inventory_state.json"
        make_object_keys_tsv(self.object_keys)
        make_inventory_state_json(self.inventory_state)
        out = generate_plans(
            object_keys_tsv=self.object_keys,
            exchange="binance",
            stream="trade",
            windows=[("20260126", "20260127")],
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            state_dir=self.state_dir,
        )
        self.assertEqual(out["added_count"], 1)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_outside_window_noop(self):
        # 17:00Z => 20:00 Europe/Istanbul => outside 21:00..08:00
        args = parse_scheduler_args(
            [
                "--max-jobs",
                "1",
                "--dry-run",
                "--state-dir",
                str(self.state_dir),
                "--inventory-state-json",
                str(self.inventory_state),
                "--now-utc",
                "2026-03-05T17:00:00Z",
                "--active-window-start",
                "21:00",
                "--active-window-end",
                "08:00",
                "--active-window-tz",
                "Europe/Istanbul",
            ]
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = run_scheduler(args, repo=self.tmpdir)
        txt = out.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("jobs_processed=0", txt)
        self.assertIn("stop_reason=NOOP_OUTSIDE_ACTIVE_WINDOW", txt)

    def test_active_window_processes(self):
        # 19:00Z => 22:00 Europe/Istanbul => active
        args = parse_scheduler_args(
            [
                "--max-jobs",
                "1",
                "--dry-run",
                "--state-dir",
                str(self.state_dir),
                "--inventory-state-json",
                str(self.inventory_state),
                "--now-utc",
                "2026-03-05T19:00:00Z",
                "--active-window-start",
                "21:00",
                "--active-window-end",
                "08:00",
                "--active-window-tz",
                "Europe/Istanbul",
            ]
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = run_scheduler(args, repo=self.tmpdir)
        txt = out.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("selected_plan_id=", txt)
        self.assertIn("jobs_processed=1", txt)

    def test_cross_midnight_window_helper(self):
        active_0730, _ = is_in_active_window(
            now_utc=datetime(2026, 3, 5, 4, 30, tzinfo=timezone.utc),
            window_start="21:00",
            window_end="08:00",
            tz_name="Europe/Istanbul",
        )
        active_0900, _ = is_in_active_window(
            now_utc=datetime(2026, 3, 5, 6, 0, tzinfo=timezone.utc),
            window_start="21:00",
            window_end="08:00",
            tz_name="Europe/Istanbul",
        )
        self.assertTrue(active_0730)
        self.assertFalse(active_0900)


if __name__ == "__main__":
    unittest.main()
