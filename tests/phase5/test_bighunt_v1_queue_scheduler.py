import csv
import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from tools.phase5_bighunt_state_v1 import canonical_plan_id, load_queue_records, rebuild_index
from tools.phase5_big_hunt_plan_v1 import generate_plans
from tools.phase5_big_hunt_scheduler_v1 import parse_args as parse_scheduler_args
from tools.phase5_big_hunt_scheduler_v1 import pick_next_plan, run_scheduler


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
            "binance/trade/btcusdt/20260128": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/ethusdt/20260127": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/ethusdt/20260128": {"status": "success", "day_quality_post": "GOOD"},
        }
    }
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class BigHuntV1QueueSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="bighunt_v1_test_"))
        self.state_dir = self.tmpdir / "phase5_state"
        self.object_keys = self.tmpdir / "object_keys_selected.tsv"
        self.inventory_state = self.tmpdir / "inventory_state.json"
        make_object_keys_tsv(self.object_keys)
        make_inventory_state_json(self.inventory_state)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _enqueue_two_windows(self) -> None:
        out = generate_plans(
            object_keys_tsv=self.object_keys,
            exchange="binance",
            stream="trade",
            windows=[("20260126", "20260127"), ("20260127", "20260128")],
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            state_dir=self.state_dir,
        )
        self.assertEqual(out["added_count"], 2)

    def _enqueue_one_window(self) -> None:
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

    def test_plan_id_deterministic(self):
        a = canonical_plan_id(
            exchange="binance",
            stream="trade",
            start="20260126",
            end="20260127",
            object_keys_tsv="/tmp/a/object_keys_selected.tsv",
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
        )
        b = canonical_plan_id(
            exchange="binance",
            stream="trade",
            start="20260126",
            end="20260127",
            object_keys_tsv="/other/path/object_keys_selected.tsv",
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
        )
        self.assertEqual(a, b)

    def test_plan_append_idempotent(self):
        out1 = generate_plans(
            object_keys_tsv=self.object_keys,
            exchange="binance",
            stream="trade",
            windows=[("20260126", "20260127"), ("20260127", "20260128")],
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            state_dir=self.state_dir,
        )
        out2 = generate_plans(
            object_keys_tsv=self.object_keys,
            exchange="binance",
            stream="trade",
            windows=[("20260126", "20260127"), ("20260127", "20260128")],
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            state_dir=self.state_dir,
        )
        self.assertEqual(out1["added_count"], 2)
        self.assertEqual(out2["added_count"], 0)
        queue_path = self.state_dir / "bighunt_queue.jsonl"
        self.assertEqual(len(load_queue_records(queue_path)), 2)

    def test_index_rebuild_latest_wins(self):
        pid = "plan-a"
        records = [
            {"plan_id": pid, "status": "PENDING", "tries": 0},
            {"plan_id": pid, "status": "RUNNING", "tries": 1},
            {"plan_id": pid, "status": "FAILED", "tries": 1},
            {"plan_id": "plan-b", "status": "DONE", "tries": 1},
        ]
        idx = rebuild_index(records, max_tries=2)
        self.assertEqual(idx["plan_latest"][pid]["status"], "FAILED")
        self.assertEqual(idx["by_status"]["FAILED"], 1)
        self.assertEqual(idx["by_status"]["DONE"], 1)
        self.assertIn(pid, idx["retryable_failed_plan_ids"])

    def test_scheduler_dry_run_selection(self):
        self._enqueue_two_windows()
        args = parse_scheduler_args(
            [
                "--max-jobs",
                "1",
                "--max-tries",
                "2",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--dry-run",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = run_scheduler(args, repo=self.tmpdir)
        self.assertEqual(rc, 0)
        txt = out.getvalue()
        self.assertIn("selected_plan_id=", txt)
        self.assertIn("jobs_processed=1", txt)
        queue_path = self.state_dir / "bighunt_queue.jsonl"
        self.assertEqual(len(load_queue_records(queue_path)), 2)

    def test_scheduler_running_to_done_with_mocked_subprocess(self):
        self._enqueue_two_windows()
        args = parse_scheduler_args(
            [
                "--max-jobs",
                "1",
                "--max-tries",
                "2",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )
        repo = self.tmpdir

        def fake_run(cmd, cwd, capture_output, text):  # noqa: ANN001
            run_id = cmd[cmd.index("--run-id") + 1]
            archive_dir = repo / "archive" / run_id
            archive_dir.mkdir(parents=True, exist_ok=True)
            report = {
                "status": "PASS",
                "archive_dir": str(archive_dir),
                "post_eval": {
                    "decision": "PROMOTE",
                    "record_appended": "true",
                    "state_diff": {"records_nonempty": "1->2", "index_record_count": "1->2"},
                },
            }
            (archive_dir / "campaign_report.json").write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            stdout = "\n".join(
                [
                    f"RUN_ID={run_id}",
                    f"ARCHIVE_DIR={archive_dir}",
                    "record_appended=true",
                    "decision=PROMOTE",
                ]
            )
            return CompletedProcess(cmd, 0, stdout=stdout, stderr="")

        out = io.StringIO()
        with patch("tools.phase5_big_hunt_scheduler_v1.subprocess.run", side_effect=fake_run):
            with redirect_stdout(out):
                rc = run_scheduler(args, repo=repo)
        self.assertEqual(rc, 0)
        txt = out.getvalue()
        self.assertIn("status=DONE", txt)
        self.assertIn("jobs_processed=1", txt)

        records = load_queue_records(self.state_dir / "bighunt_queue.jsonl")
        idx = rebuild_index(records, max_tries=2)
        latest_statuses = [idx["plan_latest"][pid]["status"] for pid in idx["created_order_plan_ids"]]
        self.assertIn("DONE", latest_statuses)

    def test_scheduler_failure_retry_and_max_tries(self):
        self._enqueue_one_window()
        args = parse_scheduler_args(
            [
                "--max-jobs",
                "1",
                "--max-tries",
                "2",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )

        def fail_run(cmd, cwd, capture_output, text):  # noqa: ANN001
            return CompletedProcess(cmd, 1, stdout="", stderr="simulated failure")

        out1 = io.StringIO()
        out2 = io.StringIO()
        out3 = io.StringIO()
        with patch("tools.phase5_big_hunt_scheduler_v1.subprocess.run", side_effect=fail_run):
            with redirect_stdout(out1):
                rc1 = run_scheduler(args, repo=self.tmpdir)
            with redirect_stdout(out2):
                rc2 = run_scheduler(args, repo=self.tmpdir)
            with redirect_stdout(out3):
                rc3 = run_scheduler(args, repo=self.tmpdir)

        self.assertEqual(rc1, 2)
        self.assertEqual(rc2, 2)
        self.assertEqual(rc3, 0)
        self.assertIn("jobs_processed=1", out1.getvalue())
        self.assertIn("jobs_processed=1", out2.getvalue())
        self.assertIn("jobs_processed=0", out3.getvalue())

        records = load_queue_records(self.state_dir / "bighunt_queue.jsonl")
        idx = rebuild_index(records, max_tries=2)
        self.assertEqual(len(idx["created_order_plan_ids"]), 1)
        pid = idx["created_order_plan_ids"][0]
        self.assertEqual(idx["plan_latest"][pid]["status"], "FAILED")
        self.assertEqual(int(idx["plan_latest"][pid]["tries"]), 2)
        self.assertEqual(idx["retryable_failed_plan_ids"], [])

    def test_pick_next_plan_prefers_older_pending_over_newer_failed_retry(self):
        index_obj = {
            "plan_latest": {
                "newer-failed": {
                    "plan_id": "newer-failed",
                    "status": "FAILED",
                    "tries": 1,
                    "exchange": "binance",
                    "stream": "bbo",
                    "start": "20260105",
                    "end": "20260105",
                    "created_ts_utc": "2026-03-06T11:33:14Z",
                },
                "older-pending": {
                    "plan_id": "older-pending",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T12:33:14Z",
                },
            },
            "created_order_plan_ids": ["newer-failed", "older-pending"],
        }
        picked = pick_next_plan(
            index_obj,
            max_tries=2,
            stale_running_min=180.0,
            now_dt=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
        )
        self.assertIsNotNone(picked)
        self.assertEqual(picked[0], "older-pending")
        self.assertEqual(picked[2], "PENDING")

    def test_pick_next_plan_tie_breaks_by_exchange_then_stream_then_reason(self):
        index_obj = {
            "plan_latest": {
                "okx-trade": {
                    "plan_id": "okx-trade",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "okx",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:33:14Z",
                },
                "binance-trade": {
                    "plan_id": "binance-trade",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:34:14Z",
                },
                "binance-bbo": {
                    "plan_id": "binance-bbo",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "bbo",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:35:14Z",
                },
            },
            "created_order_plan_ids": ["okx-trade", "binance-trade", "binance-bbo"],
        }
        picked = pick_next_plan(
            index_obj,
            max_tries=2,
            stale_running_min=180.0,
            now_dt=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
        )
        self.assertIsNotNone(picked)
        self.assertEqual(picked[0], "binance-bbo")

    def test_pick_next_plan_same_lane_prefers_pending_then_failed_then_stale(self):
        index_obj = {
            "plan_latest": {
                "same-lane-stale": {
                    "plan_id": "same-lane-stale",
                    "status": "RUNNING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:33:14Z",
                    "updated_ts_utc": "2026-03-01T00:00:00Z",
                },
                "same-lane-failed": {
                    "plan_id": "same-lane-failed",
                    "status": "FAILED",
                    "tries": 1,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:32:14Z",
                },
                "same-lane-pending": {
                    "plan_id": "same-lane-pending",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:31:14Z",
                },
            },
            "created_order_plan_ids": ["same-lane-stale", "same-lane-failed", "same-lane-pending"],
        }
        picked = pick_next_plan(
            index_obj,
            max_tries=2,
            stale_running_min=180.0,
            now_dt=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
        )
        self.assertIsNotNone(picked)
        self.assertEqual(picked[0], "same-lane-pending")
        self.assertEqual(picked[2], "PENDING")


if __name__ == "__main__":
    unittest.main()
