import csv
import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from tools.phase5_bighunt_state_v1 import append_queue_record, load_queue_records, rebuild_index, write_index
from tools.phase5_big_hunt_plan_v1 import generate_plans
from tools.phase5_big_hunt_scheduler_v1 import parse_args as parse_scheduler_args
from tools.phase5_big_hunt_scheduler_v1 import run_scheduler


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
            "binance/trade/adausdt/20260126": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/adausdt/20260127": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/adausdt/20260128": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/btcusdt/20260126": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/btcusdt/20260127": {"status": "success", "day_quality_post": "GOOD"},
            "binance/trade/btcusdt/20260128": {"status": "success", "day_quality_post": "GOOD"},
        }
    }
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class BigHuntSchedulerBatchModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="bighunt_batch_v1_"))
        self.state_dir = self.tmpdir / "phase5_state"
        self.object_keys = self.tmpdir / "object_keys_selected.tsv"
        self.inventory_state = self.tmpdir / "inventory_state.json"
        make_object_keys_tsv(self.object_keys)
        make_inventory_state_json(self.inventory_state)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _enqueue_windows(self, windows):
        out = generate_plans(
            object_keys_tsv=self.object_keys,
            exchange="binance",
            stream="trade",
            windows=windows,
            max_symbols=20,
            per_run_timeout_min=12,
            max_wall_min=120,
            category="FULLSCAN_MAJOR",
            state_dir=self.state_dir,
        )
        return out

    def test_quota_stops_after_budget(self):
        self._enqueue_windows([("20260126", "20260127"), ("20260127", "20260128")])
        args = parse_scheduler_args(
            [
                "--dry-run",
                "--max-jobs",
                "5",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--session-wall-budget-min",
                "120",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = run_scheduler(args, repo=self.tmpdir)
        txt = out.getvalue()
        self.assertEqual(rc, 2)
        self.assertIn("jobs_processed=1", txt)
        self.assertIn("stop_reason=SESSION_WALL_BUDGET_EXCEEDED", txt)

    def test_backoff_called_between_jobs(self):
        self._enqueue_windows([("20260126", "20260127"), ("20260127", "20260128")])
        args = parse_scheduler_args(
            [
                "--max-jobs",
                "2",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--sleep-between-jobs-sec",
                "5",
                "--sleep-jitter-sec",
                "0",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )
        sleeps = []

        def fake_sleep(sec):
            sleeps.append(sec)

        def fake_run(cmd, cwd, capture_output, text):  # noqa: ANN001
            run_id = cmd[cmd.index("--run-id") + 1]
            archive_dir = self.tmpdir / "archive" / run_id
            archive_dir.mkdir(parents=True, exist_ok=True)
            report = {
                "status": "PASS",
                "archive_dir": str(archive_dir),
                "post_eval": {"decision": "PROMOTE", "record_appended": "true"},
            }
            (archive_dir / "campaign_report.json").write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            stdout = "\n".join([f"RUN_ID={run_id}", f"ARCHIVE_DIR={archive_dir}", "decision=PROMOTE", "record_appended=true"])
            return CompletedProcess(cmd, 0, stdout=stdout, stderr="")

        with patch("tools.phase5_big_hunt_scheduler_v1.subprocess.run", side_effect=fake_run):
            rc = run_scheduler(args, repo=self.tmpdir, sleep_fn=fake_sleep)
        self.assertEqual(rc, 0)
        self.assertEqual(sleeps, [5.0])

    def test_stop_after_failures_works(self):
        self._enqueue_windows([("20260126", "20260127"), ("20260127", "20260128")])
        args = parse_scheduler_args(
            [
                "--max-jobs",
                "2",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--stop-after-failures",
                "1",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )

        def fail_run(cmd, cwd, capture_output, text):  # noqa: ANN001
            return CompletedProcess(cmd, 1, stdout="", stderr="simulated fail")

        out = io.StringIO()
        with patch("tools.phase5_big_hunt_scheduler_v1.subprocess.run", side_effect=fail_run):
            with redirect_stdout(out):
                rc = run_scheduler(args, repo=self.tmpdir, sleep_fn=lambda _: None)
        txt = out.getvalue()
        self.assertEqual(rc, 2)
        self.assertIn("jobs_processed=1", txt)
        self.assertIn("stop_reason=STOP_AFTER_FAILURES", txt)

    def test_stale_running_reclaimed(self):
        self._enqueue_windows([("20260126", "20260127")])
        queue_path = self.state_dir / "bighunt_queue.jsonl"
        index_path = self.state_dir / "bighunt_index.json"
        records = load_queue_records(queue_path)
        self.assertEqual(len(records), 1)
        stale = dict(records[0])
        stale["status"] = "RUNNING"
        stale["tries"] = 0
        stale["updated_ts_utc"] = "2026-01-01T00:00:00Z"
        append_queue_record(queue_path, stale)
        records.append(stale)
        write_index(index_path, rebuild_index(records, max_tries=2))

        args = parse_scheduler_args(
            [
                "--dry-run",
                "--max-jobs",
                "1",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--stale-running-min",
                "180",
                "--now-utc",
                "2026-01-02T00:00:00Z",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = run_scheduler(args, repo=self.tmpdir)
        txt = out.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("selection_reason=RUNNING_STALE_RECLAIM", txt)

    def test_selector_drops_symbol_missing_required_day(self):
        # adausdt is missing 20260128 in this synthetic inventory, so it must be excluded.
        obj = {
            "partitions": {
                "binance/trade/adausdt/20260127": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260127": {"status": "success", "day_quality_post": "GOOD"},
                "binance/trade/btcusdt/20260128": {"status": "success", "day_quality_post": "GOOD"},
            }
        }
        self.inventory_state.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self._enqueue_windows([("20260127", "20260128")])
        args = parse_scheduler_args(
            [
                "--dry-run",
                "--max-jobs",
                "1",
                "--state-dir",
                str(self.state_dir),
                "--ignore-active-window",
                "--inventory-state-json",
                str(self.inventory_state),
            ]
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = run_scheduler(args, repo=self.tmpdir)
        txt = out.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("selected_symbols_csv=btcusdt", txt)
        self.assertNotIn("selected_symbols_csv=adausdt", txt)


if __name__ == "__main__":
    unittest.main()
