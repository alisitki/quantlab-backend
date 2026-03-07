import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tools.phase5_nightly_orchestrator_v0 import parse_args, run_orchestrator


class NightlyOrchestratorV0Tests(unittest.TestCase):
    def test_cli_defaults_to_all_dates(self):
        args = parse_args([])
        self.assertTrue(args.all_dates)

    def _args(self, state_dir: Path, **overrides):
        base = {
            "exchanges": "binance,bybit,okx",
            "streams": "trade,bbo",
            "window_days": 1,
            "all_dates": True,
            "max_symbols": 20,
            "per_run_timeout_min": 12,
            "session_wall_budget_min": 360.0,
            "max_jobs": 20,
            "stop_after_failures": 1,
            "sleep_between_jobs_sec": 20.0,
            "sleep_jitter_sec": 10.0,
            "failure_backoff_sec": 120.0,
            "stale_running_min": 180.0,
            "active_window_start": "20:00",
            "active_window_end": "07:00",
            "active_window_tz": "Europe/Istanbul",
            "ignore_active_window": False,
            "state_dir": str(state_dir),
            "inventory_state_json": "/tmp/compacted__state.json",
            "inventory_bucket": "quantlab-compact",
            "inventory_key": "compacted/_state.json",
            "inventory_s3_tool": "/tmp/s3_compact_tool.py",
            "lane_policy": "tools/phase5_state/lane_policy_v0.json",
            "inventory_require_quality_pass": True,
            "dry_run": False,
            "now_utc": "2026-03-06T12:00:00Z",
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_outside_active_window_noops(self):
        with tempfile.TemporaryDirectory(prefix="nightly_orch_noop_") as td:
            state_dir = Path(td)
            exit_code, report, report_path = run_orchestrator(self._args(state_dir), repo=Path(td))
            self.assertEqual(exit_code, 0)
            self.assertEqual(report["status"], "NOOP_OUTSIDE_ACTIVE_WINDOW")
            self.assertTrue(report_path.exists())

    def test_planner_scheduler_candidate_flow(self):
        with tempfile.TemporaryDirectory(prefix="nightly_orch_ok_") as td:
            root = Path(td)
            state_dir = root / "phase5_state"
            (root / "tools" / "phase6_state").mkdir(parents=True, exist_ok=True)
            (root / "tools" / "phase6_state" / "candidate_index.json").write_text(
                json.dumps({"record_count": 2, "by_tier": {"PROMOTE": 2, "PROMOTE_STRONG": 0}}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            (root / "tools" / "phase6_state" / "candidate_review.json").write_text(
                json.dumps(
                    {
                        "record_count": 2,
                        "top_candidates": [{"pack_id": "pack-1", "score": "55.000000"}],
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            calls = []

            def runner(cmd, cwd):
                calls.append(list(cmd))
                text = " ".join(cmd)
                if "phase5_big_hunt_plan_v2.py" in text:
                    return {"exit_code": 0, "stdout": "added_count=3\nskipped_existing_count=2\nskipped_done_count=1\nwindows_total=6\n", "stderr": "", "kv": {"added_count": "3", "skipped_existing_count": "2", "skipped_done_count": "1", "windows_total": "6"}}
                if "phase5_big_hunt_scheduler_v1.py" in text:
                    return {"exit_code": 0, "stdout": "jobs_processed=2\ndone_count=2\nfailed_count=0\npromote_new_count=1\nbatch_report_path=/tmp/batch.json\n", "stderr": "", "kv": {"jobs_processed": "2", "done_count": "2", "failed_count": "0", "promote_new_count": "1", "batch_report_path": "/tmp/batch.json"}}
                if "phase6_candidate_export_v0.py" in text:
                    return {"exit_code": 0, "stdout": "candidate_count_total=2\nstrong_count=0\n", "stderr": "", "kv": {"candidate_count_total": "2", "strong_count": "0"}}
                if "phase6_candidate_review_v0.py" in text:
                    return {"exit_code": 0, "stdout": "review_count=2\ntop_pack_id=pack-1\ntop_score=55.000000\n", "stderr": "", "kv": {"review_count": "2", "top_pack_id": "pack-1", "top_score": "55.000000"}}
                raise AssertionError(text)

            args = self._args(state_dir, ignore_active_window=True)
            exit_code, report, report_path = run_orchestrator(args, repo=root, runner=runner)
            self.assertEqual(exit_code, 0)
            self.assertEqual(report["status"], "OK")
            self.assertEqual(report["planner"]["added_count"], 3)
            self.assertEqual(report["scheduler"]["done_count"], 2)
            self.assertEqual(report["candidate"]["top_pack_id"], "pack-1")
            self.assertTrue(report_path.exists())
            self.assertEqual(len(calls), 4)

    def test_phase6_v2_auto_apply_runs_before_candidate_refresh(self):
        with tempfile.TemporaryDirectory(prefix="nightly_orch_v2_") as td:
            root = Path(td)
            state_dir = root / "phase5_state"
            (root / "tools" / "phase6_state").mkdir(parents=True, exist_ok=True)
            (root / "tools" / "phase6_state" / "candidate_index.json").write_text(
                json.dumps({"record_count": 1, "by_tier": {"PROMOTE": 1, "PROMOTE_STRONG": 0}}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            (root / "tools" / "phase6_state" / "candidate_review.json").write_text(
                json.dumps({"record_count": 1, "top_candidates": [{"pack_id": "pack-z", "score": "51.0"}]}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            batch_report_path = state_dir / "bighunt_batch_report_20260307_120000.json"
            batch_report_path.parent.mkdir(parents=True, exist_ok=True)
            batch_report_path.write_text(
                json.dumps(
                    {
                        "processed": [
                            {
                                "plan_id": "p1",
                                "final_status": "DONE",
                                "archive_dir": str(root / "archive" / "pack_new"),
                                "decision": "PROMOTE",
                            }
                        ]
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            calls = []

            def runner(cmd, cwd):
                calls.append(list(cmd))
                text = " ".join(cmd)
                if "phase5_big_hunt_plan_v2.py" in text:
                    return {"exit_code": 0, "stdout": "added_count=0\nskipped_existing_count=1\nskipped_done_count=0\nwindows_total=1\n", "stderr": "", "kv": {"added_count": "0", "skipped_existing_count": "1", "skipped_done_count": "0", "windows_total": "1"}}
                if "phase5_big_hunt_scheduler_v1.py" in text:
                    return {"exit_code": 0, "stdout": f"jobs_processed=1\ndone_count=1\nfailed_count=0\npromote_new_count=1\nbatch_report_path={batch_report_path}\n", "stderr": "", "kv": {"jobs_processed": "1", "done_count": "1", "failed_count": "0", "promote_new_count": "1", "batch_report_path": str(batch_report_path)}}
                if "phase6_promotion_guards_v2.py" in text:
                    return {"exit_code": 0, "stdout": "decision=PROMOTE_STRONG\nrecord_appended=true\n", "stderr": "", "kv": {"decision": "PROMOTE_STRONG", "record_appended": "true"}}
                if "phase6_candidate_export_v0.py" in text:
                    return {"exit_code": 0, "stdout": "candidate_count_total=2\nstrong_count=1\n", "stderr": "", "kv": {"candidate_count_total": "2", "strong_count": "1"}}
                if "phase6_candidate_review_v0.py" in text:
                    return {"exit_code": 0, "stdout": "review_count=2\ntop_pack_id=pack-new\ntop_score=61.0\n", "stderr": "", "kv": {"review_count": "2", "top_pack_id": "pack-new", "top_score": "61.0"}}
                raise AssertionError(text)

            args = self._args(state_dir, ignore_active_window=True)
            exit_code, report, _report_path = run_orchestrator(args, repo=root, runner=runner)
            self.assertEqual(exit_code, 0)
            self.assertEqual(report["phase6_v2"]["pack_count"], 1)
            self.assertEqual(report["phase6_v2"]["record_appended_count"], 1)
            call_text = [" ".join(cmd) for cmd in calls]
            self.assertLess(call_text.index(next(x for x in call_text if "phase6_promotion_guards_v2.py" in x)), call_text.index(next(x for x in call_text if "phase6_candidate_export_v0.py" in x)))

    def test_dry_run_skips_scheduler_but_refreshes_candidate(self):
        with tempfile.TemporaryDirectory(prefix="nightly_orch_dry_") as td:
            root = Path(td)
            state_dir = root / "phase5_state"
            (root / "tools" / "phase6_state").mkdir(parents=True, exist_ok=True)
            (root / "tools" / "phase6_state" / "candidate_index.json").write_text(
                json.dumps({"record_count": 1, "by_tier": {"PROMOTE": 1, "PROMOTE_STRONG": 0}}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            (root / "tools" / "phase6_state" / "candidate_review.json").write_text(
                json.dumps({"record_count": 1, "top_candidates": [{"pack_id": "pack-a", "score": "42.0"}]}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

            calls = []

            def runner(cmd, cwd):
                calls.append(list(cmd))
                text = " ".join(cmd)
                if "phase5_big_hunt_plan_v2.py" in text:
                    return {"exit_code": 0, "stdout": "would_add_count=4\nwindows_total=4\n", "stderr": "", "kv": {"would_add_count": "4", "windows_total": "4"}}
                if "phase6_candidate_export_v0.py" in text:
                    return {"exit_code": 0, "stdout": "candidate_count_total=1\nstrong_count=0\n", "stderr": "", "kv": {"candidate_count_total": "1", "strong_count": "0"}}
                if "phase6_candidate_review_v0.py" in text:
                    return {"exit_code": 0, "stdout": "review_count=1\ntop_pack_id=pack-a\ntop_score=42.0\n", "stderr": "", "kv": {"review_count": "1", "top_pack_id": "pack-a", "top_score": "42.0"}}
                raise AssertionError(text)

            args = self._args(state_dir, ignore_active_window=True, dry_run=True)
            exit_code, report, _report_path = run_orchestrator(args, repo=root, runner=runner)
            self.assertEqual(exit_code, 0)
            self.assertEqual(report["status"], "OK")
            self.assertTrue(report["scheduler"]["dry_run"])
            self.assertEqual(report["planner"]["added_count"], 4)
            self.assertEqual(len(calls), 3)

    def test_planner_failure_stops_before_scheduler(self):
        with tempfile.TemporaryDirectory(prefix="nightly_orch_fail_") as td:
            root = Path(td)
            state_dir = root / "phase5_state"

            calls = []

            def runner(cmd, cwd):
                calls.append(list(cmd))
                return {"exit_code": 2, "stdout": "", "stderr": "planner failed", "kv": {}}

            args = self._args(state_dir, ignore_active_window=True)
            exit_code, report, report_path = run_orchestrator(args, repo=root, runner=runner)
            self.assertEqual(exit_code, 2)
            self.assertEqual(report["status"], "FAIL_PLANNER")
            self.assertTrue(report_path.exists())
            self.assertEqual(len(calls), 1)

    def test_report_keys_are_deterministic(self):
        with tempfile.TemporaryDirectory(prefix="nightly_orch_keys_") as td:
            root = Path(td)
            state_dir = root / "phase5_state"
            (root / "tools" / "phase6_state").mkdir(parents=True, exist_ok=True)
            (root / "tools" / "phase6_state" / "candidate_index.json").write_text("{}\n", encoding="utf-8")
            (root / "tools" / "phase6_state" / "candidate_review.json").write_text("{}\n", encoding="utf-8")

            def runner(cmd, cwd):
                text = " ".join(cmd)
                if "phase5_big_hunt_plan_v2.py" in text:
                    return {"exit_code": 0, "stdout": "added_count=0\nskipped_existing_count=0\nskipped_done_count=0\nwindows_total=0\n", "stderr": "", "kv": {"added_count": "0", "skipped_existing_count": "0", "skipped_done_count": "0", "windows_total": "0"}}
                if "phase5_big_hunt_scheduler_v1.py" in text:
                    return {"exit_code": 0, "stdout": "jobs_processed=0\ndone_count=0\nfailed_count=0\npromote_new_count=0\nbatch_report_path=\n", "stderr": "", "kv": {"jobs_processed": "0", "done_count": "0", "failed_count": "0", "promote_new_count": "0", "batch_report_path": ""}}
                if "phase6_candidate_export_v0.py" in text:
                    return {"exit_code": 0, "stdout": "candidate_count_total=0\nstrong_count=0\n", "stderr": "", "kv": {"candidate_count_total": "0", "strong_count": "0"}}
                if "phase6_candidate_review_v0.py" in text:
                    return {"exit_code": 0, "stdout": "review_count=0\ntop_pack_id=\ntop_score=\n", "stderr": "", "kv": {"review_count": "0", "top_pack_id": "", "top_score": ""}}
                raise AssertionError(text)

            args = self._args(state_dir, ignore_active_window=True)
            _exit_code, report, report_path = run_orchestrator(args, repo=root, runner=runner)
            loaded = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(sorted(loaded.keys()), sorted(report.keys()))
            self.assertEqual(sorted(loaded["planner"].keys())[:4], ["added_count", "exit_code", "skipped_done_count", "skipped_existing_count"])


if __name__ == "__main__":
    unittest.main()
