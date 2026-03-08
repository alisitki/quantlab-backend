import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-long-shadow-launch-v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, sort_keys=True) for row in rows]
    path.write_text(("\n".join(lines) + "\n") if lines else "", encoding="utf-8")


def write_fake_batch_tool(path: Path, config_path: Path) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            "config = json.loads(Path(%r).read_text(encoding='utf-8'))\n"
            "args = sys.argv[1:]\n"
            "result_json = Path(args[args.index('--result-json') + 1])\n"
            "refresh_json = Path(args[args.index('--refresh-result-json') + 1])\n"
            "dry_run = '--dry-run' in args\n"
            "summary_path = Path(config['summary_path'])\n"
            "stdout_path = Path(config['stdout_path'])\n"
            "stderr_path = Path(config['stderr_path'])\n"
            "audit_dir = Path(config['audit_dir'])\n"
            "summary_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "stdout_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "stderr_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "audit_dir.mkdir(parents=True, exist_ok=True)\n"
            "stdout_path.write_text(config.get('item_stdout', ''), encoding='utf-8')\n"
            "stderr_path.write_text(config.get('item_stderr', ''), encoding='utf-8')\n"
            "if config.get('write_summary', not dry_run):\n"
            "    summary_path.write_text(json.dumps(config['summary_payload']) + '\\n', encoding='utf-8')\n"
            "refresh_payload = config['refresh_payload'] if not dry_run else config.get('refresh_payload_dry_run', {'schema_version': 'shadow_derived_surface_refresh_v0', 'sync_ok': False, 'failed_step': '', 'steps': []})\n"
            "refresh_json.parent.mkdir(parents=True, exist_ok=True)\n"
            "refresh_json.write_text(json.dumps(refresh_payload) + '\\n', encoding='utf-8')\n"
            "batch_payload = config['batch_payload'] if not dry_run else config['batch_payload_dry_run']\n"
            "batch_payload['results'][0]['summary_json_path'] = str(summary_path)\n"
            "batch_payload['results'][0]['stdout_log_path'] = str(stdout_path)\n"
            "batch_payload['results'][0]['stderr_log_path'] = str(stderr_path)\n"
            "batch_payload['results'][0]['audit_spool_dir'] = str(audit_dir)\n"
            "result_json.parent.mkdir(parents=True, exist_ok=True)\n"
            "result_json.write_text(json.dumps(batch_payload) + '\\n', encoding='utf-8')\n"
            "print('fake_batch_done')\n"
            "raise SystemExit(int(config.get('exit_code', 0)))\n"
        )
        % str(config_path),
        encoding="utf-8",
    )
    path.chmod(0o755)


def batch_payload(pack_id: str, *, dry_run: bool = False) -> dict:
    return {
        "schema_version": "shadow_observation_batch_result_v0",
        "generated_ts_utc": "2026-03-08T12:00:00Z",
        "watchlist_path": "/tmp/watchlist.json",
        "strategy": "core/strategy/strategies/PrintHeadTailStrategy.js",
        "max_items": 1,
        "attempted_count": 1,
        "completed_count": 0 if dry_run else 1,
        "dry_run": dry_run,
        "refresh_executed": False if dry_run else True,
        "refresh_exit_code": "not_run" if dry_run else 0,
        "refresh_result_json_path": "/tmp/refresh.json",
        "surfaces_synced": False if dry_run else True,
        "refresh_command": "python3 tools/refresh-shadow-derived-surfaces-v0.py",
        "refresh_note": "dry_run" if dry_run else "",
        "execution_ledger_rebuild_executed": False if dry_run else True,
        "execution_ledger_rebuild_exit_code": "not_run" if dry_run else 0,
        "execution_ledger_path": "/tmp/execution-ledger.jsonl",
        "execution_ledger_rebuild_command": "",
        "execution_pack_summary_rebuild_executed": False if dry_run else True,
        "execution_pack_summary_rebuild_exit_code": "not_run" if dry_run else 0,
        "execution_pack_summary_path": "/tmp/execution-pack-summary.json",
        "execution_pack_summary_rebuild_command": "",
        "execution_artifacts_synced": False if dry_run else True,
        "execution_rebuild_note": "no_history_updates_for_execution_rebuild" if dry_run else "",
        "results": [
            {
                "rank": 1,
                "pack_id": pack_id,
                "exchange": "bybit",
                "symbols": ["BTCUSDT"],
                "run_executed": False if dry_run else True,
                "run_exit_code": "not_run" if dry_run else 0,
                "verify_soft_live_pass": "unknown" if dry_run else True,
                "summary_generated": False if dry_run else True,
                "summary_json_path": "",
                "history_updated": False if dry_run else True,
                "stdout_log_path": "",
                "stderr_log_path": "",
                "audit_spool_dir": "",
                "wrapper_command": "node fake_wrapper.js",
                "summary_command": "python3 fake_summary.py",
                "history_command": "python3 fake_history.py",
                "verify_command": "node fake_verify.js",
                "note": "dry_run" if dry_run else "",
            }
        ],
    }


def refresh_payload() -> dict:
    return {
        "schema_version": "shadow_derived_surface_refresh_v0",
        "generated_ts_utc": "2026-03-08T12:00:01Z",
        "sync_ok": True,
        "failed_step": "",
        "steps": [
            {"name": "candidate_review", "status": "OK", "exit_code": 0, "command": "candidate_review", "output_path": "/tmp/candidate_review.json"},
            {"name": "watchlist", "status": "OK", "exit_code": 0, "command": "watchlist", "output_path": "/tmp/watchlist.json"},
            {"name": "execution_ledger", "status": "OK", "exit_code": 0, "command": "execution_ledger", "output_path": "/tmp/execution-ledger.jsonl"},
            {"name": "execution_events", "status": "OK", "exit_code": 0, "command": "execution_events", "output_path": "/tmp/execution-events.jsonl"},
            {"name": "trade_ledger", "status": "OK", "exit_code": 0, "command": "trade_ledger", "output_path": "/tmp/trade-ledger.jsonl"},
            {"name": "execution_pack_summary", "status": "OK", "exit_code": 0, "command": "execution_pack_summary", "output_path": "/tmp/execution-pack-summary.json"},
            {"name": "execution_rollup_snapshot", "status": "OK", "exit_code": 0, "command": "execution_rollup_snapshot", "output_path": "/tmp/execution-rollup.json"},
            {"name": "execution_outcome_review", "status": "OK", "exit_code": 0, "command": "execution_outcome_review", "output_path": "/tmp/execution-outcome-review.json"},
            {"name": "operator_snapshot", "status": "OK", "exit_code": 0, "command": "operator_snapshot", "output_path": "/tmp/operator-snapshot.json"},
            {"name": "execution_review_queue", "status": "OK", "exit_code": 0, "command": "execution_review_queue", "output_path": "/tmp/review-queue.json"},
        ],
    }


class RunLongShadowLaunchV0Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def _base_paths(self, root: Path) -> dict[str, Path]:
        return {
            "watchlist": root / "watchlist.json",
            "batch_tool": root / "fake_batch.py",
            "config": root / "config.json",
            "summary": root / "summary.json",
            "item_stdout": root / "item_stdout.log",
            "item_stderr": root / "item_stderr.log",
            "audit_dir": root / "audit",
            "batch_result": root / "batch_result.json",
            "launch_result": root / "launch_result.json",
            "refresh_result": root / "refresh_result.json",
            "operator_snapshot": root / "shadow_operator_snapshot_v0.json",
            "review_queue": root / "shadow_execution_review_queue_v0.json",
            "events": root / "shadow_execution_events_v1.jsonl",
            "trade_ledger": root / "shadow_trade_ledger_v1.jsonl",
            "batch_stdout": root / "batch_stdout.log",
            "batch_stderr": root / "batch_stderr.log",
            "audit_base": root / "audit_base",
            "out_dir": root / "out",
        }

    def _invoke_wrapper(self, paths: dict[str, Path], *, dry_run: bool = False) -> subprocess.CompletedProcess[str]:
        args = [
            "--watchlist",
            str(paths["watchlist"]),
            "--batch-tool",
            str(paths["batch_tool"]),
            "--batch-result-json",
            str(paths["batch_result"]),
            "--launch-result-json",
            str(paths["launch_result"]),
            "--refresh-result-json",
            str(paths["refresh_result"]),
            "--operator-snapshot-json",
            str(paths["operator_snapshot"]),
            "--execution-review-queue-json",
            str(paths["review_queue"]),
            "--execution-events-jsonl",
            str(paths["events"]),
            "--trade-ledger-jsonl",
            str(paths["trade_ledger"]),
            "--batch-stdout-log",
            str(paths["batch_stdout"]),
            "--batch-stderr-log",
            str(paths["batch_stderr"]),
            "--audit-base-dir",
            str(paths["audit_base"]),
            "--out-dir",
            str(paths["out_dir"]),
        ]
        if dry_run:
            args.append("--dry-run")
        return self._run(*args)

    def test_dry_run_classifies_without_valid_run(self):
        with tempfile.TemporaryDirectory(prefix="long_shadow_dry_") as td:
            root = Path(td)
            paths = self._base_paths(root)
            paths["watchlist"].write_text("{}\n", encoding="utf-8")
            config = {
                "exit_code": 0,
                "summary_path": str(paths["summary"]),
                "stdout_path": str(paths["item_stdout"]),
                "stderr_path": str(paths["item_stderr"]),
                "audit_dir": str(paths["audit_dir"]),
                "summary_payload": {
                    "live_run_id": "run_pack_a",
                    "heartbeat_seen": True,
                    "processed_event_count": 7,
                },
                "batch_payload": batch_payload("pack_a", dry_run=False),
                "batch_payload_dry_run": batch_payload("pack_a", dry_run=True),
                "refresh_payload": refresh_payload(),
            }
            write_json(paths["config"], config)
            write_fake_batch_tool(paths["batch_tool"], paths["config"])

            res = self._invoke_wrapper(paths, dry_run=True)

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(paths["launch_result"].read_text(encoding="utf-8"))
            self.assertEqual(payload["launch_status"], "DRY_RUN_ONLY")
            self.assertFalse(payload["valid_run"])
            self.assertEqual(payload["selected_pack_id"], "")

    def test_valid_no_execution_activity_classification(self):
        with tempfile.TemporaryDirectory(prefix="long_shadow_valid_empty_") as td:
            root = Path(td)
            paths = self._base_paths(root)
            paths["watchlist"].write_text("{}\n", encoding="utf-8")
            config = {
                "exit_code": 0,
                "summary_path": str(paths["summary"]),
                "stdout_path": str(paths["item_stdout"]),
                "stderr_path": str(paths["item_stderr"]),
                "audit_dir": str(paths["audit_dir"]),
                "summary_payload": {
                    "live_run_id": "run_pack_a",
                    "heartbeat_seen": True,
                    "processed_event_count": 9,
                },
                "batch_payload": batch_payload("pack_a", dry_run=False),
                "batch_payload_dry_run": batch_payload("pack_a", dry_run=True),
                "refresh_payload": refresh_payload(),
            }
            write_json(paths["config"], config)
            write_fake_batch_tool(paths["batch_tool"], paths["config"])
            paths["operator_snapshot"].write_text("{}\n", encoding="utf-8")
            paths["review_queue"].write_text("{}\n", encoding="utf-8")
            write_jsonl(paths["events"], [])
            write_jsonl(paths["trade_ledger"], [])

            res = self._invoke_wrapper(paths)

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(paths["launch_result"].read_text(encoding="utf-8"))
            self.assertEqual(payload["launch_status"], "VALID_NO_EXECUTION_ACTIVITY")
            self.assertTrue(payload["valid_run"])
            self.assertTrue(payload["required_artifacts_ok"])
            self.assertEqual(payload["matched_execution_event_count"], 0)
            self.assertEqual(payload["matched_trade_count"], 0)
            self.assertEqual(payload["selection_profile"], "WATCHLIST_TOP_1_ONLY")
            self.assertEqual(payload["max_items"], 1)

    def test_valid_with_execution_activity_classification(self):
        with tempfile.TemporaryDirectory(prefix="long_shadow_valid_events_") as td:
            root = Path(td)
            paths = self._base_paths(root)
            paths["watchlist"].write_text("{}\n", encoding="utf-8")
            config = {
                "exit_code": 0,
                "summary_path": str(paths["summary"]),
                "stdout_path": str(paths["item_stdout"]),
                "stderr_path": str(paths["item_stderr"]),
                "audit_dir": str(paths["audit_dir"]),
                "summary_payload": {
                    "live_run_id": "run_pack_a",
                    "heartbeat_seen": True,
                    "processed_event_count": 11,
                },
                "batch_payload": batch_payload("pack_a", dry_run=False),
                "batch_payload_dry_run": batch_payload("pack_a", dry_run=True),
                "refresh_payload": refresh_payload(),
            }
            write_json(paths["config"], config)
            write_fake_batch_tool(paths["batch_tool"], paths["config"])
            paths["operator_snapshot"].write_text("{}\n", encoding="utf-8")
            paths["review_queue"].write_text("{}\n", encoding="utf-8")
            write_jsonl(
                paths["events"],
                [
                    {
                        "schema_version": "shadow_execution_events_v1",
                        "event_id": "pack_a|run_pack_a|event|1",
                        "selected_pack_id": "pack_a",
                        "live_run_id": "run_pack_a",
                    }
                ],
            )
            write_jsonl(
                paths["trade_ledger"],
                [
                    {
                        "schema_version": "shadow_trade_ledger_v1",
                        "trade_id": "pack_a|trade|1",
                        "selected_pack_id": "pack_a",
                        "open_live_run_id": "run_pack_a",
                        "last_live_run_id": "run_pack_a",
                    }
                ],
            )

            res = self._invoke_wrapper(paths)

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(paths["launch_result"].read_text(encoding="utf-8"))
            self.assertEqual(payload["launch_status"], "VALID_WITH_EXECUTION_ACTIVITY")
            self.assertTrue(payload["valid_run"])
            self.assertEqual(payload["matched_execution_event_count"], 1)
            self.assertEqual(payload["matched_trade_count"], 1)

    def test_missing_trade_ledger_is_invalid_and_nonzero(self):
        with tempfile.TemporaryDirectory(prefix="long_shadow_missing_trade_") as td:
            root = Path(td)
            paths = self._base_paths(root)
            paths["watchlist"].write_text("{}\n", encoding="utf-8")
            config = {
                "exit_code": 0,
                "summary_path": str(paths["summary"]),
                "stdout_path": str(paths["item_stdout"]),
                "stderr_path": str(paths["item_stderr"]),
                "audit_dir": str(paths["audit_dir"]),
                "summary_payload": {
                    "live_run_id": "run_pack_a",
                    "heartbeat_seen": True,
                    "processed_event_count": 5,
                },
                "batch_payload": batch_payload("pack_a", dry_run=False),
                "batch_payload_dry_run": batch_payload("pack_a", dry_run=True),
                "refresh_payload": refresh_payload(),
            }
            write_json(paths["config"], config)
            write_fake_batch_tool(paths["batch_tool"], paths["config"])
            paths["operator_snapshot"].write_text("{}\n", encoding="utf-8")
            paths["review_queue"].write_text("{}\n", encoding="utf-8")
            write_jsonl(paths["events"], [])

            res = self._invoke_wrapper(paths)

            self.assertEqual(res.returncode, 1)
            payload = json.loads(paths["launch_result"].read_text(encoding="utf-8"))
            self.assertEqual(payload["launch_status"], "INVALID")
            self.assertFalse(payload["valid_run"])
            self.assertEqual(payload["invalid_reason"], "trade_ledger_missing")
            self.assertFalse(payload["required_artifacts_ok"])
