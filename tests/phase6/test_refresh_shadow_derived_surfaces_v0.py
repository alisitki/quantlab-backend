import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "refresh-shadow-derived-surfaces-v0.py"


def write_script(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def append_trace_script(path: Path, *, trace: Path, step_name: str, body: str) -> None:
    write_script(
        path,
        (
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            "import sys\n"
            f"trace = Path({trace.as_posix()!r})\n"
            f"step_name = {step_name!r}\n"
            "trace.parent.mkdir(parents=True, exist_ok=True)\n"
            "existing = trace.read_text(encoding='utf-8') if trace.exists() else ''\n"
            "trace.write_text(existing + step_name + '\\n', encoding='utf-8')\n"
            f"{body}"
        ),
    )


def write_candidate_review_script(path: Path, *, trace: Path) -> None:
    append_trace_script(
        path,
        trace=trace,
        step_name="candidate_review",
        body=(
            "args = sys.argv[1:]\n"
            "state_dir = Path(args[args.index('--state-dir') + 1])\n"
            "state_dir.mkdir(parents=True, exist_ok=True)\n"
            "(state_dir / 'candidate_review.json').write_text('{}\\n', encoding='utf-8')\n"
        ),
    )


def write_watchlist_script(path: Path, *, trace: Path) -> None:
    append_trace_script(
        path,
        trace=trace,
        step_name="watchlist",
        body=(
            "args = sys.argv[1:]\n"
            "out_dir = Path(args[args.index('--out-dir') + 1])\n"
            "out_dir.mkdir(parents=True, exist_ok=True)\n"
            "(out_dir / 'shadow_watchlist_v0.json').write_text('{}\\n', encoding='utf-8')\n"
        ),
    )


def write_flag_output_script(path: Path, *, trace: Path, step_name: str, output_flag: str) -> None:
    append_trace_script(
        path,
        trace=trace,
        step_name=step_name,
        body=(
            "args = sys.argv[1:]\n"
            f"out_path = Path(args[args.index({output_flag!r}) + 1])\n"
            "out_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "out_path.write_text('{}\\n', encoding='utf-8')\n"
        ),
    )


def write_failing_script(path: Path, *, trace: Path, step_name: str, exit_code: int) -> None:
    append_trace_script(
        path,
        trace=trace,
        step_name=step_name,
        body=(
            "import sys\n"
            "print('intentional failure', file=sys.stderr)\n"
            f"raise SystemExit({exit_code})\n"
        ),
    )


class RefreshShadowDerivedSurfacesV0Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_dry_run_writes_contract(self):
        with tempfile.TemporaryDirectory(prefix="refresh_shadow_derived_dry_") as td:
            root = Path(td)
            result_json = root / "result.json"
            state_dir = root / "state"
            shadow_dir = root / "shadow"
            state_dir.mkdir(parents=True, exist_ok=True)
            shadow_dir.mkdir(parents=True, exist_ok=True)

            res = self._run(
                "--dry-run",
                "--state-dir",
                str(state_dir),
                "--shadow-state-dir",
                str(shadow_dir),
                "--result-json",
                str(result_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "shadow_derived_surface_refresh_v0")
            self.assertEqual(payload["sync_ok"], False)
            self.assertEqual(payload["failed_step"], "")
            self.assertEqual(payload["steps"][0]["status"], "NOT_RUN")
            self.assertEqual(payload["steps"][-1]["status"], "NOT_RUN")

    def test_success_runs_full_chain(self):
        with tempfile.TemporaryDirectory(prefix="refresh_shadow_derived_ok_") as td:
            root = Path(td)
            trace = root / "trace.log"
            result_json = root / "result.json"
            state_dir = root / "state"
            shadow_dir = root / "shadow"
            state_dir.mkdir(parents=True, exist_ok=True)
            shadow_dir.mkdir(parents=True, exist_ok=True)

            candidate_review_tool = root / "candidate_review.py"
            watchlist_tool = root / "watchlist.py"
            execution_ledger_tool = root / "execution_ledger.py"
            execution_events_tool = root / "execution_events.py"
            futures_paper_ledger_tool = root / "futures_paper_ledger.py"
            trade_ledger_tool = root / "trade_ledger.py"
            execution_pack_summary_tool = root / "execution_pack_summary.py"
            execution_rollup_tool = root / "execution_rollup.py"
            execution_outcome_review_tool = root / "execution_outcome_review.py"
            operator_snapshot_tool = root / "operator_snapshot.py"
            execution_review_queue_tool = root / "execution_review_queue.py"

            write_candidate_review_script(candidate_review_tool, trace=trace)
            write_watchlist_script(watchlist_tool, trace=trace)
            write_flag_output_script(execution_ledger_tool, trace=trace, step_name="execution_ledger", output_flag="--out-jsonl")
            write_flag_output_script(execution_events_tool, trace=trace, step_name="execution_events", output_flag="--out-jsonl")
            write_flag_output_script(futures_paper_ledger_tool, trace=trace, step_name="futures_paper_ledger", output_flag="--out-json")
            write_flag_output_script(trade_ledger_tool, trace=trace, step_name="trade_ledger", output_flag="--out-jsonl")
            write_flag_output_script(
                execution_pack_summary_tool,
                trace=trace,
                step_name="execution_pack_summary",
                output_flag="--out-json",
            )
            write_flag_output_script(
                execution_rollup_tool,
                trace=trace,
                step_name="execution_rollup_snapshot",
                output_flag="--out-json",
            )
            write_flag_output_script(
                execution_outcome_review_tool,
                trace=trace,
                step_name="execution_outcome_review",
                output_flag="--out-json",
            )
            write_flag_output_script(
                operator_snapshot_tool,
                trace=trace,
                step_name="operator_snapshot",
                output_flag="--out-json",
            )
            write_flag_output_script(
                execution_review_queue_tool,
                trace=trace,
                step_name="execution_review_queue",
                output_flag="--out-json",
            )

            res = self._run(
                "--state-dir",
                str(state_dir),
                "--shadow-state-dir",
                str(shadow_dir),
                "--candidate-review-tool",
                str(candidate_review_tool),
                "--watchlist-tool",
                str(watchlist_tool),
                "--execution-ledger-tool",
                str(execution_ledger_tool),
                "--execution-events-tool",
                str(execution_events_tool),
                "--futures-paper-ledger-tool",
                str(futures_paper_ledger_tool),
                "--trade-ledger-tool",
                str(trade_ledger_tool),
                "--execution-pack-summary-tool",
                str(execution_pack_summary_tool),
                "--execution-rollup-tool",
                str(execution_rollup_tool),
                "--execution-outcome-review-tool",
                str(execution_outcome_review_tool),
                "--operator-snapshot-tool",
                str(operator_snapshot_tool),
                "--execution-review-queue-tool",
                str(execution_review_queue_tool),
                "--result-json",
                str(result_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["sync_ok"], True)
            self.assertEqual(payload["failed_step"], "")
            self.assertEqual(
                trace.read_text(encoding="utf-8").splitlines(),
                [
                    "candidate_review",
                    "watchlist",
                    "execution_ledger",
                    "execution_events",
                    "futures_paper_ledger",
                    "trade_ledger",
                    "execution_pack_summary",
                    "execution_rollup_snapshot",
                    "execution_outcome_review",
                    "operator_snapshot",
                    "execution_review_queue",
                ],
            )
            self.assertTrue((shadow_dir / "shadow_operator_snapshot_v0.json").exists())
            self.assertTrue((shadow_dir / "shadow_execution_review_queue_v0.json").exists())
            self.assertTrue((shadow_dir / "shadow_execution_events_v1.jsonl").exists())
            self.assertTrue((shadow_dir / "shadow_futures_paper_ledger_v1.json").exists())
            self.assertTrue((shadow_dir / "shadow_trade_ledger_v1.jsonl").exists())

    def test_skip_candidate_review_starts_with_watchlist(self):
        with tempfile.TemporaryDirectory(prefix="refresh_shadow_derived_skip_") as td:
            root = Path(td)
            trace = root / "trace.log"
            result_json = root / "result.json"
            state_dir = root / "state"
            shadow_dir = root / "shadow"
            state_dir.mkdir(parents=True, exist_ok=True)
            shadow_dir.mkdir(parents=True, exist_ok=True)
            (state_dir / "candidate_review.json").write_text("{}\n", encoding="utf-8")

            candidate_review_tool = root / "candidate_review_should_not_run.py"
            watchlist_tool = root / "watchlist.py"
            execution_ledger_tool = root / "execution_ledger.py"
            execution_events_tool = root / "execution_events.py"
            futures_paper_ledger_tool = root / "futures_paper_ledger.py"
            trade_ledger_tool = root / "trade_ledger.py"
            execution_pack_summary_tool = root / "execution_pack_summary.py"
            execution_rollup_tool = root / "execution_rollup.py"
            execution_outcome_review_tool = root / "execution_outcome_review.py"
            operator_snapshot_tool = root / "operator_snapshot.py"
            execution_review_queue_tool = root / "execution_review_queue.py"

            write_failing_script(candidate_review_tool, trace=trace, step_name="candidate_review", exit_code=9)
            write_watchlist_script(watchlist_tool, trace=trace)
            write_flag_output_script(execution_ledger_tool, trace=trace, step_name="execution_ledger", output_flag="--out-jsonl")
            write_flag_output_script(execution_events_tool, trace=trace, step_name="execution_events", output_flag="--out-jsonl")
            write_flag_output_script(futures_paper_ledger_tool, trace=trace, step_name="futures_paper_ledger", output_flag="--out-json")
            write_flag_output_script(trade_ledger_tool, trace=trace, step_name="trade_ledger", output_flag="--out-jsonl")
            write_flag_output_script(
                execution_pack_summary_tool,
                trace=trace,
                step_name="execution_pack_summary",
                output_flag="--out-json",
            )
            write_flag_output_script(
                execution_rollup_tool,
                trace=trace,
                step_name="execution_rollup_snapshot",
                output_flag="--out-json",
            )
            write_flag_output_script(
                execution_outcome_review_tool,
                trace=trace,
                step_name="execution_outcome_review",
                output_flag="--out-json",
            )
            write_flag_output_script(
                operator_snapshot_tool,
                trace=trace,
                step_name="operator_snapshot",
                output_flag="--out-json",
            )
            write_flag_output_script(
                execution_review_queue_tool,
                trace=trace,
                step_name="execution_review_queue",
                output_flag="--out-json",
            )

            res = self._run(
                "--skip-candidate-review",
                "--state-dir",
                str(state_dir),
                "--shadow-state-dir",
                str(shadow_dir),
                "--candidate-review-tool",
                str(candidate_review_tool),
                "--watchlist-tool",
                str(watchlist_tool),
                "--execution-ledger-tool",
                str(execution_ledger_tool),
                "--execution-events-tool",
                str(execution_events_tool),
                "--futures-paper-ledger-tool",
                str(futures_paper_ledger_tool),
                "--trade-ledger-tool",
                str(trade_ledger_tool),
                "--execution-pack-summary-tool",
                str(execution_pack_summary_tool),
                "--execution-rollup-tool",
                str(execution_rollup_tool),
                "--execution-outcome-review-tool",
                str(execution_outcome_review_tool),
                "--operator-snapshot-tool",
                str(operator_snapshot_tool),
                "--execution-review-queue-tool",
                str(execution_review_queue_tool),
                "--result-json",
                str(result_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["steps"][0]["status"], "SKIPPED")
            self.assertEqual(trace.read_text(encoding="utf-8").splitlines()[0], "watchlist")

    def test_failure_stops_chain(self):
        with tempfile.TemporaryDirectory(prefix="refresh_shadow_derived_fail_") as td:
            root = Path(td)
            trace = root / "trace.log"
            result_json = root / "result.json"
            state_dir = root / "state"
            shadow_dir = root / "shadow"
            state_dir.mkdir(parents=True, exist_ok=True)
            shadow_dir.mkdir(parents=True, exist_ok=True)

            candidate_review_tool = root / "candidate_review.py"
            watchlist_tool = root / "watchlist.py"
            execution_ledger_tool = root / "execution_ledger.py"
            execution_events_tool = root / "execution_events.py"
            futures_paper_ledger_tool = root / "futures_paper_ledger.py"
            trade_ledger_tool = root / "trade_ledger.py"
            execution_pack_summary_tool = root / "execution_pack_summary.py"
            execution_rollup_tool = root / "execution_rollup_fail.py"
            execution_outcome_review_tool = root / "execution_outcome_review.py"
            operator_snapshot_tool = root / "operator_snapshot.py"
            execution_review_queue_tool = root / "execution_review_queue.py"

            write_candidate_review_script(candidate_review_tool, trace=trace)
            write_watchlist_script(watchlist_tool, trace=trace)
            write_flag_output_script(execution_ledger_tool, trace=trace, step_name="execution_ledger", output_flag="--out-jsonl")
            write_flag_output_script(execution_events_tool, trace=trace, step_name="execution_events", output_flag="--out-jsonl")
            write_flag_output_script(futures_paper_ledger_tool, trace=trace, step_name="futures_paper_ledger", output_flag="--out-json")
            write_flag_output_script(trade_ledger_tool, trace=trace, step_name="trade_ledger", output_flag="--out-jsonl")
            write_flag_output_script(
                execution_pack_summary_tool,
                trace=trace,
                step_name="execution_pack_summary",
                output_flag="--out-json",
            )
            write_failing_script(
                execution_rollup_tool,
                trace=trace,
                step_name="execution_rollup_snapshot",
                exit_code=7,
            )
            write_flag_output_script(
                execution_outcome_review_tool,
                trace=trace,
                step_name="execution_outcome_review",
                output_flag="--out-json",
            )
            write_flag_output_script(
                operator_snapshot_tool,
                trace=trace,
                step_name="operator_snapshot",
                output_flag="--out-json",
            )
            write_flag_output_script(
                execution_review_queue_tool,
                trace=trace,
                step_name="execution_review_queue",
                output_flag="--out-json",
            )

            res = self._run(
                "--state-dir",
                str(state_dir),
                "--shadow-state-dir",
                str(shadow_dir),
                "--candidate-review-tool",
                str(candidate_review_tool),
                "--watchlist-tool",
                str(watchlist_tool),
                "--execution-ledger-tool",
                str(execution_ledger_tool),
                "--execution-events-tool",
                str(execution_events_tool),
                "--futures-paper-ledger-tool",
                str(futures_paper_ledger_tool),
                "--trade-ledger-tool",
                str(trade_ledger_tool),
                "--execution-pack-summary-tool",
                str(execution_pack_summary_tool),
                "--execution-rollup-tool",
                str(execution_rollup_tool),
                "--execution-outcome-review-tool",
                str(execution_outcome_review_tool),
                "--operator-snapshot-tool",
                str(operator_snapshot_tool),
                "--execution-review-queue-tool",
                str(execution_review_queue_tool),
                "--result-json",
                str(result_json),
            )
            self.assertEqual(res.returncode, 2)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["sync_ok"], False)
            self.assertEqual(payload["failed_step"], "execution_rollup_snapshot")
            step_names = trace.read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                step_names,
                [
                    "candidate_review",
                    "watchlist",
                    "execution_ledger",
                    "execution_events",
                    "futures_paper_ledger",
                    "trade_ledger",
                    "execution_pack_summary",
                    "execution_rollup_snapshot",
                ],
            )
            self.assertEqual(payload["steps"][8]["status"], "NOT_RUN")
            self.assertEqual(payload["steps"][9]["status"], "NOT_RUN")
            self.assertEqual(payload["steps"][10]["status"], "NOT_RUN")


if __name__ == "__main__":
    unittest.main()
