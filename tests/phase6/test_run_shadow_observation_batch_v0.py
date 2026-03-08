import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-shadow-observation-batch-v0.py"
SUMMARY_TOOL = REPO / "tools" / "shadow_observation_summary_v0.py"
HISTORY_TOOL = REPO / "tools" / "shadow_observation_history_v0.py"
VERIFY_SCRIPT = REPO / "tools" / "verify-soft-live.js"
EXECUTION_LEDGER_TOOL = REPO / "tools" / "shadow_execution_ledger_v0.py"
EXECUTION_PACK_SUMMARY_TOOL = REPO / "tools" / "shadow_execution_pack_summary_v0.py"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_watchlist(path: Path, items: list[dict]) -> None:
    write_json(
        path,
        {
            "schema_version": "shadow_watchlist_v0",
            "generated_ts_utc": "2026-03-07T08:00:00Z",
            "source": "candidate_review.tsv",
            "selection_policy": {"top_n": len(items)},
            "items": items,
        },
    )


def make_item(rank: int, pack_id: str, exchange: str, symbols: list[str]) -> dict:
    return {
        "rank": rank,
        "selection_slot": "overall_fill",
        "pack_id": pack_id,
        "pack_path": f"/tmp/{pack_id}",
        "decision_tier": "PROMOTE_STRONG",
        "score": "80.000000",
        "exchange": exchange,
        "stream": "bbo",
        "symbols": symbols,
        "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
        "watch_status": "ACTIVE",
        "observed_before": False,
        "observation_count": 0,
        "last_observed_at": "",
        "last_verify_soft_live_pass": "unknown",
        "last_stop_reason": "",
        "last_processed_event_count": "unknown",
        "last_observation_age_hours": "unknown",
        "observation_recency_bucket": "NEVER_OBSERVED",
        "observation_last_outcome_short": "NO_HISTORY",
        "observation_attention_flag": "false",
        "observation_status": "NEW",
        "next_action_hint": "READY_TO_OBSERVE",
        "reobserve_status": "NOT_OBSERVED",
        "recent_observation_trail": "",
        "notes": "",
    }


def write_fake_wrapper(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env node
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const args = process.argv.slice(2);
let packId = '';
for (let i = 0; i < args.length; i += 1) {
  if (args[i] === '--pack-id') {
    packId = String(args[i + 1] || '');
  }
}
if (packId.includes('fail')) {
  console.error('fake_wrapper_fail');
  process.exit(2);
}
const summaryPath = process.env.SHADOW_BATCH_SUMMARY_JSON;
const auditDir = process.env.AUDIT_SPOOL_DIR;
const liveRunId = `run_${packId}`;
mkdirSync(auditDir, { recursive: true });
writeFileSync(
  path.join(auditDir, 'audit.jsonl'),
  JSON.stringify({ action: 'RUN_START', metadata: { live_run_id: liveRunId } }) + '\\n' +
  JSON.stringify({ action: 'RUN_STOP', metadata: { live_run_id: liveRunId } }) + '\\n',
  'utf8'
);
writeFileSync(
  summaryPath,
  JSON.stringify({
    live_run_id: liveRunId,
    started_at: '2026-03-07T07:16:21.367Z',
    finished_at: '2026-03-07T07:16:43.420Z',
    stop_reason: 'STREAM_END'
  }) + '\\n',
  'utf8'
);
console.log(JSON.stringify({ event: 'soft_live_heartbeat', live_run_id: liveRunId, status: 'RUNNING', decision_count: 0 }));
console.log('[run_fake] [INFO] total_processed: 7');
process.exit(0);
""",
        encoding="utf-8",
    )


def write_fake_refresh(path: Path, *, exit_code: int = 0) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "state_dir = Path(args[args.index('--state-dir') + 1])\n"
            "shadow_state_dir = Path(args[args.index('--shadow-state-dir') + 1])\n"
            "execution_ledger_jsonl = Path(args[args.index('--execution-ledger-jsonl') + 1])\n"
            "execution_pack_summary_json = Path(args[args.index('--execution-pack-summary-json') + 1])\n"
            "result_path = Path(args[args.index('--result-json') + 1])\n"
            "execution_ledger_jsonl.parent.mkdir(parents=True, exist_ok=True)\n"
            "execution_pack_summary_json.parent.mkdir(parents=True, exist_ok=True)\n"
            "execution_ledger_jsonl.write_text('{}\\n', encoding='utf-8')\n"
            "execution_pack_summary_json.write_text('{}\\n', encoding='utf-8')\n"
            "result_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "result_path.write_text(json.dumps({'schema_version':'shadow_derived_surface_refresh_v0','sync_ok':%s,'failed_step':'','steps':[{'name':'candidate_review','status':'OK','exit_code':0,'command':'candidate_review','output_path':str(state_dir / 'candidate_review.json')},{'name':'watchlist','status':'OK','exit_code':0,'command':'watchlist','output_path':str(shadow_state_dir / 'shadow_watchlist_v0.json')},{'name':'execution_ledger','status':'OK','exit_code':0,'command':'execution_ledger','output_path':str(execution_ledger_jsonl)},{'name':'execution_pack_summary','status':'OK','exit_code':0,'command':'execution_pack_summary','output_path':str(execution_pack_summary_json)}]}) + '\\n', encoding='utf-8')\n"
            "raise SystemExit(%d)\n"
        )
        % ("True" if exit_code == 0 else "False", exit_code),
        encoding="utf-8",
    )
    path.chmod(0o755)


def write_fake_refresh_with_steps(
    path: Path,
    *,
    exit_code: int,
    sync_ok: bool,
    failed_step: str,
    execution_ledger_status: str,
    execution_ledger_exit_code: int | str,
    execution_pack_summary_status: str,
    execution_pack_summary_exit_code: int | str,
) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import sys\n"
            "from pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "state_dir = Path(args[args.index('--state-dir') + 1])\n"
            "shadow_state_dir = Path(args[args.index('--shadow-state-dir') + 1])\n"
            "execution_ledger_jsonl = Path(args[args.index('--execution-ledger-jsonl') + 1])\n"
            "execution_pack_summary_json = Path(args[args.index('--execution-pack-summary-json') + 1])\n"
            "result_path = Path(args[args.index('--result-json') + 1])\n"
            "if %r == 'OK':\n"
            "    execution_ledger_jsonl.parent.mkdir(parents=True, exist_ok=True)\n"
            "    execution_ledger_jsonl.write_text('{}\\n', encoding='utf-8')\n"
            "if %r == 'OK':\n"
            "    execution_pack_summary_json.parent.mkdir(parents=True, exist_ok=True)\n"
            "    execution_pack_summary_json.write_text('{}\\n', encoding='utf-8')\n"
            "payload = {\n"
            "    'schema_version': 'shadow_derived_surface_refresh_v0',\n"
            "    'sync_ok': %s,\n"
            "    'failed_step': %r,\n"
            "    'steps': [\n"
            "        {'name': 'candidate_review', 'status': 'OK', 'exit_code': 0, 'command': 'candidate_review', 'output_path': str(state_dir / 'candidate_review.json')},\n"
            "        {'name': 'watchlist', 'status': 'OK', 'exit_code': 0, 'command': 'watchlist', 'output_path': str(shadow_state_dir / 'shadow_watchlist_v0.json')},\n"
            "        {'name': 'execution_ledger', 'status': %r, 'exit_code': %r, 'command': 'execution_ledger', 'output_path': str(execution_ledger_jsonl)},\n"
            "        {'name': 'execution_pack_summary', 'status': %r, 'exit_code': %r, 'command': 'execution_pack_summary', 'output_path': str(execution_pack_summary_json)}\n"
            "    ]\n"
            "}\n"
            "result_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "result_path.write_text(json.dumps(payload) + '\\n', encoding='utf-8')\n"
            "raise SystemExit(%d)\n"
        )
        % (
            execution_ledger_status,
            execution_pack_summary_status,
            "True" if sync_ok else "False",
            failed_step,
            execution_ledger_status,
            execution_ledger_exit_code,
            execution_pack_summary_status,
            execution_pack_summary_exit_code,
            exit_code,
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def write_fake_python_exit(path: Path, *, exit_code: int) -> None:
    path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "raise SystemExit(%d)\n"
        )
        % exit_code,
        encoding="utf-8",
    )
    path.chmod(0o755)


class ShadowObservationBatchV0Tests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )

    def test_dry_run_writes_result_without_spawning(self):
        with tempfile.TemporaryDirectory(prefix="shadow_batch_dry_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            result_json = root / "result.json"
            out_dir = root / "out"
            history_jsonl = root / "history.jsonl"
            index_json = root / "index.json"
            refresh_json = root / "refresh.json"
            refresh_tool = root / "fake-refresh.py"
            write_fake_refresh(refresh_tool)
            write_watchlist(
                watchlist,
                [
                    make_item(1, "pack_a", "bybit", ["BNBUSDT"]),
                    make_item(2, "pack_b", "binance", ["BTCUSDT"]),
                ],
            )

            res = self._run(
                "--watchlist",
                str(watchlist),
                "--max-items",
                "2",
                "--out-dir",
                str(out_dir),
                "--result-json",
                str(result_json),
                "--history-jsonl",
                str(history_jsonl),
                "--index-json",
                str(index_json),
                "--refresh-tool",
                str(refresh_tool),
                "--phase6-state-dir",
                str(root / "phase6_state"),
                "--shadow-state-dir",
                str(root / "shadow_state"),
                "--refresh-result-json",
                str(refresh_json),
                "--dry-run",
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["attempted_count"], 2)
            self.assertEqual(payload["completed_count"], 0)
            self.assertTrue(payload["dry_run"])
            self.assertEqual(payload["refresh_executed"], False)
            self.assertEqual(payload["surfaces_synced"], False)
            self.assertEqual(payload["execution_ledger_rebuild_executed"], False)
            self.assertEqual(payload["execution_pack_summary_rebuild_executed"], False)
            self.assertEqual(payload["execution_artifacts_synced"], False)
            self.assertEqual(payload["results"][0]["run_executed"], False)
            self.assertEqual(payload["results"][0]["note"], "dry_run")
            self.assertIn("--pack-id pack_a", payload["results"][0]["wrapper_command"])

    def test_top1_success_path_updates_summary_and_history(self):
        with tempfile.TemporaryDirectory(prefix="shadow_batch_success_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            result_json = root / "result.json"
            out_dir = root / "out"
            history_jsonl = root / "history.jsonl"
            index_json = root / "index.json"
            execution_ledger_jsonl = root / "execution_ledger.jsonl"
            execution_pack_summary_json = root / "execution_pack_summary.json"
            summary_json = root / "summary_runtime.json"
            audit_base = root / "audit"
            wrapper = root / "fake-wrapper.js"
            refresh_json = root / "refresh.json"
            refresh_tool = root / "fake-refresh.py"
            write_fake_wrapper(wrapper)
            write_fake_refresh(refresh_tool)
            write_watchlist(watchlist, [make_item(1, "pack_ok", "bybit", ["BNBUSDT"])])

            res = self._run(
                "--watchlist",
                str(watchlist),
                "--max-items",
                "1",
                "--wrapper-script",
                str(wrapper),
                "--summary-tool",
                str(SUMMARY_TOOL),
                "--history-tool",
                str(HISTORY_TOOL),
                "--verify-script",
                str(VERIFY_SCRIPT),
                "--summary-json-path",
                str(summary_json),
                "--history-jsonl",
                str(history_jsonl),
                "--index-json",
                str(index_json),
                "--execution-ledger-tool",
                str(EXECUTION_LEDGER_TOOL),
                "--execution-pack-summary-tool",
                str(EXECUTION_PACK_SUMMARY_TOOL),
                "--execution-ledger-jsonl",
                str(execution_ledger_jsonl),
                "--execution-pack-summary-json",
                str(execution_pack_summary_json),
                "--refresh-tool",
                str(refresh_tool),
                "--phase6-state-dir",
                str(root / "phase6_state"),
                "--shadow-state-dir",
                str(root / "shadow_state"),
                "--refresh-result-json",
                str(refresh_json),
                "--audit-base-dir",
                str(audit_base),
                "--out-dir",
                str(out_dir),
                "--result-json",
                str(result_json),
                "--strategy",
                "core/strategy/strategies/PrintHeadTailStrategy.js",
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["attempted_count"], 1)
            self.assertEqual(payload["completed_count"], 1)
            self.assertEqual(payload["refresh_executed"], True)
            self.assertEqual(payload["refresh_exit_code"], 0)
            self.assertEqual(payload["surfaces_synced"], True)
            self.assertEqual(payload["execution_ledger_rebuild_executed"], True)
            self.assertEqual(payload["execution_ledger_rebuild_exit_code"], 0)
            self.assertEqual(payload["execution_pack_summary_rebuild_executed"], True)
            self.assertEqual(payload["execution_pack_summary_rebuild_exit_code"], 0)
            self.assertEqual(payload["execution_artifacts_synced"], True)
            item = payload["results"][0]
            self.assertEqual(item["run_exit_code"], 0)
            self.assertEqual(item["verify_soft_live_pass"], True)
            self.assertEqual(item["summary_generated"], True)
            self.assertEqual(item["history_updated"], True)
            self.assertTrue(Path(item["summary_json_path"]).exists())
            history_lines = [line for line in history_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(history_lines), 1)
            index_payload = json.loads(index_json.read_text(encoding="utf-8"))
            self.assertEqual(index_payload["pack_count"], 1)
            self.assertTrue(refresh_json.exists())
            self.assertTrue(execution_ledger_jsonl.exists())
            self.assertTrue(execution_pack_summary_json.exists())

    def test_failure_on_one_item_continues_to_next(self):
        with tempfile.TemporaryDirectory(prefix="shadow_batch_fail_continue_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            result_json = root / "result.json"
            out_dir = root / "out"
            history_jsonl = root / "history.jsonl"
            index_json = root / "index.json"
            execution_ledger_jsonl = root / "execution_ledger.jsonl"
            execution_pack_summary_json = root / "execution_pack_summary.json"
            summary_json = root / "summary_runtime.json"
            audit_base = root / "audit"
            wrapper = root / "fake-wrapper.js"
            refresh_json = root / "refresh.json"
            refresh_tool = root / "fake-refresh.py"
            write_fake_wrapper(wrapper)
            write_fake_refresh_with_steps(
                refresh_tool,
                exit_code=7,
                sync_ok=False,
                failed_step="operator_snapshot",
                execution_ledger_status="OK",
                execution_ledger_exit_code=0,
                execution_pack_summary_status="OK",
                execution_pack_summary_exit_code=0,
            )
            write_watchlist(
                watchlist,
                [
                    make_item(1, "pack_fail", "bybit", ["BNBUSDT"]),
                    make_item(2, "pack_ok", "binance", ["BTCUSDT"]),
                ],
            )

            res = self._run(
                "--watchlist",
                str(watchlist),
                "--max-items",
                "2",
                "--wrapper-script",
                str(wrapper),
                "--summary-tool",
                str(SUMMARY_TOOL),
                "--history-tool",
                str(HISTORY_TOOL),
                "--verify-script",
                str(VERIFY_SCRIPT),
                "--summary-json-path",
                str(summary_json),
                "--history-jsonl",
                str(history_jsonl),
                "--index-json",
                str(index_json),
                "--execution-ledger-tool",
                str(EXECUTION_LEDGER_TOOL),
                "--execution-pack-summary-tool",
                str(EXECUTION_PACK_SUMMARY_TOOL),
                "--execution-ledger-jsonl",
                str(execution_ledger_jsonl),
                "--execution-pack-summary-json",
                str(execution_pack_summary_json),
                "--refresh-tool",
                str(refresh_tool),
                "--phase6-state-dir",
                str(root / "phase6_state"),
                "--shadow-state-dir",
                str(root / "shadow_state"),
                "--refresh-result-json",
                str(refresh_json),
                "--audit-base-dir",
                str(audit_base),
                "--out-dir",
                str(out_dir),
                "--result-json",
                str(result_json),
                "--strategy",
                "core/strategy/strategies/PrintHeadTailStrategy.js",
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["attempted_count"], 2)
            self.assertEqual(payload["completed_count"], 1)
            self.assertEqual(payload["refresh_executed"], True)
            self.assertEqual(payload["refresh_exit_code"], 7)
            self.assertEqual(payload["surfaces_synced"], False)
            self.assertEqual(payload["refresh_note"], "refresh_exit_7:operator_snapshot")
            self.assertEqual(payload["execution_ledger_rebuild_executed"], True)
            self.assertEqual(payload["execution_ledger_rebuild_exit_code"], 0)
            self.assertEqual(payload["execution_pack_summary_rebuild_executed"], True)
            self.assertEqual(payload["execution_pack_summary_rebuild_exit_code"], 0)
            self.assertEqual(payload["execution_artifacts_synced"], True)
            first, second = payload["results"]
            self.assertEqual(first["run_exit_code"], 2)
            self.assertEqual(first["summary_generated"], False)
            self.assertEqual(first["history_updated"], False)
            self.assertIn("wrapper_exit_2", first["note"])
            self.assertEqual(second["run_exit_code"], 0)
            self.assertEqual(second["history_updated"], True)

    def test_canonical_refresh_failure_exposes_execution_step_status(self):
        with tempfile.TemporaryDirectory(prefix="shadow_batch_exec_fail_") as td:
            root = Path(td)
            watchlist = root / "watchlist.json"
            result_json = root / "result.json"
            out_dir = root / "out"
            history_jsonl = root / "history.jsonl"
            index_json = root / "index.json"
            execution_ledger_jsonl = root / "execution_ledger.jsonl"
            execution_pack_summary_json = root / "execution_pack_summary.json"
            summary_json = root / "summary_runtime.json"
            audit_base = root / "audit"
            wrapper = root / "fake-wrapper.js"
            refresh_json = root / "refresh.json"
            refresh_tool = root / "fake-refresh.py"
            write_fake_wrapper(wrapper)
            write_fake_refresh_with_steps(
                refresh_tool,
                exit_code=2,
                sync_ok=False,
                failed_step="execution_ledger",
                execution_ledger_status="FAILED",
                execution_ledger_exit_code=9,
                execution_pack_summary_status="NOT_RUN",
                execution_pack_summary_exit_code="not_run",
            )
            write_watchlist(watchlist, [make_item(1, "pack_ok", "bybit", ["BNBUSDT"])])

            res = self._run(
                "--watchlist",
                str(watchlist),
                "--max-items",
                "1",
                "--wrapper-script",
                str(wrapper),
                "--summary-tool",
                str(SUMMARY_TOOL),
                "--history-tool",
                str(HISTORY_TOOL),
                "--verify-script",
                str(VERIFY_SCRIPT),
                "--summary-json-path",
                str(summary_json),
                "--history-jsonl",
                str(history_jsonl),
                "--index-json",
                str(index_json),
                "--execution-ledger-jsonl",
                str(execution_ledger_jsonl),
                "--execution-pack-summary-json",
                str(execution_pack_summary_json),
                "--refresh-tool",
                str(refresh_tool),
                "--phase6-state-dir",
                str(root / "phase6_state"),
                "--shadow-state-dir",
                str(root / "shadow_state"),
                "--refresh-result-json",
                str(refresh_json),
                "--audit-base-dir",
                str(audit_base),
                "--out-dir",
                str(out_dir),
                "--result-json",
                str(result_json),
                "--strategy",
                "core/strategy/strategies/PrintHeadTailStrategy.js",
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["completed_count"], 1)
            self.assertEqual(payload["refresh_executed"], True)
            self.assertEqual(payload["refresh_exit_code"], 2)
            self.assertEqual(payload["surfaces_synced"], False)
            self.assertEqual(payload["execution_ledger_rebuild_executed"], True)
            self.assertEqual(payload["execution_ledger_rebuild_exit_code"], 9)
            self.assertEqual(payload["execution_pack_summary_rebuild_executed"], False)
            self.assertEqual(payload["execution_pack_summary_rebuild_exit_code"], "not_run")
            self.assertEqual(payload["execution_artifacts_synced"], False)
            self.assertEqual(payload["execution_rebuild_note"], "execution_ledger_exit_9")
            self.assertEqual(payload["refresh_note"], "refresh_exit_2:execution_ledger")


if __name__ == "__main__":
    unittest.main()
