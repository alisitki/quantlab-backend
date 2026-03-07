import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "refresh-shadow-observation-surfaces-v0.py"


def write_script(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


class RefreshShadowObservationSurfacesV0Tests(unittest.TestCase):
    def _run(self, *args: str):
        cmd = ["python3", str(SCRIPT), *args]
        return subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)

    def test_dry_run_writes_contract(self):
        with tempfile.TemporaryDirectory(prefix="refresh_surfaces_dry_") as td:
            root = Path(td)
            state_dir = root / "state"
            shadow_dir = root / "shadow"
            result_json = root / "result.json"
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
            self.assertEqual(payload["dry_run"], True)
            self.assertEqual(payload["candidate_review_exit_code"], "not_run")
            self.assertEqual(payload["watchlist_exit_code"], "not_run")
            self.assertEqual(payload["sync_ok"], False)

    def test_success_runs_review_then_watchlist(self):
        with tempfile.TemporaryDirectory(prefix="refresh_surfaces_ok_") as td:
            root = Path(td)
            state_dir = root / "state"
            shadow_dir = root / "shadow"
            state_dir.mkdir(parents=True, exist_ok=True)
            shadow_dir.mkdir(parents=True, exist_ok=True)
            trace = root / "trace.log"
            result_json = root / "result.json"
            review_tool = root / "fake_review.py"
            watchlist_tool = root / "fake_watchlist.py"

            write_script(
                review_tool,
                "#!/usr/bin/env python3\n"
                "from pathlib import Path\n"
                "import sys\n"
                "trace = Path(sys.argv[sys.argv.index('--state-dir')+1]).parent / 'trace.log'\n"
                "trace.write_text(trace.read_text() + 'review\\n' if trace.exists() else 'review\\n', encoding='utf-8')\n"
                "state_dir = Path(sys.argv[sys.argv.index('--state-dir')+1])\n"
                "(state_dir / 'candidate_review.tsv').write_text('x\\n', encoding='utf-8')\n"
                "(state_dir / 'candidate_review.json').write_text('{}\\n', encoding='utf-8')\n",
            )
            write_script(
                watchlist_tool,
                "#!/usr/bin/env python3\n"
                "from pathlib import Path\n"
                "import sys\n"
                "state_dir = Path(sys.argv[sys.argv.index('--state-dir')+1])\n"
                "trace = state_dir.parent / 'trace.log'\n"
                "trace.write_text(trace.read_text() + 'watchlist\\n', encoding='utf-8')\n"
                "out_dir = Path(sys.argv[sys.argv.index('--out-dir')+1])\n"
                "(out_dir / 'shadow_watchlist_v0.json').write_text('{}\\n', encoding='utf-8')\n"
                "(out_dir / 'shadow_watchlist_v0.tsv').write_text('x\\n', encoding='utf-8')\n",
            )

            res = self._run(
                "--state-dir",
                str(state_dir),
                "--shadow-state-dir",
                str(shadow_dir),
                "--candidate-review-tool",
                str(review_tool),
                "--watchlist-tool",
                str(watchlist_tool),
                "--result-json",
                str(result_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["candidate_review_exit_code"], 0)
            self.assertEqual(payload["watchlist_exit_code"], 0)
            self.assertEqual(payload["sync_ok"], True)
            self.assertEqual(trace.read_text(encoding="utf-8").splitlines(), ["review", "watchlist"])

    def test_review_failure_stops_chain(self):
        with tempfile.TemporaryDirectory(prefix="refresh_surfaces_fail_") as td:
            root = Path(td)
            state_dir = root / "state"
            shadow_dir = root / "shadow"
            state_dir.mkdir(parents=True, exist_ok=True)
            shadow_dir.mkdir(parents=True, exist_ok=True)
            result_json = root / "result.json"
            review_tool = root / "fake_review_fail.py"
            watchlist_tool = root / "fake_watchlist.py"

            write_script(
                review_tool,
                "#!/usr/bin/env python3\n"
                "import sys\n"
                "print('review failed', file=sys.stderr)\n"
                "raise SystemExit(9)\n",
            )
            write_script(
                watchlist_tool,
                "#!/usr/bin/env python3\n"
                "raise SystemExit(0)\n",
            )

            res = self._run(
                "--state-dir",
                str(state_dir),
                "--shadow-state-dir",
                str(shadow_dir),
                "--candidate-review-tool",
                str(review_tool),
                "--watchlist-tool",
                str(watchlist_tool),
                "--result-json",
                str(result_json),
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            payload = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["candidate_review_exit_code"], 9)
            self.assertEqual(payload["watchlist_exit_code"], "not_run")
            self.assertEqual(payload["sync_ok"], False)


if __name__ == "__main__":
    unittest.main()
