import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "verify-soft-live.js"


class VerifySoftLiveV0Tests(unittest.TestCase):
    def test_verify_uses_explicit_summary_path_env(self):
        with tempfile.TemporaryDirectory(prefix="verify_soft_live_") as td:
            root = Path(td)
            summary_json = root / "summary.json"
            audit_dir = root / "audit"
            audit_dir.mkdir(parents=True, exist_ok=True)
            live_run_id = "run_env_summary"
            summary_json.write_text(json.dumps({"live_run_id": live_run_id}) + "\n", encoding="utf-8")
            (audit_dir / "audit.jsonl").write_text(
                json.dumps({"action": "RUN_START", "metadata": {"live_run_id": live_run_id}}) + "\n"
                + json.dumps({"action": "RUN_STOP", "metadata": {"live_run_id": live_run_id}}) + "\n",
                encoding="utf-8",
            )

            env = {
                **os.environ,
                "SOFT_LIVE_SUMMARY_JSON": str(summary_json),
                "AUDIT_SPOOL_DIR": str(audit_dir),
                "RUN_ARCHIVE_ENABLED": "0",
            }
            res = subprocess.run(
                ["node", str(SCRIPT)],
                cwd=str(REPO),
                env=env,
                capture_output=True,
                text=True,
            )

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("PASS", res.stdout)


if __name__ == "__main__":
    unittest.main()
