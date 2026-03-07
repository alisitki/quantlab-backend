import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "run-shadow-watchlist-v0.js"


def write_watchlist(path: Path, items) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "shadow_watchlist_v0",
                "generated_ts_utc": "2026-03-07T06:30:00Z",
                "source": "candidate_review.tsv",
                "selection_policy": {},
                "items": items,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def item(*, rank: int, pack_id: str, exchange: str, stream: str, symbols: list[str]) -> dict:
    return {
        "rank": rank,
        "selection_slot": "overall_fill",
        "pack_id": pack_id,
        "pack_path": f"/tmp/{pack_id}",
        "decision_tier": "PROMOTE_STRONG",
        "score": "64.000000",
        "exchange": exchange,
        "stream": stream,
        "symbols": symbols,
        "context_flags": "MARK=PASS;FUNDING=PASS;OI=PASS",
        "watch_status": "ACTIVE",
        "notes": "",
    }


class RunShadowWatchlistV0Tests(unittest.TestCase):
    def _run(self, *args: str, env: dict | None = None):
        merged_env = os.environ.copy()
        if env:
            merged_env.update(env)
        return subprocess.run(
            ["node", str(SCRIPT), *args],
            cwd=str(REPO),
            capture_output=True,
            text=True,
            env=merged_env,
        )

    def test_help_works(self):
        res = self._run("--help")
        self.assertEqual(res.returncode, 0, msg=res.stderr)
        self.assertIn("Usage:", res.stdout)
        self.assertIn("--rank", res.stdout)

    def test_missing_strategy_fails_fast(self):
        with tempfile.TemporaryDirectory(prefix="shadow_wrapper_missing_strategy_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            write_watchlist(watchlist, [item(rank=1, pack_id="pack_a", exchange="bybit", stream="bbo", symbols=["ETHUSDT"])])
            env = os.environ.copy()
            env.pop("GO_LIVE_STRATEGY", None)
            res = subprocess.run(
                ["node", str(SCRIPT), "--watchlist", str(watchlist), "--dry-run"],
                cwd=str(REPO),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("missing_env:GO_LIVE_STRATEGY", res.stderr)

    def test_nonexistent_rank_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_wrapper_bad_rank_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            write_watchlist(watchlist, [item(rank=1, pack_id="pack_a", exchange="bybit", stream="bbo", symbols=["ETHUSDT"])])
            res = self._run("--watchlist", str(watchlist), "--rank", "5", "--dry-run", env={"GO_LIVE_STRATEGY": "core/strategy/test.js"})
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("rank_not_found:5", res.stderr)

    def test_nonexistent_pack_id_fails(self):
        with tempfile.TemporaryDirectory(prefix="shadow_wrapper_bad_packid_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            write_watchlist(watchlist, [item(rank=1, pack_id="pack_a", exchange="bybit", stream="bbo", symbols=["ETHUSDT"])])
            res = self._run("--watchlist", str(watchlist), "--pack-id", "missing_pack", "--dry-run", env={"GO_LIVE_STRATEGY": "core/strategy/test.js"})
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("pack_id_not_found:missing_pack", res.stderr)

    def test_conflicting_rank_and_pack_id_fail(self):
        with tempfile.TemporaryDirectory(prefix="shadow_wrapper_conflict_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            write_watchlist(
                watchlist,
                [
                    item(rank=1, pack_id="pack_a", exchange="bybit", stream="bbo", symbols=["ETHUSDT"]),
                    item(rank=2, pack_id="pack_b", exchange="binance", stream="trade", symbols=["BTCUSDT"]),
                ],
            )
            res = self._run(
                "--watchlist",
                str(watchlist),
                "--rank",
                "1",
                "--pack-id",
                "pack_b",
                "--dry-run",
                env={"GO_LIVE_STRATEGY": "core/strategy/test.js"},
            )
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("selection_conflict:rank=1:pack_id=pack_b", res.stderr)

    def test_dry_run_prints_exact_env_mapping(self):
        with tempfile.TemporaryDirectory(prefix="shadow_wrapper_env_") as td:
            root = Path(td)
            watchlist = root / "shadow_watchlist_v0.json"
            write_watchlist(
                watchlist,
                [
                    item(rank=1, pack_id="pack_a", exchange="bybit", stream="bbo", symbols=["ETHUSDT", "BTCUSDT"]),
                    item(rank=2, pack_id="pack_b", exchange="binance", stream="trade", symbols=["ADAUSDT"]),
                ],
            )
            res = self._run(
                "--watchlist",
                str(watchlist),
                "--pack-id",
                "pack_a",
                "--dry-run",
                env={"GO_LIVE_STRATEGY": "core/strategy/test.js"},
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertIn("selected_pack_id=pack_a", res.stdout)
            self.assertIn("selected_exchange=bybit", res.stdout)
            self.assertIn("selected_symbols_csv=ETHUSDT,BTCUSDT", res.stdout)
            self.assertIn("env_GO_LIVE_EXCHANGE=bybit", res.stdout)
            self.assertIn("env_GO_LIVE_SYMBOLS=ETHUSDT,BTCUSDT", res.stdout)
            self.assertIn("env_GO_LIVE_STRATEGY=core/strategy/test.js", res.stdout)
            self.assertIn("runner_path=", res.stdout)
            self.assertIn("dry_run=1", res.stdout)


if __name__ == "__main__":
    unittest.main()
