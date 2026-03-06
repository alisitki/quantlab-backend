import csv
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from tools.phase5_big_hunt_v0 import default_run_id, load_object_keys_rows, select_symbols


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "phase5_big_hunt_v0.py"


def write_tsv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


class BigHuntV0ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="bighunt_v0_test_"))
        self.created_pack_dirs = []

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        for d in self.created_pack_dirs:
            shutil.rmtree(d, ignore_errors=True)

    def test_deterministic_selection_with_explicit_and_partition_key_fallback(self):
        tsv = self.tmpdir / "keys.tsv"
        write_tsv(
            tsv,
            ["label", "partition_key", "date", "data_key", "bucket", "exchange", "stream", "symbol"],
            [
                ["d1", "", "20260104", "exchange=binance/stream=trade/symbol=ethusdt/date=20260104/data.parquet", "quantlab-compact", "binance", "trade", "ethusdt"],
                ["d1", "binance/trade/btcusdt/20260104", "", "exchange=binance/stream=trade/symbol=btcusdt/date=20260104/data.parquet", "quantlab-compact", "", "", ""],
                ["d1", "binance/trade/solusdt/20260104", "", "exchange=binance/stream=trade/symbol=solusdt/date=20260104/data.parquet", "quantlab-compact", "", "", ""],
            ],
        )
        _, rows = load_object_keys_rows(tsv)
        sel = select_symbols(
            rows,
            exchange="binance",
            stream="trade",
            start="20260104",
            end="20260104",
            max_symbols=20,
            max_wall_min=120,
            per_run_timeout_min=12,
        )
        self.assertEqual(sel["available_symbols"], ["btcusdt", "ethusdt", "solusdt"])
        self.assertEqual(sel["selected_symbols"], ["btcusdt", "ethusdt", "solusdt"])

    def test_run_id_format_contains_fullscan_major(self):
        rid = default_run_id("binance", "trade", "20260104", "20260105")
        self.assertIn("__FULLSCAN_MAJOR", rid)
        self.assertRegex(
            rid,
            r"^multi-hypothesis-phase5-bighunt-binance-trade-20260104\.\.20260105-\d{8}_\d{6}__FULLSCAN_MAJOR$",
        )

    def test_dry_run_plan_generation_command_count_matches_selection(self):
        tsv = self.tmpdir / "keys.tsv"
        write_tsv(
            tsv,
            ["label", "partition_key", "date", "data_key", "meta_key", "bucket"],
            [
                ["d1", "binance/trade/adausdt/20260104", "20260104", "exchange=binance/stream=trade/symbol=adausdt/date=20260104/data.parquet", "", "quantlab-compact"],
                ["d1", "binance/trade/btcusdt/20260104", "20260104", "exchange=binance/stream=trade/symbol=btcusdt/date=20260104/data.parquet", "", "quantlab-compact"],
                ["d1", "binance/trade/ethusdt/20260104", "20260104", "exchange=binance/stream=trade/symbol=ethusdt/date=20260104/data.parquet", "", "quantlab-compact"],
            ],
        )
        token = uuid4().hex[:8]
        run_id = f"multi-hypothesis-phase5-bighunt-binance-trade-20260104..20260104-{token}__FULLSCAN_MAJOR"
        plan_out = self.tmpdir / "plan.json"
        cmd = [
            "python3",
            str(SCRIPT),
            "--objectKeysTsv",
            str(tsv),
            "--exchange",
            "binance",
            "--stream",
            "trade",
            "--start",
            "20260104",
            "--end",
            "20260104",
            "--max-symbols",
            "2",
            "--per-run-timeout-min",
            "12",
            "--max-wall-min",
            "120",
            "--run-id",
            run_id,
            "--plan-out",
            str(plan_out),
            "--dry-run",
        ]
        res = subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)
        self.created_pack_dirs.append(REPO / "evidence" / run_id)
        self.assertEqual(res.returncode, 0, msg=res.stderr)
        obj = __import__("json").loads(plan_out.read_text(encoding="utf-8"))
        self.assertEqual(len(obj["selected_symbols"]), 2)
        self.assertEqual(len(obj["commands"]), 2)

    def test_zero_coverage_stop(self):
        tsv = self.tmpdir / "keys_zero.tsv"
        write_tsv(
            tsv,
            ["label", "partition_key", "date", "data_key", "bucket"],
            [["d1", "binance/bbo/btcusdt/20260104", "20260104", "exchange=binance/stream=bbo/symbol=btcusdt/date=20260104/data.parquet", "quantlab-compact"]],
        )
        token = uuid4().hex[:8]
        run_id = f"multi-hypothesis-phase5-bighunt-binance-trade-20260104..20260104-{token}__FULLSCAN_MAJOR"
        cmd = [
            "python3",
            str(SCRIPT),
            "--objectKeysTsv",
            str(tsv),
            "--exchange",
            "binance",
            "--stream",
            "trade",
            "--start",
            "20260104",
            "--end",
            "20260104",
            "--run-id",
            run_id,
            "--dry-run",
        ]
        res = subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)
        self.created_pack_dirs.append(REPO / "evidence" / run_id)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("available_symbols=0", res.stdout + res.stderr)

    def test_max_wall_budget_reduces_selection(self):
        tsv = self.tmpdir / "keys_wall.tsv"
        write_tsv(
            tsv,
            ["label", "partition_key", "date", "data_key", "bucket"],
            [
                ["d1", "binance/trade/adausdt/20260104", "20260104", "exchange=binance/stream=trade/symbol=adausdt/date=20260104/data.parquet", "quantlab-compact"],
                ["d1", "binance/trade/btcusdt/20260104", "20260104", "exchange=binance/stream=trade/symbol=btcusdt/date=20260104/data.parquet", "quantlab-compact"],
                ["d1", "binance/trade/ethusdt/20260104", "20260104", "exchange=binance/stream=trade/symbol=ethusdt/date=20260104/data.parquet", "quantlab-compact"],
            ],
        )
        _, rows = load_object_keys_rows(tsv)
        sel = select_symbols(
            rows,
            exchange="binance",
            stream="trade",
            start="20260104",
            end="20260104",
            max_symbols=20,
            max_wall_min=25,
            per_run_timeout_min=12,
        )
        self.assertEqual(sel["k_wall"], 2)
        self.assertEqual(sel["selected_count"], 2)
        self.assertEqual(sel["selected_symbols"], ["adausdt", "btcusdt"])


if __name__ == "__main__":
    unittest.main()
