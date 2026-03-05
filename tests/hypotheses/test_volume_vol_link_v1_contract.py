import csv
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "hypotheses" / "volume_vol_link_v1.py"
RUNNER = REPO / "tools" / "run-multi-hypothesis.js"


class VolumeVolLinkV1ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        token = uuid4().hex[:10]
        self.exchange = f"uvvl{token}"
        self.symbol = f"vvlv1{token}usdt"
        self.created_paths = []
        self.tmpdir = Path(tempfile.mkdtemp(prefix="vvlv1_contract_"))

    def tearDown(self) -> None:
        for p in reversed(self.created_paths):
            if p.is_file():
                p.unlink(missing_ok=True)
        shutil.rmtree(REPO / "data" / "curated" / f"exchange={self.exchange}", ignore_errors=True)
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_trade_day(self, day: str, ts, seq, price) -> None:
        out = (
            REPO
            / "data"
            / "curated"
            / f"exchange={self.exchange}"
            / "stream=trade"
            / f"symbol={self.symbol}"
            / f"date={day}"
            / "data.parquet"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "ts_event": pa.array(ts, type=pa.int64()),
                "seq": pa.array(seq, type=pa.int64()),
                "price": pa.array(price, type=pa.float64()),
            }
        )
        pq.write_table(table, out)
        self.created_paths.append(out)

    def _run_family(self, *, start: str, end: str, delta_ms: str, h_ms: str) -> Path:
        results = self.tmpdir / "results.tsv"
        summary = self.tmpdir / "summary.tsv"
        report = self.tmpdir / "report.json"
        cmd = [
            "python3",
            str(SCRIPT),
            "--exchange",
            self.exchange,
            "--symbol",
            self.symbol,
            "--stream",
            "trade",
            "--start",
            start,
            "--end",
            end,
            "--vvlDeltaMsList",
            delta_ms,
            "--vvlHMsList",
            h_ms,
            "--results-out",
            str(results),
            "--summary-out",
            str(summary),
            "--report-out",
            str(report),
        ]
        subprocess.run(cmd, cwd=str(REPO), check=True)
        return results

    def test_golden_header_and_stable_row_order(self) -> None:
        self._write_trade_day("20990501", [0, 1, 2, 3, 4], [1, 2, 3, 4, 5], [100.0, 101.0, 99.0, 102.0, 100.0])
        self._write_trade_day("20990502", [0, 1, 2, 3], [1, 2, 3, 4], [100.0, 100.5, 100.25, 100.75])
        results = self._run_family(start="20990501", end="20990502", delta_ms="2,1", h_ms="1")

        with results.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            rows = list(reader)
            self.assertEqual(
                reader.fieldnames,
                [
                    "exchange",
                    "symbol",
                    "date",
                    "stream",
                    "delta_ms",
                    "h_ms",
                    "sample_count",
                    "mean_activity",
                    "mean_rv_fwd",
                    "corr",
                    "t_stat",
                ],
            )

        self.assertEqual(len(rows), 4)
        self.assertEqual(
            [(r["date"], int(r["delta_ms"]), int(r["h_ms"])) for r in rows],
            [
                ("20990501", 1, 1),
                ("20990501", 2, 1),
                ("20990502", 1, 1),
                ("20990502", 2, 1),
            ],
        )
        for r in rows:
            self.assertRegex(r["mean_activity"], r"^-?\d+\.\d{15}$")
            self.assertRegex(r["mean_rv_fwd"], r"^-?\d+\.\d{15}$")
            self.assertRegex(r["corr"], r"^-?\d+\.\d{15}$")
            self.assertRegex(r["t_stat"], r"^-?\d+\.\d{15}$")

    def test_t_stat_zero_when_sample_size_below_three(self) -> None:
        self._write_trade_day("20990503", [0, 2, 4], [1, 2, 3], [100.0, 101.0, 102.0])
        results = self._run_family(start="20990503", end="20990503", delta_ms="2", h_ms="2")
        with results.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(rows[0]["sample_count"]), 1)
        self.assertEqual(rows[0]["t_stat"], "0.000000000000000")

    def test_runner_contract_contains_volume_vol_link_family_and_args(self) -> None:
        text = RUNNER.read_text(encoding="utf-8")
        self.assertIn("parseFamilyVolumeVolLink", text)
        self.assertIn("parseVolumeVolLinkResults", text)
        self.assertIn("--vvlDeltaMsList", text)
        self.assertIn("--vvlHMsList", text)
        self.assertIn("volume_vol_link_v1", text)


if __name__ == "__main__":
    unittest.main()
