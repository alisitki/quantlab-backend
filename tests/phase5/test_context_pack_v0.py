import csv
import hashlib
import json
import shutil
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import tools.phase5_big_hunt_v0 as bighunt
from tools.context_pack_v0 import (
    Point,
    TSV_COLUMNS,
    compute_basis_stats,
    main as context_main,
    nearest_trade_index,
    point_arrays,
)


def write_tsv(path: Path, header, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def read_tsv_row(path: Path) -> dict[str, str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            return {str(k): str(v) for k, v in row.items()}
    raise AssertionError(f"no rows in {path}")


def write_time_log(path: Path, elapsed: str = "0:00.01", rss_kb: int = 12345) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                f"Elapsed (wall clock) time (h:mm:ss or m:ss): {elapsed}",
                f"Maximum resident set size (kbytes): {rss_kb}",
                "Exit status: 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def write_stream_day(root: Path, exchange: str, stream: str, symbol: str, day: str, rows: list[dict]) -> Path:
    path = (
        root
        / f"exchange={exchange}"
        / f"stream={stream}"
        / f"symbol={symbol}"
        / f"date={day}"
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if stream == "trade":
        data = {
            "ts_event": [int(r["ts_event"]) for r in rows],
            "seq": [int(r["seq"]) for r in rows],
            "ts_recv": [int(r.get("ts_recv", r["ts_event"])) for r in rows],
            "exchange": [exchange for _ in rows],
            "symbol": [symbol for _ in rows],
            "stream": [stream for _ in rows],
            "stream_version": [1 for _ in rows],
            "price": [float(r["price"]) for r in rows],
            "qty": [float(r.get("qty", 1.0)) for r in rows],
            "side": [int(r.get("side", 1)) for r in rows],
            "trade_id": [str(r.get("trade_id", f"{day}-{idx}")) for idx, r in enumerate(rows)],
        }
    elif stream == "mark_price":
        data = {
            "ts_event": [int(r["ts_event"]) for r in rows],
            "seq": [int(r["seq"]) for r in rows],
            "ts_recv": [int(r.get("ts_recv", r["ts_event"])) for r in rows],
            "exchange": [exchange for _ in rows],
            "symbol": [symbol for _ in rows],
            "stream": [stream for _ in rows],
            "stream_version": [1 for _ in rows],
            "mark_price": [float(r["mark_price"]) for r in rows],
            "index_price": [float(r.get("index_price", r["mark_price"])) for r in rows],
        }
    elif stream == "funding":
        data = {
            "ts_event": [int(r["ts_event"]) for r in rows],
            "seq": [int(r["seq"]) for r in rows],
            "ts_recv": [int(r.get("ts_recv", r["ts_event"])) for r in rows],
            "exchange": [exchange for _ in rows],
            "symbol": [symbol for _ in rows],
            "stream": [stream for _ in rows],
            "stream_version": [1 for _ in rows],
            "funding_rate": [float(r["funding_rate"]) for r in rows],
            "next_funding_ts": [int(r.get("next_funding_ts", 0)) for r in rows],
        }
    elif stream == "open_interest":
        data = {
            "ts_event": [int(r["ts_event"]) for r in rows],
            "seq": [int(r["seq"]) for r in rows],
            "ts_recv": [int(r.get("ts_recv", r["ts_event"])) for r in rows],
            "exchange": [exchange for _ in rows],
            "symbol": [symbol for _ in rows],
            "stream": [stream for _ in rows],
            "stream_version": [1 for _ in rows],
            "open_interest": [float(r["open_interest"]) for r in rows],
        }
    else:
        raise AssertionError(f"unsupported test stream {stream}")
    pq.write_table(pa.Table.from_pydict(data), path)
    return path


def write_trade_download_day(root: Path, exchange: str, symbol: str, day: str, rows: list[dict]) -> Path:
    path = root / f"date={day}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "ts_event": [int(r["ts_event"]) for r in rows],
        "seq": [int(r["seq"]) for r in rows],
        "ts_recv": [int(r.get("ts_recv", r["ts_event"])) for r in rows],
        "exchange": [exchange for _ in rows],
        "symbol": [symbol for _ in rows],
        "stream": ["trade" for _ in rows],
        "stream_version": [1 for _ in rows],
        "price": [float(r["price"]) for r in rows],
        "qty": [float(r.get("qty", 1.0)) for r in rows],
        "side": [int(r.get("side", 1)) for r in rows],
        "trade_id": [str(r.get("trade_id", f"{day}-{idx}")) for idx, r in enumerate(rows)],
    }
    pq.write_table(pa.Table.from_pydict(data), path)
    return path


class ContextPackV0Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="context_pack_v0_"))
        self.curated_root = self.tmpdir / "curated"
        self.downloads_root = self.tmpdir / "downloads"
        self.out_root = self.tmpdir / "out"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def run_helper(
        self,
        *,
        exchange: str,
        symbol: str,
        core_stream: str,
        start: str,
        end: str,
        out_dir: Path,
    ) -> dict[str, str]:
        rc = context_main(
            [
                "--exchange",
                exchange,
                "--symbol",
                symbol,
                "--core-stream",
                core_stream,
                "--start",
                start,
                "--end",
                end,
                "--out-dir",
                str(out_dir),
                "--downloads-dir",
                str(self.downloads_root),
                "--curated-root",
                str(self.curated_root),
                "--s3-tool",
                str(self.tmpdir / "missing_s3_tool.py"),
            ]
        )
        self.assertEqual(rc, 0)
        return read_tsv_row(out_dir / "context_summary.tsv")

    def test_binance_trade_window_outputs_mark_and_funding_and_unsupported_oi(self) -> None:
        exchange = "binance"
        symbol = "btcusdt"
        day1 = "20990101"
        day2 = "20990102"
        write_trade_download_day(
            self.downloads_root,
            exchange,
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "price": 100.0}, {"ts_event": 2000, "seq": 0, "price": 102.0}],
        )
        write_trade_download_day(
            self.downloads_root,
            exchange,
            symbol,
            day2,
            [{"ts_event": 3000, "seq": 0, "price": 103.0}, {"ts_event": 4000, "seq": 0, "price": 104.0}],
        )
        # Curated trade values differ intentionally; basis must still use downloads first.
        write_stream_day(
            self.curated_root,
            exchange,
            "trade",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "price": 999.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "trade",
            symbol,
            day2,
            [{"ts_event": 3000, "seq": 0, "price": 999.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "mark_price",
            symbol,
            day1,
            [{"ts_event": 1100, "seq": 0, "mark_price": 100.0}, {"ts_event": 2100, "seq": 0, "mark_price": 102.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "mark_price",
            symbol,
            day2,
            [{"ts_event": 3100, "seq": 0, "mark_price": 103.0}, {"ts_event": 3900, "seq": 0, "mark_price": 104.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "funding",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "funding_rate": 0.0001}, {"ts_event": 2000, "seq": 1, "funding_rate": 0.0002}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "funding",
            symbol,
            day2,
            [{"ts_event": 3000, "seq": 0, "funding_rate": 0.0003}],
        )

        row = self.run_helper(
            exchange=exchange,
            symbol=symbol,
            core_stream="trade",
            start=day1,
            end=day2,
            out_dir=self.out_root / "binance_ok",
        )
        self.assertEqual(row["ctx_mark_price_status"], "OK")
        self.assertEqual(row["ctx_mark_price_first"], "100.000000000000000")
        self.assertEqual(row["ctx_mark_price_last"], "104.000000000000000")
        self.assertEqual(row["ctx_mark_price_change_bps"], "400.000000000000000")
        self.assertEqual(row["ctx_mark_trade_basis_mean_bps"], "0.000000000000000")
        self.assertEqual(row["ctx_mark_trade_basis_max_abs_bps"], "0.000000000000000")
        self.assertEqual(row["ctx_funding_status"], "OK")
        self.assertEqual(row["ctx_funding_count"], "3")
        self.assertEqual(row["ctx_funding_mean"], "0.000200000000000")
        self.assertEqual(row["ctx_oi_status"], "UNSUPPORTED_EXCHANGE")
        self.assertEqual(row["ctx_oi_count"], "NA")
        self.assertEqual(row["notes"], "")

    def test_supported_exchange_with_missing_oi_stream_is_absent(self) -> None:
        exchange = "bybit"
        symbol = "ethusdt"
        day1 = "20990101"
        day2 = "20990102"
        for day, trade_rows, mark_rows, funding_rows in [
            (day1, [{"ts_event": 1000, "seq": 0, "price": 200.0}], [{"ts_event": 1000, "seq": 0, "mark_price": 200.0}], [{"ts_event": 1000, "seq": 0, "funding_rate": 0.001}]),
            (day2, [{"ts_event": 2000, "seq": 0, "price": 201.0}], [{"ts_event": 2000, "seq": 0, "mark_price": 201.0}], [{"ts_event": 2000, "seq": 0, "funding_rate": 0.002}]),
        ]:
            write_trade_download_day(self.downloads_root, exchange, symbol, day, trade_rows)
            write_stream_day(self.curated_root, exchange, "mark_price", symbol, day, mark_rows)
            write_stream_day(self.curated_root, exchange, "funding", symbol, day, funding_rows)
        write_stream_day(
            self.curated_root,
            exchange,
            "open_interest",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "open_interest": 1500.0}],
        )

        row = self.run_helper(
            exchange=exchange,
            symbol=symbol,
            core_stream="trade",
            start=day1,
            end=day2,
            out_dir=self.out_root / "bybit_oi_absent",
        )
        self.assertEqual(row["ctx_mark_price_status"], "OK")
        self.assertEqual(row["ctx_funding_status"], "OK")
        self.assertEqual(row["ctx_oi_status"], "ABSENT")
        self.assertEqual(row["ctx_oi_count"], "NA")
        self.assertEqual(row["ctx_oi_first"], "NA")
        self.assertEqual(row["ctx_oi_change_pct"], "NA")
        self.assertIn("open_interest_missing_days=20990102", row["notes"])

    def test_missing_mark_and_funding_days_yield_absent_and_na(self) -> None:
        exchange = "binance"
        symbol = "solusdt"
        day1 = "20990101"
        day2 = "20990102"
        write_trade_download_day(
            self.downloads_root,
            exchange,
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "price": 10.0}],
        )
        write_trade_download_day(
            self.downloads_root,
            exchange,
            symbol,
            day2,
            [{"ts_event": 2000, "seq": 0, "price": 11.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "mark_price",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "mark_price": 10.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "funding",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "funding_rate": 0.01}],
        )

        row = self.run_helper(
            exchange=exchange,
            symbol=symbol,
            core_stream="trade",
            start=day1,
            end=day2,
            out_dir=self.out_root / "missing_mark_funding",
        )
        self.assertEqual(row["ctx_mark_price_status"], "ABSENT")
        self.assertEqual(row["ctx_mark_price_first"], "NA")
        self.assertEqual(row["ctx_mark_trade_basis_mean_bps"], "NA")
        self.assertEqual(row["ctx_funding_status"], "ABSENT")
        self.assertEqual(row["ctx_funding_count"], "NA")
        self.assertEqual(row["ctx_oi_status"], "UNSUPPORTED_EXCHANGE")
        self.assertEqual(
            row["notes"],
            "funding_missing_days=20990102;mark_price_missing_days=20990102",
        )

    def test_trade_basis_alignment_is_deterministic(self) -> None:
        exchange = "binance"
        symbol = "adausdt"
        day = "20990101"
        write_trade_download_day(
            self.downloads_root,
            exchange,
            symbol,
            day,
            [
                {"ts_event": 1000, "seq": 0, "price": 100.0},
                {"ts_event": 2000, "seq": 0, "price": 110.0},
                {"ts_event": 3000, "seq": 2, "price": 130.0},
                {"ts_event": 3000, "seq": 1, "price": 120.0},
            ],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "mark_price",
            symbol,
            day,
            [{"ts_event": 1500, "seq": 0, "mark_price": 100.0}, {"ts_event": 3000, "seq": 0, "mark_price": 100.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "funding",
            symbol,
            day,
            [{"ts_event": 1000, "seq": 0, "funding_rate": 0.0}],
        )

        row = self.run_helper(
            exchange=exchange,
            symbol=symbol,
            core_stream="trade",
            start=day,
            end=day,
            out_dir=self.out_root / "basis_deterministic",
        )
        self.assertEqual(row["ctx_mark_trade_basis_mean_bps"], "1000.000000000000000")
        self.assertEqual(row["ctx_mark_trade_basis_max_abs_bps"], "2000.000000000000000")
        self.assertEqual(row["ctx_mark_price_change_bps"], "0.000000000000000")

    def test_nearest_trade_index_prefers_earlier_timestamp_on_equal_distance(self) -> None:
        trade_points = [
            Point(ts_event=1000, seq=4, value=10.0, day="20990101"),
            Point(ts_event=2000, seq=0, value=20.0, day="20990101"),
            Point(ts_event=2000, seq=3, value=21.0, day="20990101"),
            Point(ts_event=3000, seq=1, value=30.0, day="20990101"),
        ]
        trade_ts, trade_seq, _ = point_arrays(trade_points)
        self.assertEqual(nearest_trade_index(trade_ts, trade_seq, 2500), 2)
        self.assertEqual(nearest_trade_index(trade_ts, trade_seq, 2000), 1)

    def test_dense_basis_series_is_correct_and_stable(self) -> None:
        trade_points = []
        mark_points = []
        expected_basis = []
        for idx in range(2000):
            trade_ts = 1000 + (idx * 10)
            trade_price = 100.0 + (idx * 0.01)
            mark_ts = trade_ts + 3
            mark_price = trade_price - 0.05
            trade_points.append(Point(ts_event=trade_ts, seq=idx % 3, value=trade_price, day="20990101"))
            mark_points.append(Point(ts_event=mark_ts, seq=idx % 2, value=mark_price, day="20990101"))
            expected_basis.append(10000.0 * (trade_price - mark_price) / mark_price)

        basis_mean, basis_max_abs, basis_note = compute_basis_stats(mark_points, trade_points)
        self.assertIsNone(basis_note)
        self.assertAlmostEqual(basis_mean or 0.0, sum(expected_basis) / len(expected_basis), places=12)
        self.assertAlmostEqual(basis_max_abs or 0.0, max(abs(v) for v in expected_basis), places=12)

    def test_output_is_deterministic_and_json_matches_tsv(self) -> None:
        exchange = "bybit"
        symbol = "xrpusdt"
        day1 = "20990101"
        day2 = "20990102"
        write_trade_download_day(
            self.downloads_root,
            exchange,
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "price": 50.0}],
        )
        write_trade_download_day(
            self.downloads_root,
            exchange,
            symbol,
            day2,
            [{"ts_event": 2000, "seq": 0, "price": 50.5}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "mark_price",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "mark_price": 50.0}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "mark_price",
            symbol,
            day2,
            [{"ts_event": 2000, "seq": 0, "mark_price": 50.5}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "funding",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "funding_rate": 0.0001}],
        )
        write_stream_day(
            self.curated_root,
            exchange,
            "open_interest",
            symbol,
            day1,
            [{"ts_event": 1000, "seq": 0, "open_interest": 5000.0}],
        )

        out1 = self.out_root / "deterministic_a"
        out2 = self.out_root / "deterministic_b"
        row = self.run_helper(
            exchange=exchange,
            symbol=symbol,
            core_stream="trade",
            start=day1,
            end=day2,
            out_dir=out1,
        )
        self.run_helper(
            exchange=exchange,
            symbol=symbol,
            core_stream="trade",
            start=day1,
            end=day2,
            out_dir=out2,
        )
        self.assertEqual(
            (out1 / "context_summary.tsv").read_text(encoding="utf-8"),
            (out2 / "context_summary.tsv").read_text(encoding="utf-8"),
        )
        self.assertEqual(
            (out1 / "context_summary.json").read_text(encoding="utf-8"),
            (out2 / "context_summary.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(
            row["notes"],
            "funding_missing_days=20990102;open_interest_missing_days=20990102",
        )
        payload = json.loads((out1 / "context_summary.json").read_text(encoding="utf-8"))
        json_row = payload["rows"][0]
        for col in TSV_COLUMNS:
            self.assertEqual(json_row[col], row[col])


class ContextPackV0IntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="context_pack_integration_"))
        self.object_keys = self.tmpdir / "object_keys.tsv"
        self.archive_root = self.tmpdir / "archive"
        self.phase6_state_dir = self.tmpdir / "phase6_state"
        self.phase6_state_dir.mkdir(parents=True, exist_ok=True)
        self.phase6_policy = self.tmpdir / "promotion_policy.json"
        self.phase6_policy.write_text("{}\n", encoding="utf-8")
        self.curated_root = self.tmpdir / "curated"
        write_tsv(
            self.object_keys,
            ["label", "partition_key", "date", "data_key", "meta_key", "bucket"],
            [[
                "day1",
                "binance/trade/btcusdt/20990101",
                "20990101",
                "exchange=binance/stream=trade/symbol=btcusdt/date=20990101/data.parquet",
                "",
                "quantlab-compact",
            ]],
        )
        self.run_id = "multi-hypothesis-phase5-bighunt-binance-trade-20990101..20990101-contextpack__FULLSCAN_MAJOR"
        self.repo = Path(bighunt.__file__).resolve().parents[1]
        self.evidence_dir = self.repo / "evidence"
        self.cleanup_paths = [
            self.evidence_dir / self.run_id,
            self.evidence_dir / f"{self.run_id}.tar.gz",
            self.evidence_dir / f"{self.run_id}.tar.gz.sha256",
            self.evidence_dir / f"{self.run_id}.sha_verify.txt",
            self.evidence_dir / f"{self.run_id}.moved_to.txt",
        ]

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        for path in self.cleanup_paths:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass

    def _fake_core_outputs(self, out_dir: Path) -> None:
        write_trade_download_day(
            out_dir / "downloads",
            "binance",
            "btcusdt",
            "20990101",
            [{"ts_event": 1000, "seq": 0, "price": 100.0}, {"ts_event": 2000, "seq": 0, "price": 101.0}],
        )
        write_stream_day(
            self.curated_root,
            "binance",
            "mark_price",
            "btcusdt",
            "20990101",
            [{"ts_event": 1000, "seq": 0, "mark_price": 100.0}, {"ts_event": 2000, "seq": 0, "mark_price": 101.0}],
        )
        write_stream_day(
            self.curated_root,
            "binance",
            "funding",
            "btcusdt",
            "20990101",
            [{"ts_event": 1000, "seq": 0, "funding_rate": 0.0001}],
        )
        base = out_dir / "artifacts" / "multi_hypothesis"
        base.mkdir(parents=True, exist_ok=True)
        write_tsv(
            base / "determinism_compare.tsv",
            ["window", "family_id", "primary_hash", "replay_hash", "determinism_status", "compare_basis"],
            [["20990101..20990101", "momentum_v1", "abc", "abc", "PASS", "test_basis"]],
        )
        write_tsv(
            base / "artifact_manifest.tsv",
            ["expected_relpath", "resolved_relpath", "status"],
            [
                ["artifacts/multi_hypothesis/determinism_compare.tsv", "artifacts/multi_hypothesis/determinism_compare.tsv", "OK"],
                ["artifacts/multi_hypothesis/artifact_manifest.tsv", "artifacts/multi_hypothesis/artifact_manifest.tsv", "OK"],
                ["artifacts/multi_hypothesis/label_report.txt", "artifacts/multi_hypothesis/label_report.txt", "OK"],
                ["artifacts/multi_hypothesis/integrity_check.txt", "artifacts/multi_hypothesis/integrity_check.txt", "OK"],
            ],
        )
        (base / "label_report.txt").write_text("label=PASS/MULTI_HYPOTHESIS_READY\n", encoding="utf-8")
        (base / "integrity_check.txt").write_text("missing_count=0\n", encoding="utf-8")
        write_time_log(out_dir / "time-v.log", elapsed="0:00.02", rss_kb=4321)

    def _fake_finalize(self, pack_dir: Path, archive_root: Path) -> None:
        archive_root.mkdir(parents=True, exist_ok=True)
        pack_tgz = self.evidence_dir / f"{self.run_id}.tar.gz"
        pack_sha = self.evidence_dir / f"{self.run_id}.tar.gz.sha256"
        evidence_sha_verify = self.evidence_dir / f"{self.run_id}.sha_verify.txt"
        moved_to = self.evidence_dir / f"{self.run_id}.moved_to.txt"
        with tarfile.open(pack_tgz, "w:gz") as tf:
            tf.add(pack_dir, arcname=self.run_id)
        digest = hashlib.sha256(pack_tgz.read_bytes()).hexdigest()
        pack_sha.write_text(f"{digest}  {pack_tgz}\n", encoding="utf-8")
        evidence_sha_verify.write_text(f"{pack_tgz}: OK\n", encoding="utf-8")
        archive_dir = archive_root / self.run_id
        shutil.move(str(pack_dir), str(archive_dir))
        (archive_dir / "sha_verify.txt").write_text(f"{pack_tgz}: OK\n", encoding="utf-8")
        moved_to.write_text(str(archive_dir) + "\n", encoding="utf-8")

    def test_pack_integration_smoke(self) -> None:
        original_run_cmd = bighunt.run_cmd

        def fake_run_cmd(cmd, cwd, stdout_path, stderr_path):  # noqa: ANN001
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_path.write_text("", encoding="utf-8")
            stderr_path.write_text("", encoding="utf-8")
            if "tools/run-multi-hypothesis.js" in cmd:
                out_dir = Path(cmd[cmd.index("--outDir") + 1])
                self._fake_core_outputs(out_dir)
                return 0
            if "tools/context_pack_v0.py" in cmd:
                cmd_with_root = list(cmd) + ["--curated-root", str(self.curated_root)]
                return original_run_cmd(cmd_with_root, cwd, stdout_path, stderr_path)
            if "tools/slim_finalize.sh" in cmd:
                pack_dir = Path(cmd[-2])
                archive_root = Path(cmd[-1])
                self._fake_finalize(pack_dir, archive_root)
                return 0
            if "tools/phase6_promotion_guards_v1.py" in cmd:
                pack_dir = Path(cmd[cmd.index("--pack") + 1])
                guards_dir = pack_dir / "guards"
                guards_dir.mkdir(parents=True, exist_ok=True)
                (guards_dir / "decision_report.txt").write_text("decision=PROMOTE\n", encoding="utf-8")
                stdout_path.write_text("decision=PROMOTE\nrecord_appended=true\n", encoding="utf-8")
                return 0
            raise AssertionError(f"unexpected command: {cmd}")

        argv = [
            "phase5_big_hunt_v0.py",
            "--objectKeysTsv",
            str(self.object_keys),
            "--exchange",
            "binance",
            "--stream",
            "trade",
            "--start",
            "20990101",
            "--end",
            "20990101",
            "--max-symbols",
            "1",
            "--per-run-timeout-min",
            "12",
            "--max-wall-min",
            "120",
            "--run-id",
            self.run_id,
            "--archive-root",
            str(self.archive_root),
            "--phase6-policy",
            str(self.phase6_policy),
            "--phase6-state-dir",
            str(self.phase6_state_dir),
        ]
        with patch.object(sys, "argv", argv):
            with patch("tools.phase5_big_hunt_v0.run_cmd", side_effect=fake_run_cmd):
                rc = bighunt.main()
        self.assertEqual(rc, 0)
        archive_dir = self.archive_root / self.run_id
        context_tsv = archive_dir / "runs" / "btcusdt" / "artifacts" / "context" / "context_summary.tsv"
        context_json = archive_dir / "runs" / "btcusdt" / "artifacts" / "context" / "context_summary.json"
        self.assertTrue(context_tsv.exists())
        self.assertTrue(context_json.exists())
        report = json.loads((archive_dir / "campaign_report.json").read_text(encoding="utf-8"))
        self.assertEqual(report["run_results"][0]["context_status"], "OK")
        tar_members = report["finalize"]["tar_members_matched"]
        self.assertTrue(any(member.endswith("context_summary.tsv") for member in tar_members))
        self.assertTrue(any(member.endswith("context_summary.json") for member in tar_members))


if __name__ == "__main__":
    unittest.main()
