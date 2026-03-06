import unittest
from datetime import datetime, timezone

from tools.phase5_big_hunt_scheduler_v1 import pick_next_plan


class OldestFirstSelectionV0Tests(unittest.TestCase):
    def test_older_pending_beats_newer_failed_retry(self):
        index_obj = {
            "plan_latest": {
                "newer-failed": {
                    "plan_id": "newer-failed",
                    "status": "FAILED",
                    "tries": 1,
                    "exchange": "binance",
                    "stream": "bbo",
                    "start": "20260105",
                    "end": "20260105",
                    "created_ts_utc": "2026-03-06T11:33:14Z",
                },
                "older-pending": {
                    "plan_id": "older-pending",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T12:33:14Z",
                },
            }
        }
        picked = pick_next_plan(
            index_obj,
            max_tries=2,
            stale_running_min=180.0,
            now_dt=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(picked[0], "older-pending")

    def test_same_date_uses_exchange_stream_tie_break(self):
        index_obj = {
            "plan_latest": {
                "okx-trade": {
                    "plan_id": "okx-trade",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "okx",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:33:14Z",
                },
                "binance-bbo": {
                    "plan_id": "binance-bbo",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "bbo",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:35:14Z",
                },
            }
        }
        picked = pick_next_plan(
            index_obj,
            max_tries=2,
            stale_running_min=180.0,
            now_dt=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(picked[0], "binance-bbo")

    def test_same_lane_prefers_pending_over_failed_over_stale(self):
        index_obj = {
            "plan_latest": {
                "same-lane-stale": {
                    "plan_id": "same-lane-stale",
                    "status": "RUNNING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:33:14Z",
                    "updated_ts_utc": "2026-03-01T00:00:00Z",
                },
                "same-lane-failed": {
                    "plan_id": "same-lane-failed",
                    "status": "FAILED",
                    "tries": 1,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:32:14Z",
                },
                "same-lane-pending": {
                    "plan_id": "same-lane-pending",
                    "status": "PENDING",
                    "tries": 0,
                    "exchange": "binance",
                    "stream": "trade",
                    "start": "20260104",
                    "end": "20260104",
                    "created_ts_utc": "2026-03-06T11:31:14Z",
                },
            }
        }
        picked = pick_next_plan(
            index_obj,
            max_tries=2,
            stale_running_min=180.0,
            now_dt=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(picked[0], "same-lane-pending")
        self.assertEqual(picked[2], "PENDING")


if __name__ == "__main__":
    unittest.main()
