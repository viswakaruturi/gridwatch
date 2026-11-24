import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))

import run_incremental


class TestIncrementalRecovery(unittest.TestCase):

    def test_recovers_missed_days(self):
        today = datetime.now(timezone.utc).date()

        previous_checkpoint = today - timedelta(days=7)
        expected_start = today - timedelta(days=6)
        expected_end = today - timedelta(days=1)

        with patch.object(
            run_incremental,
            "load_checkpoint",
            return_value=previous_checkpoint,
        ):
            start_date, end_date = run_incremental.get_date_range()

        self.assertEqual(start_date, expected_start.isoformat())
        self.assertEqual(end_date, expected_end.isoformat())

    def test_normal_three_day_lookback(self):
        today = datetime.now(timezone.utc).date()

        previous_checkpoint = today - timedelta(days=1)
        expected_start = today - timedelta(days=3)
        expected_end = today - timedelta(days=1)

        with patch.object(
            run_incremental,
            "load_checkpoint",
            return_value=previous_checkpoint,
        ):
            start_date, end_date = run_incremental.get_date_range()

        self.assertEqual(start_date, expected_start.isoformat())
        self.assertEqual(end_date, expected_end.isoformat())


if __name__ == "__main__":
    unittest.main()