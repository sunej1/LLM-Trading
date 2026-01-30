"""Synthetic tests for price horizon detection."""
from datetime import datetime, timedelta, timezone
import unittest

from src.news.export.price_enrichment import compute_time_horizons


def build_fetcher(series):
    def fetch(_ticker, start, end):
        return [p for p in series if start <= p[0] <= end]

    return fetch


class PriceEnrichmentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.event_ts = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

    def test_bottom_and_recovery_peak(self):
        series = [
            (self.event_ts, 100.0),
            (self.event_ts + timedelta(minutes=10), 90.0),
            (self.event_ts + timedelta(minutes=30), 105.0),
        ]
        t_bottom, t_peak, bottom_ts, peak_ts = compute_time_horizons("ABC", self.event_ts, build_fetcher(series))
        self.assertEqual(t_bottom, 10)
        self.assertEqual(t_peak, 20)
        self.assertEqual(bottom_ts, self.event_ts + timedelta(minutes=10))
        self.assertEqual(peak_ts, self.event_ts + timedelta(minutes=30))

    def test_no_recovery_peak(self):
        series = [
            (self.event_ts, 50.0),
            (self.event_ts + timedelta(minutes=5), 49.0),
        ]
        t_bottom, t_peak, bottom_ts, peak_ts = compute_time_horizons("ABC", self.event_ts, build_fetcher(series))
        self.assertEqual(t_bottom, 5)
        self.assertIsNone(t_peak)
        self.assertEqual(bottom_ts, self.event_ts + timedelta(minutes=5))
        self.assertIsNone(peak_ts)

    def test_missing_data(self):
        t_bottom, t_peak, bottom_ts, peak_ts = compute_time_horizons("ABC", self.event_ts, build_fetcher([]))
        self.assertIsNone(t_bottom)
        self.assertIsNone(t_peak)
        self.assertIsNone(bottom_ts)
        self.assertIsNone(peak_ts)


if __name__ == "__main__":
    unittest.main()
