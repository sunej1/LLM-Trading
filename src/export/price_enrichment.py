"""Helpers to fetch price data and compute time-to-bottom and bottom-to-peak labels."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

WINDOW_TO_FIND_BOTTOM_MIN = 7 * 24 * 60  # 7 days after event
WINDOW_TO_FIND_PEAK_MIN = 7 * 24 * 60  # 7 days after bottom

PricePoint = Tuple[datetime, float]
PriceFetcher = Callable[[str, datetime, datetime], Iterable[PricePoint]]


def parse_timestamp_utc(raw_ts: str) -> Optional[datetime]:
    """Parse an ISO-ish timestamp string and normalize to UTC; return None on failure."""
    if not raw_ts:
        return None
    try:
        dt = datetime.fromisoformat(raw_ts)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_price_points(points: Iterable[PricePoint]) -> List[PricePoint]:
    """Normalize timestamps to UTC and return a sorted list."""
    normalized: List[PricePoint] = []
    for ts, price in points:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        normalized.append((ts, float(price)))
    normalized.sort(key=lambda x: x[0])
    return normalized


def get_minute_prices(ticker: str, start_ts: datetime, end_ts: datetime) -> List[PricePoint]:
    """Placeholder price fetcher; replace with real market data integration.

    TODO: Wire this to a real price provider. For now, this returns an empty list to
    indicate missing data.
    """
    logger.debug(
        "get_minute_prices placeholder called for ticker=%s start=%s end=%s", ticker, start_ts, end_ts
    )
    return []


def compute_time_horizons(
    ticker: str,
    event_ts: datetime,
    fetch_prices: Optional[PriceFetcher] = None,
) -> Tuple[Optional[int], Optional[int], Optional[datetime], Optional[datetime]]:
    """Compute minutes to bottom and bottom-to-peak within configured windows.

    Returns (time_to_bottom, time_bottom_to_peak, bottom_ts, peak_ts) where times are rounded ints.
    """
    fetcher = fetch_prices or get_minute_prices

    bottom_window_end = event_ts + timedelta(minutes=WINDOW_TO_FIND_BOTTOM_MIN)
    raw_points_bottom = fetcher(ticker, event_ts, bottom_window_end)
    points_bottom = [
        (ts, price) for ts, price in normalize_price_points(raw_points_bottom) if event_ts <= ts <= bottom_window_end
    ]

    if not points_bottom:
        logger.warning(
            "Price data unavailable for ticker=%s in bottom window [%s, %s]",
            ticker,
            event_ts,
            bottom_window_end,
        )
        return None, None, None, None

    bottom_ts, _bottom_price = min(points_bottom, key=lambda x: (x[1], x[0]))  # price min, earliest timestamp tie-breaker
    time_to_bottom = round((bottom_ts - event_ts).total_seconds() / 60)

    peak_window_end = bottom_ts + timedelta(minutes=WINDOW_TO_FIND_PEAK_MIN)
    raw_points_peak = fetcher(ticker, bottom_ts, peak_window_end)
    points_peak = [
        (ts, price) for ts, price in normalize_price_points(raw_points_peak) if bottom_ts < ts <= peak_window_end
    ]

    if not points_peak:
        logger.warning(
            "Price data unavailable for ticker=%s after bottom in peak window (%s, %s]",
            ticker,
            bottom_ts,
            peak_window_end,
        )
        return time_to_bottom, None, bottom_ts, None

    peak_ts, _peak_price = max(points_peak, key=lambda x: (x[1], x[0]))  # price max, earliest timestamp tie-breaker
    time_bottom_to_peak = round((peak_ts - bottom_ts).total_seconds() / 60)
    return time_to_bottom, time_bottom_to_peak, bottom_ts, peak_ts


def _run_synthetic_tests() -> None:
    """Quick self-contained checks using synthetic price series."""
    def build_fetcher(series: List[PricePoint]) -> PriceFetcher:
        def fetch(_ticker: str, start: datetime, end: datetime) -> Iterable[PricePoint]:
            return [p for p in series if start <= p[0] <= end]

        return fetch

    event_ts = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

    # Simple drop then recovery
    series1 = [
        (event_ts + timedelta(minutes=0), 100.0),
        (event_ts + timedelta(minutes=10), 95.0),
        (event_ts + timedelta(minutes=20), 110.0),
    ]
    t_bottom, t_peak, bottom_ts, peak_ts = compute_time_horizons("TEST", event_ts, build_fetcher(series1))
    assert t_bottom == 10, f"expected 10, got {t_bottom}"
    assert t_peak == 10, f"expected 10, got {t_peak}"
    assert bottom_ts == event_ts + timedelta(minutes=10)
    assert peak_ts == event_ts + timedelta(minutes=20)

    # Flat then immediate bottom
    series2 = [
        (event_ts, 50.0),
        (event_ts + timedelta(minutes=5), 51.0),
        (event_ts + timedelta(minutes=15), 49.0),
    ]
    t_bottom, t_peak, bottom_ts, peak_ts = compute_time_horizons("TEST", event_ts, build_fetcher(series2))
    assert t_bottom == 15, f"expected 15, got {t_bottom}"
    assert t_peak is None  # no data after bottom for peak
    assert bottom_ts == event_ts + timedelta(minutes=15)
    assert peak_ts is None

    # No data case
    t_bottom, t_peak, bottom_ts, peak_ts = compute_time_horizons("TEST", event_ts, build_fetcher([]))
    assert t_bottom is None and t_peak is None and bottom_ts is None and peak_ts is None

    print("Synthetic price enrichment tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    _run_synthetic_tests()
