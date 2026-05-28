"""Compute event-level CAPM plus news-severity mispricing.

The labeled event CSV is the row driver: one input event produces one output
row. Price data is used only to estimate the event's CAPM baseline and the
realized next-trading-day price move.
"""
from __future__ import annotations

import contextlib
import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/matplotlib")

import matplotlib
import numpy as np
import pandas as pd
import statsmodels.api as sm
import yfinance as yf


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_EVENT_PATH = PROJECT_ROOT / "data" / "combined" / "combined_labeled.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "combined" / "capm_event_level_mispricing.csv"

MARKET_TICKER = "SPY"
ROLLING_WINDOW = 252
RISK_FREE_DAILY = 0.0
DAILY_INTERVALS = {"1d", "5d", "1wk", "1mo", "3mo"}

SEVERITY_MOVE_MAP = {
    1: 0.005,
    2: 0.02,
    3: 0.05,
    4: 0.11,
    5: 0.18,
}

YAHOO_TICKER_ALIASES = {
    "BRKA": "BRK-A",
    "BRK.A": "BRK-A",
    "BRK/A": "BRK-A",
    "BRKB": "BRK-B",
    "BRK.B": "BRK-B",
    "BRK/B": "BRK-B",
}
MAX_TICKER_LENGTH = 6
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")


def load_events(event_path: Path = DEFAULT_EVENT_PATH) -> pd.DataFrame:
    """Read labeled events without modifying the source CSV."""
    events = pd.read_csv(event_path)
    events["_event_timestamp"] = pd.to_datetime(events["timestamp"], errors="coerce", utc=True)
    events["_ticker_normalized"] = events["ticker"].astype(str).str.upper().str.strip()
    events["yahoo_ticker"] = events["_ticker_normalized"].apply(normalize_yahoo_ticker)
    events["_label_severity_numeric"] = pd.to_numeric(events["label_severity"], errors="coerce")
    events["_label_direction_numeric"] = pd.to_numeric(events["label_direction"], errors="coerce")
    direction_map = {"positive": 1, "neutral": 0, "mixed": 0, "negative": -1}
    direction_text = events["label_direction"].astype(str).str.lower().str.strip()
    events["_label_direction_numeric"] = events["_label_direction_numeric"].fillna(
        direction_text.map(direction_map)
    )
    return events


def normalize_yahoo_ticker(ticker: object) -> str:
    """Normalize ticker strings into Yahoo Finance format, or blank if invalid."""
    if pd.isna(ticker):
        return ""

    normalized = str(ticker).upper().strip().replace("/", "-")
    if not normalized or normalized in {"NAN", "NONE", "NULL"}:
        return ""

    normalized = YAHOO_TICKER_ALIASES.get(normalized, normalized)
    if "." in normalized:
        parts = normalized.split(".")
        if len(parts) == 2 and all(parts):
            normalized = f"{parts[0]}-{parts[1]}"

    if len(normalized.replace("-", "")) > MAX_TICKER_LENGTH:
        return ""
    if not TICKER_PATTERN.match(normalized):
        return ""
    return normalized


def _select_price_column(price_data: pd.DataFrame) -> pd.Series:
    """Use adjusted close prices when available, otherwise close prices."""
    if isinstance(price_data.columns, pd.MultiIndex):
        first_level = price_data.columns.get_level_values(0)
        if "Adj Close" in first_level:
            return price_data.xs("Adj Close", axis=1, level=0).squeeze()
        if "Close" in first_level:
            return price_data.xs("Close", axis=1, level=0).squeeze()
        raise ValueError("Downloaded price data did not include Adj Close or Close.")

    if "Adj Close" in price_data.columns:
        return price_data["Adj Close"]
    if "Close" in price_data.columns:
        return price_data["Close"]
    raise ValueError("Downloaded price data did not include Adj Close or Close.")


def is_intraday_interval(price_interval: str) -> bool:
    """Return True when the requested Yahoo interval is intraday."""
    return price_interval.lower() not in DAILY_INTERVALS


def _index_to_naive_utc(index: pd.Index) -> pd.Series:
    """Convert Yahoo price index values to timezone-naive UTC timestamps."""
    timestamps = pd.to_datetime(index)
    if getattr(timestamps, "tz", None) is not None:
        timestamps = timestamps.tz_convert("UTC").tz_localize(None)
    return pd.Series(timestamps)


def get_price_data(
    ticker: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    price_cache: Dict[str, pd.DataFrame],
    price_interval: str = "1d",
) -> pd.DataFrame:
    """Download and cache price data by ticker and interval."""
    ticker = normalize_yahoo_ticker(ticker)
    price_interval = price_interval.lower()
    cache_key = f"{ticker}|{price_interval}"
    if cache_key in price_cache:
        return price_cache[cache_key]
    if not ticker:
        empty_prices = pd.DataFrame(columns=["date", "price"])
        price_cache[cache_key] = empty_prices
        return empty_prices

    with open(os.devnull, "w") as devnull, contextlib.redirect_stderr(devnull):
        raw_prices = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval=price_interval,
            auto_adjust=False,
            prepost=False,
            progress=False,
        )

    if raw_prices.empty:
        prices = pd.DataFrame(columns=["date", "price"])
    else:
        prices = pd.DataFrame(
            {
                "date": _index_to_naive_utc(raw_prices.index),
                "price": _select_price_column(raw_prices).to_numpy(),
            }
        ).sort_values("date")

    price_cache[cache_key] = prices
    return prices


def find_previous_next_trading_days(
    event_timestamp: pd.Timestamp,
    stock_prices: pd.DataFrame,
    market_prices: pd.DataFrame,
    price_interval: str = "1d",
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Find valid stock/market bars immediately before and after an event."""
    if pd.isna(event_timestamp) or stock_prices.empty or market_prices.empty:
        return None, None

    # Use only days present for both the event stock and SPY.
    common_dates = pd.Series(
        sorted(set(stock_prices["date"]).intersection(set(market_prices["date"])))
    )
    if common_dates.empty:
        return None, None

    event_dt = event_timestamp.tz_convert("UTC").tz_localize(None)
    if not is_intraday_interval(price_interval):
        event_dt = event_dt.normalize()

    previous_dates = common_dates.loc[common_dates < event_dt]
    next_dates = common_dates.loc[common_dates > event_dt]

    previous_trading_day = previous_dates.iloc[-1] if not previous_dates.empty else None
    next_trading_day = next_dates.iloc[0] if not next_dates.empty else None
    return previous_trading_day, next_trading_day


def estimate_event_beta(
    stock_prices: pd.DataFrame,
    market_prices: pd.DataFrame,
    previous_trading_day: Optional[pd.Timestamp],
    rolling_window: int = ROLLING_WINDOW,
    risk_free_daily: float = RISK_FREE_DAILY,
) -> float:
    """Estimate CAPM beta from the prior 252 daily returns ending at the previous trading day."""
    if previous_trading_day is None or stock_prices.empty or market_prices.empty:
        return np.nan

    merged = stock_prices.merge(
        market_prices,
        on="date",
        how="inner",
        suffixes=("_stock", "_market"),
    ).sort_values("date")
    merged = merged.loc[merged["date"] <= previous_trading_day].copy()
    merged["stock_return"] = merged["price_stock"].pct_change()
    merged["market_return"] = merged["price_market"].pct_change()

    # Need exactly prior-window return observations; fewer rows keep beta as NaN.
    regression_window = merged.dropna(subset=["stock_return", "market_return"]).tail(rolling_window)
    if len(regression_window) < rolling_window:
        return np.nan

    y = regression_window["stock_return"] - risk_free_daily
    x = regression_window["market_return"] - risk_free_daily
    x = sm.add_constant(x)
    model = sm.OLS(y, x).fit()
    return float(model.params["market_return"])


def _price_on_date(prices: pd.DataFrame, trading_day: Optional[pd.Timestamp]) -> float:
    """Return the price for a trading day, or NaN if unavailable."""
    if trading_day is None or prices.empty:
        return np.nan
    values = prices.loc[prices["date"] == trading_day, "price"]
    if values.empty:
        return np.nan
    return float(values.iloc[0])


def compute_event_mispricing(
    event: pd.Series,
    stock_prices: pd.DataFrame,
    market_prices: pd.DataFrame,
    rolling_window: int = ROLLING_WINDOW,
    risk_free_daily: float = RISK_FREE_DAILY,
    price_interval: str = "1d",
) -> Dict[str, object]:
    """Compute CAPM and news-adjusted mispricing fields for one event row."""
    previous_trading_day, next_trading_day = find_previous_next_trading_days(
        event["_event_timestamp"],
        stock_prices,
        market_prices,
        price_interval=price_interval,
    )

    previous_stock_price = _price_on_date(stock_prices, previous_trading_day)
    actual_stock_price = _price_on_date(stock_prices, next_trading_day)
    previous_market_price = _price_on_date(market_prices, previous_trading_day)
    actual_market_price = _price_on_date(market_prices, next_trading_day)

    beta = estimate_event_beta(
        stock_prices,
        market_prices,
        previous_trading_day,
        rolling_window=rolling_window,
        risk_free_daily=risk_free_daily,
    )

    # Market move from the prior close to the first close after the event.
    if previous_market_price and not np.isnan(previous_market_price):
        market_return_event_window = (actual_market_price / previous_market_price) - 1
    else:
        market_return_event_window = np.nan

    capm_fair_return = beta * market_return_event_window
    capm_fair_price = previous_stock_price * (1 + capm_fair_return)

    severity_move = SEVERITY_MOVE_MAP.get(event["_label_severity_numeric"], np.nan)
    signed_expected_move = event["_label_direction_numeric"] * severity_move

    news_adjusted_expected_return = capm_fair_return + signed_expected_move
    news_adjusted_fair_price = previous_stock_price * (1 + news_adjusted_expected_return)
    news_adjusted_mispricing_pct = (
        actual_stock_price - news_adjusted_fair_price
    ) / news_adjusted_fair_price

    return {
        "previous_trading_day": previous_trading_day,
        "next_trading_day": next_trading_day,
        "price_interval": price_interval,
        "previous_stock_price": previous_stock_price,
        "actual_stock_price": actual_stock_price,
        "previous_market_price": previous_market_price,
        "actual_market_price": actual_market_price,
        "beta": beta,
        "market_return_event_window": market_return_event_window,
        "capm_fair_return": capm_fair_return,
        "capm_fair_price": capm_fair_price,
        "severity_move": severity_move,
        "signed_expected_move": signed_expected_move,
        "news_adjusted_expected_return": news_adjusted_expected_return,
        "news_adjusted_fair_price": news_adjusted_fair_price,
        "news_adjusted_mispricing_pct": news_adjusted_mispricing_pct,
    }


def process_all_events(
    event_path: Path = DEFAULT_EVENT_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    market_ticker: str = MARKET_TICKER,
    rolling_window: int = ROLLING_WINDOW,
    risk_free_daily: float = RISK_FREE_DAILY,
    verbose: bool = False,
    price_interval: str = "1d",
) -> pd.DataFrame:
    """Process every input event into one output row with CAPM/news mispricing fields."""
    events = load_events(event_path)
    price_cache: Dict[str, pd.DataFrame] = {}
    price_interval = price_interval.lower()

    valid_timestamps = events["_event_timestamp"].dropna()
    if valid_timestamps.empty:
        download_start = pd.Timestamp.today().normalize() - pd.Timedelta(days=500)
        download_end = pd.Timestamp.today().normalize() + pd.Timedelta(days=10)
    else:
        min_event_date = valid_timestamps.min().tz_convert(None).normalize()
        max_event_date = valid_timestamps.max().tz_convert(None).normalize()
        if is_intraday_interval(price_interval):
            # Yahoo intraday history is limited. Use recent data only, which is
            # enough for minute/hour event windows and the prior-bar beta window.
            intraday_floor = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=58)
            download_start = max(min_event_date - pd.Timedelta(days=10), intraday_floor)
        else:
            # Calendar-day buffer covers the 252-trading-day beta window plus holidays.
            download_start = min_event_date - pd.Timedelta(days=500)
        download_end = max_event_date + pd.Timedelta(days=10)

    market_prices = get_price_data(
        market_ticker,
        download_start,
        download_end,
        price_cache,
        price_interval=price_interval,
    )

    result_rows = []
    total_events = len(events)
    for idx, event in events.iterrows():
        row_num = len(result_rows) + 1
        ticker = event.get("_ticker_normalized", "")
        yahoo_ticker = event.get("yahoo_ticker", "")
        event_id = event.get("event_id", "")
        timestamp = event.get("timestamp", "")
        if verbose:
            print(
                f"[capm] start row {row_num}/{total_events} "
                f"event_id={event_id} ticker={ticker} yahoo_ticker={yahoo_ticker} "
                f"timestamp={timestamp} interval={price_interval}",
                flush=True,
            )

        stock_prices = get_price_data(
            ticker,
            download_start,
            download_end,
            price_cache,
            price_interval=price_interval,
        )

        computed_fields = compute_event_mispricing(
            event,
            stock_prices,
            market_prices,
            rolling_window=rolling_window,
            risk_free_daily=risk_free_daily,
            price_interval=price_interval,
        )

        # Preserve all original CSV columns and append computed event-level fields.
        result = event.drop(
            labels=[
                "_event_timestamp",
                "_ticker_normalized",
                "_label_direction_numeric",
                "_label_severity_numeric",
            ],
            errors="ignore",
        ).to_dict()
        result.update(computed_fields)
        result_rows.append(result)

        if verbose:
            print(
                f"[capm] finished row {row_num}/{total_events} "
                f"prev={computed_fields['previous_trading_day']} "
                f"next={computed_fields['next_trading_day']} "
                f"beta={computed_fields['beta']} "
                f"mispricing={computed_fields['news_adjusted_mispricing_pct']}",
                flush=True,
            )

    output = pd.DataFrame(result_rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False)
    return output


def main() -> None:
    """Run the event-level CAPM/news mispricing preparation stage."""
    input_rows = len(pd.read_csv(DEFAULT_EVENT_PATH))
    output = process_all_events()

    print(f"Input rows: {input_rows}")
    print(f"Output rows: {len(output)}")
    assert input_rows == len(output), "Output row count must equal input event row count."
    print(output.head(10))
    print(f"Saved event-level CAPM news-adjusted mispricing data to {DEFAULT_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
