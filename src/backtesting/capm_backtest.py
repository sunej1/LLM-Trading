"""Run CAPM/news fair-value backtest on an already-labeled backtest file.

Trade timing:
- reference bar: last bar before the news timestamp, used only for fair value
- entry bar: first bar after the news timestamp
- exit bar: first later bar that hits fair value, stop loss, or max hold
"""
from __future__ import annotations

import argparse
import itertools
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.modeling.capm_news_mispricing import (
    MARKET_TICKER,
    RISK_FREE_DAILY,
    ROLLING_WINDOW,
    SEVERITY_MOVE_MAP,
    estimate_event_beta,
    find_previous_next_trading_days,
    get_price_data,
    is_intraday_interval,
    load_events,
)


DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "training" / "backtest_labeled.csv"
DEFAULT_ENRICHED_PATH = PROJECT_ROOT / "data" / "training" / "backtest_event_mispricing.csv"
DEFAULT_RESULTS_PATH = PROJECT_ROOT / "data" / "training" / "backtest_event_results.csv"
DEFAULT_SUMMARY_PATH = PROJECT_ROOT / "data" / "training" / "backtest_summary.csv"
DEFAULT_TUNING_PATH = PROJECT_ROOT / "data" / "training" / "backtest_param_tuning.csv"
DEFAULT_PRICE_INTERVAL = "15m"
DEFAULT_STOP_LOSS_PCT = 0.05
DEFAULT_MAX_HOLD_BARS = 52
DEFAULT_MIN_EDGE_PCT = 0.001
DEFAULT_MAX_TARGET_DISTANCE_PCT = 0.10
DEFAULT_CV_FOLDS = 5
DEFAULT_MIN_CV_TRADES = 5

STOP_LOSS_GRID = [0.01, 0.015, 0.02, 0.03, 0.05]
MAX_HOLD_BARS_GRID = [4, 8, 13, 26, 52]
MIN_EDGE_PCT_GRID = [0.001, 0.003, 0.005, 0.01]
MAX_TARGET_DISTANCE_PCT_GRID = [0.05, 0.10, 0.15, 0.25]

DIRECTION_MAP = {
    "positive": 1,
    "neutral": 0,
    "mixed": 0,
    "negative": -1,
}


def load_backtest_events(input_path: Path = DEFAULT_INPUT_PATH) -> pd.DataFrame:
    """Read the labeled held-out backtest CSV."""
    return pd.read_csv(input_path)


def count_labeled_rows(events: pd.DataFrame) -> int:
    """Count rows that have both severity and direction labels."""
    required_columns = {"label_severity", "label_direction"}
    if not required_columns.issubset(events.columns):
        return 0

    severity_present = events["label_severity"].notna() & (
        events["label_severity"].astype(str).str.strip() != ""
    )
    direction_present = events["label_direction"].notna() & (
        events["label_direction"].astype(str).str.strip() != ""
    )
    return int((severity_present & direction_present).sum())


def _direction_to_numeric(value: object) -> float:
    """Convert label_direction into numeric event direction."""
    if pd.isna(value):
        return np.nan

    text = str(value).strip().lower()
    if text in DIRECTION_MAP:
        return float(DIRECTION_MAP[text])

    try:
        numeric = float(text)
    except ValueError:
        return np.nan

    if numeric > 0:
        return 1.0
    if numeric < 0:
        return -1.0
    return 0.0


def _price_on_bar(prices: pd.DataFrame, bar_time: Optional[pd.Timestamp]) -> float:
    """Return close price for one bar timestamp."""
    if bar_time is None or prices.empty:
        return np.nan
    values = prices.loc[prices["date"] == bar_time, "price"]
    if values.empty:
        return np.nan
    return float(values.iloc[0])


def _future_bars_after(prices: pd.DataFrame, entry_bar: Optional[pd.Timestamp], max_hold_bars: int) -> pd.DataFrame:
    """Return bars strictly after entry, capped at max holding period."""
    if entry_bar is None or prices.empty:
        return pd.DataFrame(columns=prices.columns)
    return prices.loc[prices["date"] > entry_bar].sort_values("date").head(max_hold_bars)


def _skipped_trade_result(price_interval: str, reason: str) -> Dict[str, object]:
    """Return a row-shaped result for events that cannot be priced."""
    return {
        "reference_bar": pd.NaT,
        "entry_bar": pd.NaT,
        "exit_bar": pd.NaT,
        "price_interval": price_interval,
        "reference_stock_price": np.nan,
        "entry_price": np.nan,
        "exit_price": np.nan,
        "reference_market_price": np.nan,
        "entry_market_price": np.nan,
        "beta": np.nan,
        "market_return_to_entry": np.nan,
        "capm_fair_return": np.nan,
        "capm_fair_price": np.nan,
        "severity_move": np.nan,
        "signed_expected_move": np.nan,
        "news_adjusted_expected_return": np.nan,
        "news_adjusted_fair_price": np.nan,
        "entry_mispricing_pct": np.nan,
        "signal": 0.0,
        "trade_side": "skipped",
        "signal_reason": reason,
        "exit_reason": reason,
        "bars_held": 0,
        "stop_loss_pct": np.nan,
        "max_hold_bars": np.nan,
        "min_edge_pct": np.nan,
        "max_target_distance_pct": np.nan,
        "stock_return_trade_window": np.nan,
        "strategy_return": np.nan,
    }


def _choose_signal(
    entry_price: float,
    fair_price: float,
    signed_expected_move: float,
    min_edge_pct: float,
    max_target_distance_pct: float,
) -> Tuple[float, str]:
    """Choose trade direction from entry versus CAPM/news fair value."""
    if any(np.isnan(x) for x in [entry_price, fair_price, signed_expected_move]):
        return 0.0, "missing_inputs"
    if signed_expected_move == 0:
        return 0.0, "neutral_news"

    target_distance_pct = (fair_price / entry_price) - 1
    if abs(target_distance_pct) < min_edge_pct:
        return 0.0, "edge_too_small"
    if abs(target_distance_pct) > max_target_distance_pct:
        return 0.0, "target_too_far"
    if target_distance_pct > 0:
        return 1.0, "fair_value_above_entry"
    return -1.0, "fair_value_below_entry"


def _simulate_exit(
    signal: float,
    entry_price: float,
    fair_price: float,
    future_bars: pd.DataFrame,
    stop_loss_pct: float,
) -> Dict[str, object]:
    """Exit at fair value, stop loss, or max-hold expiry."""
    if signal == 0 or np.isnan(entry_price) or np.isnan(fair_price):
        return {"exit_bar": pd.NaT, "exit_price": np.nan, "exit_reason": "no_trade", "bars_held": 0}
    if future_bars.empty:
        return {"exit_bar": pd.NaT, "exit_price": np.nan, "exit_reason": "missing_future_bars", "bars_held": 0}

    if signal > 0:
        stop_price = entry_price * (1 - stop_loss_pct)
        for bars_held, row in enumerate(future_bars.itertuples(index=False), start=1):
            price = float(row.price)
            if price <= stop_price:
                return {"exit_bar": row.date, "exit_price": price, "exit_reason": "stop_loss", "bars_held": bars_held}
            if price >= fair_price:
                return {"exit_bar": row.date, "exit_price": price, "exit_reason": "fair_value_hit", "bars_held": bars_held}
    else:
        stop_price = entry_price * (1 + stop_loss_pct)
        for bars_held, row in enumerate(future_bars.itertuples(index=False), start=1):
            price = float(row.price)
            if price >= stop_price:
                return {"exit_bar": row.date, "exit_price": price, "exit_reason": "stop_loss", "bars_held": bars_held}
            if price <= fair_price:
                return {"exit_bar": row.date, "exit_price": price, "exit_reason": "fair_value_hit", "bars_held": bars_held}

    final = future_bars.iloc[-1]
    return {
        "exit_bar": final["date"],
        "exit_price": float(final["price"]),
        "exit_reason": "max_hold",
        "bars_held": len(future_bars),
    }


def _download_window(events: pd.DataFrame, price_interval: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Choose Yahoo download start/end for the event timestamps."""
    valid_timestamps = events["_event_timestamp"].dropna()
    if valid_timestamps.empty:
        now = pd.Timestamp.utcnow().tz_localize(None).normalize()
        return now - pd.Timedelta(days=58), now + pd.Timedelta(days=2)

    min_event_date = valid_timestamps.min().tz_convert(None).normalize()
    max_event_date = valid_timestamps.max().tz_convert(None).normalize()
    if is_intraday_interval(price_interval):
        intraday_floor = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=58)
        start = max(min_event_date - pd.Timedelta(days=10), intraday_floor)
    else:
        start = min_event_date - pd.Timedelta(days=500)
    return start, max_event_date + pd.Timedelta(days=10)


def compute_event_trade(
    event: pd.Series,
    stock_prices: pd.DataFrame,
    market_prices: pd.DataFrame,
    price_interval: str,
    stop_loss_pct: float,
    max_hold_bars: int,
    min_edge_pct: float,
    max_target_distance_pct: float,
) -> Dict[str, object]:
    """Compute fair value, post-news entry, safeguarded exit, and return."""
    if is_intraday_interval(price_interval):
        if market_prices.empty:
            return _skipped_trade_result(price_interval, "missing_intraday_market_data")
        if stock_prices.empty:
            return _skipped_trade_result(price_interval, "missing_intraday_stock_data")

    reference_bar, entry_bar = find_previous_next_trading_days(
        event["_event_timestamp"],
        stock_prices,
        market_prices,
        price_interval=price_interval,
    )

    if reference_bar is None or entry_bar is None or pd.isna(reference_bar) or pd.isna(entry_bar):
        reason = "missing_intraday_bars" if is_intraday_interval(price_interval) else "missing_price_bars"
        return _skipped_trade_result(price_interval, reason)

    reference_stock_price = _price_on_bar(stock_prices, reference_bar)
    entry_price = _price_on_bar(stock_prices, entry_bar)
    reference_market_price = _price_on_bar(market_prices, reference_bar)
    entry_market_price = _price_on_bar(market_prices, entry_bar)

    beta = estimate_event_beta(
        stock_prices,
        market_prices,
        reference_bar,
        rolling_window=ROLLING_WINDOW,
        risk_free_daily=RISK_FREE_DAILY,
    )

    if reference_market_price and not np.isnan(reference_market_price):
        market_return_to_entry = (entry_market_price / reference_market_price) - 1
    else:
        market_return_to_entry = np.nan

    capm_fair_return = beta * market_return_to_entry
    severity_move = SEVERITY_MOVE_MAP.get(event["_label_severity_numeric"], np.nan)
    direction = _direction_to_numeric(event.get("label_direction"))
    signed_expected_move = direction * severity_move
    news_adjusted_expected_return = capm_fair_return + signed_expected_move

    # Fair value is anchored to the last bar before news, not to the trade entry.
    capm_fair_price = reference_stock_price * (1 + capm_fair_return)
    news_adjusted_fair_price = reference_stock_price * (1 + news_adjusted_expected_return)

    signal, signal_reason = _choose_signal(
        entry_price=entry_price,
        fair_price=news_adjusted_fair_price,
        signed_expected_move=signed_expected_move,
        min_edge_pct=min_edge_pct,
        max_target_distance_pct=max_target_distance_pct,
    )
    future_bars = _future_bars_after(stock_prices, entry_bar, max_hold_bars)
    exit_fields = _simulate_exit(
        signal=signal,
        entry_price=entry_price,
        fair_price=news_adjusted_fair_price,
        future_bars=future_bars,
        stop_loss_pct=stop_loss_pct,
    )

    exit_price = exit_fields["exit_price"]
    if signal == 0:
        stock_return_trade_window = np.nan
        strategy_return = 0.0
    elif np.isnan(entry_price) or np.isnan(exit_price):
        stock_return_trade_window = np.nan
        strategy_return = np.nan
    else:
        stock_return_trade_window = (exit_price / entry_price) - 1
        strategy_return = signal * stock_return_trade_window

    if not np.isnan(entry_price) and not np.isnan(news_adjusted_fair_price):
        entry_mispricing_pct = (entry_price - news_adjusted_fair_price) / news_adjusted_fair_price
    else:
        entry_mispricing_pct = np.nan

    return {
        "reference_bar": reference_bar,
        "entry_bar": entry_bar,
        "exit_bar": exit_fields["exit_bar"],
        "price_interval": price_interval,
        "reference_stock_price": reference_stock_price,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "reference_market_price": reference_market_price,
        "entry_market_price": entry_market_price,
        "beta": beta,
        "market_return_to_entry": market_return_to_entry,
        "capm_fair_return": capm_fair_return,
        "capm_fair_price": capm_fair_price,
        "severity_move": severity_move,
        "signed_expected_move": signed_expected_move,
        "news_adjusted_expected_return": news_adjusted_expected_return,
        "news_adjusted_fair_price": news_adjusted_fair_price,
        "entry_mispricing_pct": entry_mispricing_pct,
        "signal": signal,
        "trade_side": {1.0: "long", -1.0: "short", 0.0: "flat"}.get(signal, "flat"),
        "signal_reason": signal_reason,
        "exit_reason": exit_fields["exit_reason"],
        "bars_held": exit_fields["bars_held"],
        "stop_loss_pct": stop_loss_pct,
        "max_hold_bars": max_hold_bars,
        "min_edge_pct": min_edge_pct,
        "max_target_distance_pct": max_target_distance_pct,
        "stock_return_trade_window": stock_return_trade_window,
        "strategy_return": strategy_return,
    }


def _event_to_result_row(event: pd.Series, computed: Dict[str, object]) -> Dict[str, object]:
    """Combine one original event row with computed backtest fields."""
    result = event.drop(
        labels=[
            "_event_timestamp",
            "_ticker_normalized",
            "_label_direction_numeric",
            "_label_severity_numeric",
        ],
        errors="ignore",
    ).to_dict()
    result.update(computed)
    return result


def _download_prices_for_events(
    events: pd.DataFrame,
    price_interval: str,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    """Download market and ticker prices once for a backtest/tuning run."""
    price_cache: Dict[str, pd.DataFrame] = {}
    download_start, download_end = _download_window(events, price_interval)
    market_prices = get_price_data(
        MARKET_TICKER,
        download_start,
        download_end,
        price_cache,
        price_interval=price_interval,
    )

    for ticker in sorted(events["_ticker_normalized"].dropna().unique()):
        get_price_data(
            ticker,
            download_start,
            download_end,
            price_cache,
            price_interval=price_interval,
        )

    return price_cache, market_prices, download_start, download_end


def _results_from_cached_prices(
    events: pd.DataFrame,
    price_interval: str,
    stop_loss_pct: float,
    max_hold_bars: int,
    min_edge_pct: float,
    max_target_distance_pct: float,
    price_cache: Dict[str, pd.DataFrame],
    market_prices: pd.DataFrame,
    verbose: bool,
) -> pd.DataFrame:
    """Run event computations using already-downloaded Yahoo data."""
    rows = []
    for position, (_, event) in enumerate(events.iterrows(), start=1):
        ticker = event.get("_ticker_normalized", "")
        if verbose:
            print(
                f"[capm] start row {position}/{len(events)} "
                f"event_id={event.get('event_id', '')} ticker={ticker} interval={price_interval}",
                flush=True,
            )

        stock_prices = price_cache.get(
            f"{ticker}|{price_interval}",
            pd.DataFrame(columns=["date", "price"]),
        )
        computed = compute_event_trade(
            event=event,
            stock_prices=stock_prices,
            market_prices=market_prices,
            price_interval=price_interval,
            stop_loss_pct=stop_loss_pct,
            max_hold_bars=max_hold_bars,
            min_edge_pct=min_edge_pct,
            max_target_distance_pct=max_target_distance_pct,
        )
        rows.append(_event_to_result_row(event, computed))

        if verbose:
            print(
                f"[capm] finished row {position}/{len(events)} "
                f"side={computed['trade_side']} entry={computed['entry_bar']} "
                f"exit={computed['exit_bar']} reason={computed['exit_reason']} "
                f"return={computed['strategy_return']}",
                flush=True,
            )

    results = pd.DataFrame(rows)
    valid_strategy_returns = results["strategy_return"].fillna(0.0)
    results["cumulative_strategy_return"] = (1 + valid_strategy_returns).cumprod() - 1
    return results


def run_event_fair_value_backtest(
    events: pd.DataFrame,
    price_interval: str,
    stop_loss_pct: float,
    max_hold_bars: int,
    min_edge_pct: float,
    max_target_distance_pct: float,
    verbose: bool = True,
) -> pd.DataFrame:
    """Run the fair-value target/stop-loss backtest for every event."""
    price_cache, market_prices, _, _ = _download_prices_for_events(events, price_interval)
    return _results_from_cached_prices(
        events=events,
        price_interval=price_interval,
        stop_loss_pct=stop_loss_pct,
        max_hold_bars=max_hold_bars,
        min_edge_pct=min_edge_pct,
        max_target_distance_pct=max_target_distance_pct,
        price_cache=price_cache,
        market_prices=market_prices,
        verbose=verbose,
    )


def summarize_backtest(results: pd.DataFrame) -> pd.DataFrame:
    """Create a one-row summary of event backtest performance."""
    trade_mask = (results["signal"] != 0) & results["strategy_return"].notna()
    trades = results.loc[trade_mask]
    labeled_rows = (
        results["label_direction"].notna()
        & (results["label_direction"].astype(str).str.strip() != "")
    )
    priced_rows = results["entry_price"].notna()
    skipped_rows = (results["trade_side"] == "skipped")

    if trades.empty:
        summary: Dict[str, float] = {
            "input_rows": float(len(results)),
            "labeled_rows": float(labeled_rows.sum()),
            "priced_rows": float(priced_rows.sum()),
            "skipped_rows": float(skipped_rows.sum()),
            "trades": 0.0,
            "flat_or_unusable_rows": float(len(results)),
            "win_rate": np.nan,
            "average_strategy_return": np.nan,
            "median_strategy_return": np.nan,
            "total_strategy_return": 0.0,
            "fair_value_exits": 0.0,
            "stop_loss_exits": 0.0,
            "max_hold_exits": 0.0,
        }
    else:
        summary = {
            "input_rows": float(len(results)),
            "labeled_rows": float(labeled_rows.sum()),
            "priced_rows": float(priced_rows.sum()),
            "skipped_rows": float(skipped_rows.sum()),
            "trades": float(len(trades)),
            "flat_or_unusable_rows": float(len(results) - len(trades)),
            "win_rate": float((trades["strategy_return"] > 0).mean()),
            "average_strategy_return": float(trades["strategy_return"].mean()),
            "median_strategy_return": float(trades["strategy_return"].median()),
            "total_strategy_return": float((1 + trades["strategy_return"]).prod() - 1),
            "fair_value_exits": float((trades["exit_reason"] == "fair_value_hit").sum()),
            "stop_loss_exits": float((trades["exit_reason"] == "stop_loss").sum()),
            "max_hold_exits": float((trades["exit_reason"] == "max_hold").sum()),
        }
    return pd.DataFrame([summary])


def _chronological_folds(events: pd.DataFrame, k_folds: int) -> List[pd.DataFrame]:
    """Split events into chronological validation folds."""
    sorted_events = events.sort_values("_event_timestamp").reset_index(drop=True)
    fold_indices = np.array_split(np.arange(len(sorted_events)), k_folds)
    return [sorted_events.iloc[indices].copy() for indices in fold_indices if len(indices) > 0]


def _parameter_grid() -> Sequence[Tuple[float, int, float, float]]:
    """Build the grid of trade parameters to cross-validate."""
    return list(
        itertools.product(
            STOP_LOSS_GRID,
            MAX_HOLD_BARS_GRID,
            MIN_EDGE_PCT_GRID,
            MAX_TARGET_DISTANCE_PCT_GRID,
        )
    )


def _validation_metrics(results: pd.DataFrame) -> Dict[str, float]:
    """Calculate validation metrics for one fold."""
    trade_mask = (results["signal"] != 0) & results["strategy_return"].notna()
    trades = results.loc[trade_mask]
    if trades.empty:
        return {
            "fold_return": 0.0,
            "fold_trades": 0.0,
            "fold_win_rate": np.nan,
            "fold_avg_return": np.nan,
        }

    return {
        "fold_return": float((1 + trades["strategy_return"]).prod() - 1),
        "fold_trades": float(len(trades)),
        "fold_win_rate": float((trades["strategy_return"] > 0).mean()),
        "fold_avg_return": float(trades["strategy_return"].mean()),
    }


def tune_trade_parameters(
    events: pd.DataFrame,
    price_interval: str,
    k_folds: int = DEFAULT_CV_FOLDS,
    min_cv_trades: int = DEFAULT_MIN_CV_TRADES,
) -> pd.DataFrame:
    """Use chronological k-fold validation to rank trade parameter candidates."""
    if len(events) < k_folds:
        raise ValueError(f"Need at least {k_folds} rows for {k_folds}-fold validation.")

    print("Downloading price data once for parameter tuning...", flush=True)
    price_cache, market_prices, _, _ = _download_prices_for_events(events, price_interval)
    folds = _chronological_folds(events, k_folds)
    tuning_rows = []
    candidates = _parameter_grid()

    for candidate_idx, (
        stop_loss_pct,
        max_hold_bars,
        min_edge_pct,
        max_target_distance_pct,
    ) in enumerate(candidates, start=1):
        fold_returns = []
        fold_trades = []
        fold_win_rates = []
        fold_avg_returns = []

        for fold_events in folds:
            fold_results = _results_from_cached_prices(
                events=fold_events,
                price_interval=price_interval,
                stop_loss_pct=stop_loss_pct,
                max_hold_bars=max_hold_bars,
                min_edge_pct=min_edge_pct,
                max_target_distance_pct=max_target_distance_pct,
                price_cache=price_cache,
                market_prices=market_prices,
                verbose=False,
            )
            metrics = _validation_metrics(fold_results)
            fold_returns.append(metrics["fold_return"])
            fold_trades.append(metrics["fold_trades"])
            fold_win_rates.append(metrics["fold_win_rate"])
            fold_avg_returns.append(metrics["fold_avg_return"])

        total_validation_trades = float(np.nansum(fold_trades))
        mean_fold_return = float(np.nanmean(fold_returns))
        std_fold_return = float(np.nanstd(fold_returns))
        score = mean_fold_return - (0.5 * std_fold_return)
        if total_validation_trades < min_cv_trades:
            score = -np.inf

        tuning_rows.append(
            {
                "stop_loss_pct": stop_loss_pct,
                "max_hold_bars": max_hold_bars,
                "min_edge_pct": min_edge_pct,
                "max_target_distance_pct": max_target_distance_pct,
                "cv_folds": k_folds,
                "validation_trades": total_validation_trades,
                "mean_fold_return": mean_fold_return,
                "std_fold_return": std_fold_return,
                "score": score,
                "mean_fold_win_rate": float(np.nanmean(fold_win_rates)),
                "mean_trade_return": float(np.nanmean(fold_avg_returns)),
            }
        )

        print(
            f"[tune] {candidate_idx}/{len(candidates)} "
            f"stop={stop_loss_pct} hold={max_hold_bars} min_edge={min_edge_pct} "
            f"max_target={max_target_distance_pct} score={score}",
            flush=True,
        )

    return pd.DataFrame(tuning_rows).sort_values(
        ["score", "mean_fold_return", "validation_trades"],
        ascending=[False, False, False],
    )


def run_parameter_tuning(
    input_path: Path = DEFAULT_INPUT_PATH,
    tuning_path: Path = DEFAULT_TUNING_PATH,
    price_interval: str = DEFAULT_PRICE_INTERVAL,
    k_folds: int = DEFAULT_CV_FOLDS,
    min_cv_trades: int = DEFAULT_MIN_CV_TRADES,
) -> pd.DataFrame:
    """Load events, run parameter tuning, save ranked candidates."""
    raw_events = load_backtest_events(input_path)
    labeled_rows = count_labeled_rows(raw_events)
    print(f"Tuning source rows: {len(raw_events)}")
    print(f"Tuning labeled rows: {labeled_rows}")
    if labeled_rows == 0:
        raise ValueError(
            "Backtest input has no labeled rows. Run "
            ".venv/bin/python src/backtesting/label_backtest_events.py first."
        )

    events = load_events(input_path)
    tuning_results = tune_trade_parameters(
        events=events,
        price_interval=price_interval.lower(),
        k_folds=k_folds,
        min_cv_trades=min_cv_trades,
    )
    tuning_path.parent.mkdir(parents=True, exist_ok=True)
    tuning_results.to_csv(tuning_path, index=False)

    best = tuning_results.iloc[0]
    print(f"Tuning results written: {tuning_path}")
    print("Best cross-validated parameters:")
    print(best.to_string())
    print("Top 10 candidates:")
    print(tuning_results.head(10).to_string(index=False))
    return tuning_results


def print_results_preview(results: pd.DataFrame, rows: int = 10) -> None:
    """Print a compact preview without dumping full article text."""
    preview_columns = [
        "event_id",
        "timestamp",
        "ticker",
        "label_severity",
        "label_direction",
        "reference_bar",
        "entry_bar",
        "exit_bar",
        "trade_side",
        "signal_reason",
        "exit_reason",
        "bars_held",
        "entry_price",
        "news_adjusted_fair_price",
        "exit_price",
        "strategy_return",
    ]
    available_columns = [col for col in preview_columns if col in results.columns]
    print(results[available_columns].head(rows).to_string(index=False))


def run_capm_backtest(
    input_path: Path = DEFAULT_INPUT_PATH,
    enriched_path: Path = DEFAULT_ENRICHED_PATH,
    results_path: Path = DEFAULT_RESULTS_PATH,
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    price_interval: str = DEFAULT_PRICE_INTERVAL,
    stop_loss_pct: float = DEFAULT_STOP_LOSS_PCT,
    max_hold_bars: int = DEFAULT_MAX_HOLD_BARS,
    min_edge_pct: float = DEFAULT_MIN_EDGE_PCT,
    max_target_distance_pct: float = DEFAULT_MAX_TARGET_DISTANCE_PCT,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run post-news-entry CAPM/news fair-value backtest on labeled events."""
    raw_events = load_backtest_events(input_path)
    input_rows = len(raw_events)
    labeled_rows = count_labeled_rows(raw_events)
    print(f"Backtest source rows: {input_rows}")
    print(f"Backtest labeled rows: {labeled_rows}")
    if labeled_rows == 0:
        raise ValueError(
            "Backtest input has no labeled rows. Run "
            ".venv/bin/python src/backtesting/label_backtest_events.py first."
        )
    if labeled_rows < input_rows:
        print(
            f"Warning: only {labeled_rows}/{input_rows} backtest rows are labeled; "
            "unlabeled rows will be treated as flat.",
            flush=True,
        )

    events = load_events(input_path)
    results = run_event_fair_value_backtest(
        events=events,
        price_interval=price_interval.lower(),
        stop_loss_pct=stop_loss_pct,
        max_hold_bars=max_hold_bars,
        min_edge_pct=min_edge_pct,
        max_target_distance_pct=max_target_distance_pct,
    )
    summary = summarize_backtest(results)

    results_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(enriched_path, index=False)
    results.to_csv(results_path, index=False)
    summary.to_csv(summary_path, index=False)

    print(f"Input rows: {input_rows}")
    print(f"Output rows: {len(results)}")
    assert input_rows == len(results), "Backtest output row count must equal input row count."
    print(f"Backtest enriched rows written: {enriched_path}")
    print(f"Backtest result rows written: {results_path}")
    print(f"Backtest summary written: {summary_path}")
    print(summary.to_string(index=False))
    print_results_preview(results)
    return results, summary


def main() -> None:
    """CLI entry point for CAPM-only backtesting."""
    parser = argparse.ArgumentParser(description="Run CAPM event-level news backtest.")
    parser.add_argument("--in", dest="input_path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--enriched-out", default=str(DEFAULT_ENRICHED_PATH))
    parser.add_argument("--results-out", default=str(DEFAULT_RESULTS_PATH))
    parser.add_argument("--summary-out", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--tuning-out", default=str(DEFAULT_TUNING_PATH))
    parser.add_argument(
        "--price-interval",
        default=DEFAULT_PRICE_INTERVAL,
        help="Yahoo price interval: 1d, 1h, 60m, 30m, 15m, 5m, or 1m.",
    )
    parser.add_argument("--stop-loss-pct", type=float, default=DEFAULT_STOP_LOSS_PCT)
    parser.add_argument("--max-hold-bars", type=int, default=DEFAULT_MAX_HOLD_BARS)
    parser.add_argument("--min-edge-pct", type=float, default=DEFAULT_MIN_EDGE_PCT)
    parser.add_argument(
        "--max-target-distance-pct",
        type=float,
        default=DEFAULT_MAX_TARGET_DISTANCE_PCT,
    )
    parser.add_argument(
        "--tune-params",
        action="store_true",
        help="Run k-fold parameter tuning instead of the normal backtest.",
    )
    parser.add_argument("--cv-folds", type=int, default=DEFAULT_CV_FOLDS)
    parser.add_argument("--min-cv-trades", type=int, default=DEFAULT_MIN_CV_TRADES)
    args = parser.parse_args()

    if args.tune_params:
        run_parameter_tuning(
            input_path=Path(args.input_path),
            tuning_path=Path(args.tuning_out),
            price_interval=args.price_interval,
            k_folds=args.cv_folds,
            min_cv_trades=args.min_cv_trades,
        )
        return

    run_capm_backtest(
        input_path=Path(args.input_path),
        enriched_path=Path(args.enriched_out),
        results_path=Path(args.results_out),
        summary_path=Path(args.summary_out),
        price_interval=args.price_interval,
        stop_loss_pct=args.stop_loss_pct,
        max_hold_bars=args.max_hold_bars,
        min_edge_pct=args.min_edge_pct,
        max_target_distance_pct=args.max_target_distance_pct,
    )


if __name__ == "__main__":
    main()
