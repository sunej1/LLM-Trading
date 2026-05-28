"""Event-level backtest for labeled news events.

The backtest uses the held-out event file in data/training/backtest.csv.
Each input row is preserved. Rows with missing/neutral labels become flat
trades, while positive labels go long and negative labels go short over the
previous-trading-day close to next-trading-day close event window.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.modeling.capm_news_mispricing import process_all_events
from src.llm.label_csv_stage_a import label_csv


DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "training" / "backtest.csv"
DEFAULT_LABELED_INPUT_PATH = PROJECT_ROOT / "data" / "training" / "backtest_labeled.csv"
DEFAULT_ENRICHED_PATH = PROJECT_ROOT / "data" / "training" / "backtest_event_mispricing.csv"
DEFAULT_RESULTS_PATH = PROJECT_ROOT / "data" / "training" / "backtest_event_results.csv"
DEFAULT_SUMMARY_PATH = PROJECT_ROOT / "data" / "training" / "backtest_summary.csv"


DIRECTION_MAP = {
    "positive": 1,
    "neutral": 0,
    "mixed": 0,
    "negative": -1,
}


def load_backtest_events(input_path: Path = DEFAULT_INPUT_PATH) -> pd.DataFrame:
    """Read the held-out backtest event CSV without modifying it."""
    return pd.read_csv(input_path)


def _direction_to_signal(value: object) -> float:
    """Convert label_direction into a trading direction: long, flat, or short."""
    if pd.isna(value):
        return 0.0

    text = str(value).strip().lower()
    if text in DIRECTION_MAP:
        return float(DIRECTION_MAP[text])

    try:
        numeric = float(text)
    except ValueError:
        return 0.0

    if numeric > 0:
        return 1.0
    if numeric < 0:
        return -1.0
    return 0.0


def add_backtest_columns(event_df: pd.DataFrame) -> pd.DataFrame:
    """Add signal and event-window return columns to CAPM-enriched events."""
    results = event_df.copy()

    results["signal"] = results["label_direction"].apply(_direction_to_signal)
    results["trade_side"] = results["signal"].map({1.0: "long", -1.0: "short", 0.0: "flat"})

    # Realized stock return from previous close to first close after the event.
    results["stock_return_event_window"] = (
        results["actual_stock_price"] / results["previous_stock_price"]
    ) - 1

    # Strategy return: long earns stock return, short earns the inverse, flat earns zero.
    results["strategy_return"] = results["signal"] * results["stock_return_event_window"]
    results.loc[results["signal"] == 0, "strategy_return"] = 0.0

    # CAPM abnormal return over the same event window.
    results["capm_abnormal_return"] = (
        results["stock_return_event_window"] - results["capm_fair_return"]
    )
    results["strategy_abnormal_return"] = results["signal"] * results["capm_abnormal_return"]
    results.loc[results["signal"] == 0, "strategy_abnormal_return"] = 0.0

    # Missing prices/betas should keep the row but not count as a realized trade return.
    missing_return = results["stock_return_event_window"].isna()
    results.loc[missing_return, ["strategy_return", "strategy_abnormal_return"]] = np.nan

    valid_strategy_returns = results["strategy_return"].fillna(0.0)
    results["cumulative_strategy_return"] = (1 + valid_strategy_returns).cumprod() - 1
    return results


def summarize_backtest(results: pd.DataFrame) -> pd.DataFrame:
    """Create a one-row summary of event backtest performance."""
    trade_mask = (results["signal"] != 0) & results["strategy_return"].notna()
    trades = results.loc[trade_mask]
    labeled_rows = (
        results["label_direction"].notna()
        & (results["label_direction"].astype(str).str.strip() != "")
    )
    priced_rows = results["stock_return_event_window"].notna()

    if trades.empty:
        summary: Dict[str, float] = {
            "input_rows": float(len(results)),
            "labeled_rows": float(labeled_rows.sum()),
            "priced_rows": float(priced_rows.sum()),
            "trades": 0.0,
            "flat_or_unusable_rows": float(len(results)),
            "win_rate": np.nan,
            "average_strategy_return": np.nan,
            "median_strategy_return": np.nan,
            "total_strategy_return": 0.0,
            "average_strategy_abnormal_return": np.nan,
        }
    else:
        summary = {
            "input_rows": float(len(results)),
            "labeled_rows": float(labeled_rows.sum()),
            "priced_rows": float(priced_rows.sum()),
            "trades": float(len(trades)),
            "flat_or_unusable_rows": float(len(results) - len(trades)),
            "win_rate": float((trades["strategy_return"] > 0).mean()),
            "average_strategy_return": float(trades["strategy_return"].mean()),
            "median_strategy_return": float(trades["strategy_return"].median()),
            "total_strategy_return": float((1 + trades["strategy_return"]).prod() - 1),
            "average_strategy_abnormal_return": float(
                trades["strategy_abnormal_return"].mean()
            ),
        }

    return pd.DataFrame([summary])


def print_results_preview(results: pd.DataFrame, rows: int = 10) -> None:
    """Print a compact preview without dumping full article text."""
    preview_columns = [
        "event_id",
        "timestamp",
        "ticker",
        "label_severity",
        "label_direction",
        "previous_trading_day",
        "next_trading_day",
        "signal",
        "trade_side",
        "stock_return_event_window",
        "strategy_return",
        "capm_abnormal_return",
    ]
    available_columns = [col for col in preview_columns if col in results.columns]
    print(results[available_columns].head(rows).to_string(index=False))


def run_backtest(
    input_path: Path = DEFAULT_INPUT_PATH,
    labeled_input_path: Path = DEFAULT_LABELED_INPUT_PATH,
    enriched_path: Path = DEFAULT_ENRICHED_PATH,
    results_path: Path = DEFAULT_RESULTS_PATH,
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    label_events: bool = False,
    label_limit: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run CAPM enrichment plus event-window strategy backtest."""
    source_path = input_path
    if label_events:
        print(f"Labeling backtest events: {input_path} -> {labeled_input_path}")
        label_csv(
            input_path=str(input_path),
            output_path=str(labeled_input_path),
            limit=label_limit,
        )
        source_path = labeled_input_path

    input_rows = len(load_backtest_events(source_path))

    enriched = process_all_events(
        event_path=source_path,
        output_path=enriched_path,
    )
    results = add_backtest_columns(enriched)
    summary = summarize_backtest(results)

    results_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(results_path, index=False)
    summary.to_csv(summary_path, index=False)

    print(f"Input rows: {input_rows}")
    print(f"Output rows: {len(results)}")
    assert input_rows == len(results), "Backtest output row count must equal input row count."
    print(f"Backtest result rows written: {results_path}")
    print(f"Backtest summary written: {summary_path}")
    print(summary.to_string(index=False))
    if float(summary.loc[0, "labeled_rows"]) == 0:
        print(
            "Warning: no labeled rows found in the backtest input; "
            "all rows were treated as flat trades."
        )
    print_results_preview(results)
    return results, summary


def main() -> None:
    """CLI entry point for running the held-out event backtest."""
    parser = argparse.ArgumentParser(description="Run event-level news backtest.")
    parser.add_argument("--in", dest="input_path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--labeled-in", default=str(DEFAULT_LABELED_INPUT_PATH))
    parser.add_argument("--enriched-out", default=str(DEFAULT_ENRICHED_PATH))
    parser.add_argument("--results-out", default=str(DEFAULT_RESULTS_PATH))
    parser.add_argument("--summary-out", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument(
        "--label",
        action="store_true",
        help="Label the backtest input before running the backtest.",
    )
    parser.add_argument(
        "--label-limit",
        type=int,
        default=None,
        help="Optional max rows to label when --label is used.",
    )
    args = parser.parse_args()

    run_backtest(
        input_path=Path(args.input_path),
        labeled_input_path=Path(args.labeled_in),
        enriched_path=Path(args.enriched_out),
        results_path=Path(args.results_out),
        summary_path=Path(args.summary_out),
        label_events=args.label,
        label_limit=args.label_limit,
    )


if __name__ == "__main__":
    main()
