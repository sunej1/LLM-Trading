"""Run CAPM/news event-window backtest on an already-labeled backtest file."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.modeling.capm_news_mispricing import process_all_events


DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "training" / "backtest_labeled.csv"
DEFAULT_ENRICHED_PATH = PROJECT_ROOT / "data" / "training" / "backtest_event_mispricing.csv"
DEFAULT_RESULTS_PATH = PROJECT_ROOT / "data" / "training" / "backtest_event_results.csv"
DEFAULT_SUMMARY_PATH = PROJECT_ROOT / "data" / "training" / "backtest_summary.csv"
DEFAULT_PRICE_INTERVAL = "15m"

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


def _direction_to_signal(value: object) -> float:
    """Convert label_direction into long, flat, or short signal."""
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

    results["stock_return_event_window"] = (
        results["actual_stock_price"] / results["previous_stock_price"]
    ) - 1
    results["strategy_return"] = results["signal"] * results["stock_return_event_window"]
    results.loc[results["signal"] == 0, "strategy_return"] = 0.0

    results["capm_abnormal_return"] = (
        results["stock_return_event_window"] - results["capm_fair_return"]
    )
    results["strategy_abnormal_return"] = results["signal"] * results["capm_abnormal_return"]
    results.loc[results["signal"] == 0, "strategy_abnormal_return"] = 0.0

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
        "price_interval",
        "signal",
        "trade_side",
        "stock_return_event_window",
        "strategy_return",
        "capm_abnormal_return",
    ]
    available_columns = [col for col in preview_columns if col in results.columns]
    print(results[available_columns].head(rows).to_string(index=False))


def run_capm_backtest(
    input_path: Path = DEFAULT_INPUT_PATH,
    enriched_path: Path = DEFAULT_ENRICHED_PATH,
    results_path: Path = DEFAULT_RESULTS_PATH,
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    price_interval: str = DEFAULT_PRICE_INTERVAL,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run CAPM enrichment and strategy return calculations on labeled events."""
    source_events = load_backtest_events(input_path)
    input_rows = len(source_events)
    labeled_rows = count_labeled_rows(source_events)
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

    enriched = process_all_events(
        event_path=input_path,
        output_path=enriched_path,
        verbose=True,
        price_interval=price_interval,
    )
    results = add_backtest_columns(enriched)
    for idx, row in results.iterrows():
        print(
            f"[backtest] row {idx + 1}/{len(results)} "
            f"event_id={row.get('event_id', '')} "
            f"ticker={row.get('ticker', '')} "
            f"side={row.get('trade_side', '')} "
            f"strategy_return={row.get('strategy_return')}",
            flush=True,
        )

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
    print_results_preview(results)
    return results, summary


def main() -> None:
    """CLI entry point for CAPM-only backtesting."""
    parser = argparse.ArgumentParser(description="Run CAPM event-level news backtest.")
    parser.add_argument("--in", dest="input_path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--enriched-out", default=str(DEFAULT_ENRICHED_PATH))
    parser.add_argument("--results-out", default=str(DEFAULT_RESULTS_PATH))
    parser.add_argument("--summary-out", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument(
        "--price-interval",
        default=DEFAULT_PRICE_INTERVAL,
        help="Yahoo price interval: 1d, 1h, 60m, 30m, 15m, 5m, or 1m.",
    )
    args = parser.parse_args()

    run_capm_backtest(
        input_path=Path(args.input_path),
        enriched_path=Path(args.enriched_out),
        results_path=Path(args.results_out),
        summary_path=Path(args.summary_out),
        price_interval=args.price_interval,
    )


if __name__ == "__main__":
    main()
