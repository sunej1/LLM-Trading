"""Orchestrate held-out backtest labeling and CAPM backtesting.

For iterative work, prefer running these two stages separately:

1. src/backtesting/label_backtest_events.py
2. src/backtesting/capm_backtest.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.backtesting.capm_backtest import (
    DEFAULT_ENRICHED_PATH,
    DEFAULT_PRICE_INTERVAL,
    DEFAULT_RESULTS_PATH,
    DEFAULT_SUMMARY_PATH,
    run_capm_backtest,
)
from src.backtesting.label_backtest_events import (
    DEFAULT_INPUT_PATH,
    DEFAULT_OUTPUT_PATH,
    label_backtest_events,
)


def run_backtest(
    input_path: Path = DEFAULT_INPUT_PATH,
    labeled_input_path: Path = DEFAULT_OUTPUT_PATH,
    enriched_path: Path = DEFAULT_ENRICHED_PATH,
    results_path: Path = DEFAULT_RESULTS_PATH,
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    label_events: bool = False,
    label_limit: Optional[int] = None,
    price_interval: str = DEFAULT_PRICE_INTERVAL,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Optionally label events, then run the CAPM backtest on the labeled file."""
    source_path = labeled_input_path
    if label_events:
        label_backtest_events(
            input_path=input_path,
            output_path=labeled_input_path,
            limit=label_limit,
        )

    if not source_path.exists():
        raise FileNotFoundError(
            f"Labeled backtest file not found: {source_path}. "
            "Run .venv/bin/python src/backtesting/label_backtest_events.py first, "
            "or pass --label."
        )

    return run_capm_backtest(
        input_path=source_path,
        enriched_path=enriched_path,
        results_path=results_path,
        summary_path=summary_path,
        price_interval=price_interval,
    )


def main() -> None:
    """CLI entry point for the combined backtest workflow."""
    parser = argparse.ArgumentParser(description="Run event-level news backtest.")
    parser.add_argument("--in", dest="input_path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--labeled-in", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--enriched-out", default=str(DEFAULT_ENRICHED_PATH))
    parser.add_argument("--results-out", default=str(DEFAULT_RESULTS_PATH))
    parser.add_argument("--summary-out", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument(
        "--label",
        action="store_true",
        help="Label the backtest input before running the CAPM backtest.",
    )
    parser.add_argument(
        "--label-limit",
        type=int,
        default=None,
        help="Optional max rows to label when --label is used.",
    )
    parser.add_argument(
        "--price-interval",
        default=DEFAULT_PRICE_INTERVAL,
        help="Yahoo price interval: 1d, 1h, 60m, 30m, 15m, 5m, or 1m.",
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
        price_interval=args.price_interval,
    )


if __name__ == "__main__":
    main()
