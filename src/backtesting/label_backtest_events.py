"""Label held-out backtest events with the existing LLM labeling stage."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.llm.label_csv_stage_a import label_csv


DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "training" / "backtest.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "training" / "backtest_labeled.csv"


def label_backtest_events(
    input_path: Path = DEFAULT_INPUT_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    limit: Optional[int] = None,
) -> None:
    """Create a labeled held-out backtest CSV without modifying the source file."""
    print(f"Labeling backtest events: {input_path} -> {output_path}")
    label_csv(
        input_path=str(input_path),
        output_path=str(output_path),
        limit=limit,
    )
    print(f"Backtest labels written: {output_path}")


def main() -> None:
    """CLI entry point for labeling held-out backtest events."""
    parser = argparse.ArgumentParser(description="Label held-out backtest events.")
    parser.add_argument("--in", dest="input_path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--out", dest="output_path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    label_backtest_events(
        input_path=Path(args.input_path),
        output_path=Path(args.output_path),
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
