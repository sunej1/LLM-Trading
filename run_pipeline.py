"""Control script to run the full news pipeline end-to-end."""

import argparse
import shutil
from pathlib import Path

from src.ingest.rss_ingest import main as ingest_main
from src.clean.normalize_rss import main as normalize_main
from src.clean.text_cleaning_v1 import main as clean_main
from src.enrich.ticker_extract_v1 import main as ticker_main
from src.enrich.company_name_to_ticker_v1 import main as name_ticker_main
from src.export.build_csv import main as csv_main


DERIVED_DIRS = [
    Path("data/processed"),
    Path("data/processed_clean"),
    Path("data/processed_primary"),
    Path("data/processed_primary_name"),
    Path("data/rejected"),
    Path("data/rejected_name"),
    Path("data/combined"),
]


def cleanup_derived_data(confirm: bool = True) -> None:
    """Delete contents of derived data directories, keeping the directories themselves."""
    if confirm:
        print("The following directories will be cleared:")
        for d in DERIVED_DIRS:
            print(f" - {d}")
        response = input("Type YES to confirm: ")
        if response.strip() != "YES":
            print("Cleanup aborted. No files deleted.")
            raise SystemExit(0)

    for directory in DERIVED_DIRS:
        path = Path(directory)
        if not path.exists():
            print(f"Skipped {path} (not found)")
            continue

        file_count = sum(1 for p in path.rglob("*") if p.is_file())
        for child in path.iterdir():
            if child.is_file():
                child.unlink()
            else:
                shutil.rmtree(child)
        print(f"Cleared {path} ({file_count} files removed)")


def run_step(label: str, func) -> bool:
    """Run one pipeline stage with logging and failure handling."""
    print(f"=== Running {label} ===")
    try:
        func()
        print(f"{label} completed successfully.\n")
        return True
    except Exception as exc:
        print(f"{label} failed.")
        print(f"Error: {exc}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the news pipeline.")
    parser.add_argument("--clean", action="store_true", help="Clean derived data before running.")
    parser.add_argument(
        "--clean-force",
        action="store_true",
        help="Clean derived data before running without confirmation.",
    )
    args = parser.parse_args()

    if args.clean or args.clean_force:
        cleanup_derived_data(confirm=not args.clean_force)

    steps_before_name_resolution = [
        ("RSS ingestion", ingest_main),
        ("Normalization", normalize_main),
        ("Text cleaning", clean_main),
        ("Ticker extraction / primary resolution", ticker_main),
    ]

    for label, func in steps_before_name_resolution:
        if not run_step(label, func):
            return

    print("=== Running company name → ticker resolution ===")
    try:
        name_ticker_main()
        print("Company-name ticker resolution completed successfully.")
    except Exception as e:
        print("Company-name ticker resolution failed.")
        print(e)
        raise

    if not run_step("CSV building", csv_main):
        return

    print("Pipeline complete. Combined CSV is ready.")


if __name__ == "__main__":
    main()
