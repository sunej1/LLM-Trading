"""Export stage: combine enriched JSON files and write a consolidated CSV snapshot."""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def get_project_root() -> Path:
    """Return repository root inferred from script location."""
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent.parent


def load_entries(path: Path) -> List[dict[str, Any]] | None:
    """Load a JSON list and return only dict entries; None on read/shape failure."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"Failed to read JSON file {path}: {exc}")
        return None

    if not isinstance(data, list):
        print(f"Expected a list of entries in {path}")
        return None

    filtered: List[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            filtered.append(item)
    return filtered


def dedupe_entries(entries: Iterable[dict[str, Any]]) -> List[dict[str, Any]]:
    """Dedupe entries by URL or headline+timestamp, preserving first seen."""
    deduped: Dict[Tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        url = str(entry.get("url") or "").strip()
        headline = str(entry.get("headline_clean") or entry.get("headline") or "")
        timestamp = str(entry.get("timestamp") or "")
        key = (url, "") if url else ("", headline + "|" + timestamp)
        if key not in deduped:
            deduped[key] = entry
    return list(deduped.values())


def choose_ticker(entry: dict[str, Any]) -> str:
    """Select ticker with priority: primary_ticker, primary_ticker_name, ticker, else empty."""
    for field in ("primary_ticker", "primary_ticker_name", "ticker"):
        value = str(entry.get(field) or "").strip()
        if value:
            return value
    return ""


def write_csv(entries: Iterable[dict[str, Any]], output_path: Path) -> Tuple[bool, int]:
    """Write entries to CSV with fixed schema; return success flag and count with non-empty ticker."""
    fieldnames = [
        "event_id",
        "timestamp",
        "source",
        "headline",
        "text",
        "url",
        "ticker",
        "category",
        "label_severity",
        "label_direction",
        "label_time_horizon",
    ]

    ticker_non_empty = 0

    try:
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for entry in entries:
                ticker_value = choose_ticker(entry)
                if ticker_value:
                    ticker_non_empty += 1
                row = {
                    "event_id": entry.get("event_id", ""),
                    "timestamp": entry.get("timestamp", ""),
                    "source": entry.get("source", ""),
                    "headline": entry.get("headline_clean") or entry.get("headline", ""),
                    "text": entry.get("text_clean") or entry.get("text", ""),
                    "url": entry.get("url", ""),
                    "ticker": ticker_value,
                    "category": entry.get("category", ""),
                    "label_severity": entry.get("label_severity", ""),
                    "label_direction": entry.get("label_direction", ""),
                    "label_time_horizon": entry.get("label_time_horizon", ""),
                }
                writer.writerow(row)
        return True, ticker_non_empty
    except Exception as exc:
        print(f"Failed to write CSV file {output_path}: {exc}")
        return False, ticker_non_empty


def main() -> None:
    """Aggregate enriched JSON files and emit a combined CSV snapshot with summary logging."""
    project_root = get_project_root()
    primary_dir = project_root / "data" / "processed_primary"
    name_dir = project_root / "data" / "processed_primary_name"
    combined_dir = project_root / "data" / "combined"

    all_entries: List[dict[str, Any]] = []

    files_read_primary = 0
    files_read_name = 0
    failed_files = 0

    for path in sorted(primary_dir.glob("*.json")) if primary_dir.exists() else []:
        entries = load_entries(path)
        if entries is None:
            failed_files += 1
            continue
        all_entries.extend(entries)
        files_read_primary += 1

    for path in sorted(name_dir.glob("*.json")) if name_dir.exists() else []:
        entries = load_entries(path)
        if entries is None:
            failed_files += 1
            continue
        all_entries.extend(entries)
        files_read_name += 1

    if not all_entries and failed_files == 0:
        print("No enriched JSON files found to process.")
        return

    deduped_entries = dedupe_entries(all_entries)

    combined_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().isoformat(timespec="seconds").replace(":", "-")
    output_path = combined_dir / f"combined_{timestamp}.csv"

    success, ticker_non_empty = write_csv(deduped_entries, output_path)
    if success:
        print(
            f"CSV build complete: files primary={files_read_primary}, name_based={files_read_name}, "
            f"total_records {len(all_entries)}, after_dedupe {len(deduped_entries)}, "
            f"rows_written {len(deduped_entries)}, rows_with_ticker {ticker_non_empty}, failed_files {failed_files}."
        )
    else:
        print(
            f"CSV build failed during write: attempted rows {len(deduped_entries)}, "
            f"files primary={files_read_primary}, name_based={files_read_name}, failed_files {failed_files}."
        )


if __name__ == "__main__":
    main()
