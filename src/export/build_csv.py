from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List


def get_project_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent.parent


def load_normalized_entries(path: Path) -> List[dict[str, Any]] | None:
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


def write_csv(entries: Iterable[dict[str, Any]], output_path: Path) -> bool:
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

    try:
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for entry in entries:
                writer.writerow({key: entry.get(key, "") for key in fieldnames})
        return True
    except Exception as exc:
        print(f"Failed to write CSV file {output_path}: {exc}")
        return False


def main() -> None:
    project_root = get_project_root()
    processed_dir = project_root / "data" / "processed"
    combined_dir = project_root / "data" / "combined"

    all_entries: List[dict[str, Any]] = []
    files_read = 0
    failed_files = 0

    if not processed_dir.exists():
        print("No processed JSON files found.")
        return

    for path in sorted(processed_dir.glob("normalized_*.json")):
        entries = load_normalized_entries(path)
        if entries is None:
            failed_files += 1
            continue
        all_entries.extend(entries)
        files_read += 1

    if files_read == 0 and failed_files == 0:
        print("No processed JSON files found.")
        return

    if files_read == 0 and failed_files > 0:
        print("All processed JSON files failed to load.")
        return

    combined_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().isoformat(timespec="seconds").replace(":", "-")
    output_path = combined_dir / f"combined_{timestamp}.csv"

    if write_csv(all_entries, output_path):
        print(
            f"CSV build complete: {files_read} files read, "
            f"{len(all_entries)} rows written, {failed_files} files failed."
        )
    else:
        print(
            f"CSV build failed during write: attempted rows {len(all_entries)}, "
            f"files failed {failed_files}."
        )


if __name__ == "__main__":
    main()
