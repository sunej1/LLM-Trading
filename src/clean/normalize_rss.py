"""Normalize stage: convert raw RSS entries into a uniform schema under data/processed/."""
from __future__ import annotations

import json
import uuid
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, List


def get_project_root() -> Path:
    """Return repository root inferred from script location."""
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent.parent


def extract_source_from_filename(path: Path) -> str:
    """Derive source name from raw filename by stripping the final timestamp segment."""
    stem = path.stem  # e.g., reddit_wsb_2025-11-30T22-02-56
    parts = stem.split("_")
    if len(parts) <= 1:
        return stem
    return "_".join(parts[:-1])


def load_entries(path: Path) -> List[Any] | None:
    """Load a JSON list from a raw RSS file; return None on failure or unexpected shape."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            print(f"Expected a list of entries in {path}")
            return None
        return data
    except Exception as exc:
        print(f"Failed to read JSON file {path}: {exc}")
        return None


def parse_timestamp(entry: dict[str, Any]) -> str:
    """Convert common RSS timestamp fields to UTC ISO string; fallback to empty string."""
    for field in ("published", "updated", "created"):
        raw_value = entry.get(field)
        if not raw_value:
            continue
        try:
            dt = parsedate_to_datetime(str(raw_value))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.isoformat(timespec="seconds")
        except Exception:
            continue
    return ""


def normalize_entry(entry: dict[str, Any], source: str) -> dict[str, Any]:
    """Map a raw RSS item into the normalized event schema with generated id."""
    headline = entry.get("title") or ""
    text = entry.get("summary") or entry.get("description") or ""
    url = entry.get("link") or ""

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": parse_timestamp(entry),
        "source": source,
        "headline": headline,
        "text": text,
        "url": url,
        "ticker": "",
        "category": "",
        "label_severity": "",
        "label_direction": "",
        "label_time_horizon": "",
    }


def normalize_entries(entries: Iterable[dict[str, Any]], source: str) -> List[dict[str, Any]]:
    """Normalize all valid dict entries for a given source into a list."""
    normalized = []
    for entry in entries:
        if isinstance(entry, dict):
            normalized.append(normalize_entry(entry, source))
    return normalized


def main() -> None:
    """Load raw RSS dumps, normalize entries, and write normalized_*.json files."""
    project_root = get_project_root()
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"

    processed_dir.mkdir(parents=True, exist_ok=True)
    raw_files = sorted([p for p in raw_dir.glob("*.json") if p.is_file()])
    if not raw_files:
        print("No raw RSS files found to process.")
        return

    processed_count = 0
    skipped_count = 0
    failed_count = 0

    for raw_file in raw_files:
        output_path = processed_dir / f"normalized_{raw_file.name}"
        if output_path.exists():
            print(f"Skipping {raw_file.name} (normalized file already exists)")
            skipped_count += 1
            continue

        source = extract_source_from_filename(raw_file)
        raw_entries = load_entries(raw_file)
        if raw_entries is None:
            failed_count += 1
            continue

        try:
            normalized_entries = normalize_entries(raw_entries, source)
        except Exception as exc:
            print(f"Failed to normalize entries from {raw_file.name}: {exc}")
            failed_count += 1
            continue

        try:
            with output_path.open("w", encoding="utf-8") as f:
                json.dump(normalized_entries, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(normalized_entries)} entries to {output_path}")
            processed_count += 1
        except Exception as exc:
            print(f"Failed to write normalized file for {raw_file.name}: {exc}")
            failed_count += 1

    print(
        f"Normalization complete: {processed_count} files processed, "
        f"{skipped_count} skipped (already normalized), {failed_count} failed."
    )


if __name__ == "__main__":
    main()
