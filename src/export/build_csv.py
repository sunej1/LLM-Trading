"""Export stage: combine enriched JSON files and write a consolidated CSV snapshot with metadata."""
from __future__ import annotations

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from src.export.price_enrichment import compute_time_horizons, get_minute_prices, parse_timestamp_utc

logger = logging.getLogger(__name__)

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


def ticker_confidence(entry: dict[str, Any]) -> str:
    """Classify ticker confidence based on available resolution fields."""
    primary_ticker_val = str(entry.get("primary_ticker") or "").strip()
    primary_ticker_name_val = str(entry.get("primary_ticker_name") or "").strip()
    ticker_reason = str(entry.get("ticker_resolution_reason") or "").lower()
    ticker_version = str(entry.get("ticker_resolution_version") or "").strip()
    name_reason = str(entry.get("name_ticker_resolution_reason") or "").strip()

    if primary_ticker_val and (("exchange" in ticker_reason) or ("cashtag" in ticker_reason) or ticker_version):
        return "explicit"
    if primary_ticker_name_val or name_reason:
        if name_reason in {"unique_match", "dominant_match"}:
            return "name_high"
        return "name_medium"
    return "unknown"


SOURCE_CREDIBILITY_MAP = {
    "ap_news_": "high",
    "npr_": "high",
    "abc_news_": "high",
    "cbs_news_": "high",
    "nbc_news_": "high",
}


def source_credibility(entry: dict[str, Any]) -> str:
    """Resolve source credibility using record field or source prefix mapping."""
    existing = str(entry.get("source_credibility") or "").strip()
    if existing:
        return existing

    source = str(entry.get("source") or "").lower()
    for prefix, rating in SOURCE_CREDIBILITY_MAP.items():
        if source.startswith(prefix):
            return rating
    return "unknown"


def write_csv(
    entries: Iterable[dict[str, Any]],
    output_path: Path,
    price_fetcher=get_minute_prices,
) -> Tuple[bool, int, Dict[str, int]]:
    """Write entries to CSV with fixed schema; return success flag, count with non-empty ticker, and confidence breakdown."""
    fieldnames = [
        "event_id",
        "timestamp",
        "source",
        "headline",
        "text",
        "url",
        "ticker",
        "ticker_confidence",
        "source_credibility",
        "category",
        "label_severity",
        "label_direction",
        "label_time_horizon_1_min",
        "label_time_horizon_2_min",
    ]

    ticker_non_empty = 0
    confidence_counts: Dict[str, int] = {"explicit": 0, "name_high": 0, "name_medium": 0, "unknown": 0}

    try:
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for entry in entries:
                ticker_value = choose_ticker(entry)
                if ticker_value:
                    ticker_non_empty += 1
                confidence_value = ticker_confidence(entry)
                confidence_counts[confidence_value] = confidence_counts.get(confidence_value, 0) + 1

                event_ts = parse_timestamp_utc(str(entry.get("timestamp") or ""))

                label_time_horizon_1_min: Any = ""
                label_time_horizon_2_min: Any = ""
                bottom_ts = None
                peak_ts = None

                if ticker_value and confidence_value != "unknown" and event_ts:
                    t_to_bottom, t_bottom_to_peak, bottom_ts, peak_ts = compute_time_horizons(
                        ticker_value, event_ts, price_fetcher
                    )
                    if t_to_bottom is not None:
                        label_time_horizon_1_min = t_to_bottom
                    if t_bottom_to_peak is not None:
                        label_time_horizon_2_min = t_bottom_to_peak
                else:
                    if not ticker_value:
                        logger.debug("Skipping price enrichment for event_id=%s: missing ticker", entry.get("event_id"))
                    elif confidence_value == "unknown":
                        logger.debug(
                            "Skipping price enrichment for event_id=%s: low ticker confidence (%s)",
                            entry.get("event_id"),
                            confidence_value,
                        )
                    elif not event_ts:
                        logger.warning(
                            "Invalid or missing timestamp for event_id=%s: %s",
                            entry.get("event_id"),
                            entry.get("timestamp"),
                        )

                logger.debug(
                    "event_id=%s ticker=%s event_ts=%s bottom_ts=%s peak_ts=%s t_to_bottom=%s t_bottom_to_peak=%s",
                    entry.get("event_id"),
                    ticker_value,
                    event_ts,
                    bottom_ts,
                    peak_ts,
                    label_time_horizon_1_min if label_time_horizon_1_min != "" else None,
                    label_time_horizon_2_min if label_time_horizon_2_min != "" else None,
                )

                row = {
                    "event_id": entry.get("event_id", ""),
                    "timestamp": entry.get("timestamp", ""),
                    "source": entry.get("source", ""),
                    "headline": entry.get("headline_clean") or entry.get("headline", ""),
                    "text": entry.get("text_clean") or entry.get("text", ""),
                    "url": entry.get("url", ""),
                    "ticker": ticker_value,
                    "ticker_confidence": confidence_value,
                    "source_credibility": source_credibility(entry),
                    "category": entry.get("category", ""),
                    "label_severity": entry.get("label_severity", ""),
                    "label_direction": entry.get("label_direction", ""),
                    "label_time_horizon_1_min": label_time_horizon_1_min,
                    "label_time_horizon_2_min": label_time_horizon_2_min,
                }
                writer.writerow(row)
        return True, ticker_non_empty, confidence_counts
    except Exception as exc:
        print(f"Failed to write CSV file {output_path}: {exc}")
        return False, ticker_non_empty, confidence_counts


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

    success, ticker_non_empty, confidence_counts = write_csv(deduped_entries, output_path)
    if success:
        print(
            f"CSV build complete: files primary={files_read_primary}, name_based={files_read_name}, "
            f"total_records {len(all_entries)}, after_dedupe {len(deduped_entries)}, "
            f"rows_written {len(deduped_entries)}, rows_with_ticker {ticker_non_empty}, "
            f"confidence_breakdown {confidence_counts}, failed_files {failed_files}."
        )
    else:
        print(
            f"CSV build failed during write: attempted rows {len(deduped_entries)}, "
            f"files primary={files_read_primary}, name_based={files_read_name}, failed_files {failed_files}."
        )


if __name__ == "__main__":
    main()
