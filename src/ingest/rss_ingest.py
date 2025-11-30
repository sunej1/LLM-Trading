from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import feedparser
import requests
import yaml


def load_config(config_path: Path) -> dict[str, Any] | None:
    """Load YAML config, returning None on failure."""
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        return None

    try:
        with config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as exc:  # broad to report any read/parse issues
        print(f"Failed to read config file: {exc}")
        return None


def fetch_feed(url: str, source_name: str) -> bytes | None:
    """Fetch the RSS feed content via HTTP."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        print(f"Failed to fetch RSS feed for '{source_name}' ({url}): {exc}")
        return None


def _json_fallback(obj: Any) -> str:
    """Fallback serializer to keep raw values that aren't JSON-serializable."""
    return str(obj)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    config_path = project_root / "config" / "rss_sources.yaml"

    config = load_config(config_path)
    if not config:
        return

    sources = config.get("rss_sources") or []
    if not sources:
        print("No RSS sources found in config.")
        return

    output_dir = project_root / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    success_count = 0
    failure_count = 0
    files_written = 0

    for source in sources:
        source_name = source.get("name", "source")
        url = source.get("url")
        if not url:
            print(f"Skipping source '{source_name}': missing URL.")
            failure_count += 1
            continue

        feed_content = fetch_feed(url, source_name)
        if feed_content is None:
            failure_count += 1
            continue

        try:
            parsed_feed = feedparser.parse(feed_content)
            entries = parsed_feed.entries
        except Exception as exc:  # defensive in case parsing raises
            print(f"Failed to parse feed for '{source_name}' ({url}): {exc}")
            failure_count += 1
            continue

        timestamp = datetime.utcnow().isoformat(timespec="seconds").replace(":", "-")
        output_path = output_dir / f"{source_name}_{timestamp}.json"

        try:
            with output_path.open("w", encoding="utf-8") as f:
                json.dump(entries, f, default=_json_fallback, ensure_ascii=False, indent=2)
            print(f"Saved {len(entries)} entries for '{source_name}' to {output_path}")
            success_count += 1
            files_written += 1
        except Exception as exc:
            print(f"Failed to write output file for '{source_name}' ({output_path}): {exc}")
            failure_count += 1

    print(
        f"Summary: {success_count} sources processed successfully, "
        f"{failure_count} failed, {files_written} JSON files written."
    )


if __name__ == "__main__":
    main()
