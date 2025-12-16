from __future__ import annotations

import html
import json
import re
from pathlib import Path
from typing import Any, List


def get_project_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent.parent


URL_PATTERN = re.compile(r"https?://\S+")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
REDDIT_LINK_BLOCK_PATTERN = re.compile(r"\[link\].*?\[comments\]", re.IGNORECASE | re.DOTALL)


def clean_text(raw: Any, source: str) -> str:
    if raw is None:
        return ""

    text = str(raw)
    text = html.unescape(text)

    if source.startswith("reddit_"):
        text = text.replace("<!-- SC_OFF -->", "").replace("<!-- SC_ON -->", "")
        text = re.sub(r"submitted by", "", text, flags=re.IGNORECASE)
        text = re.sub(REDDIT_LINK_BLOCK_PATTERN, "", text)
        text = text.replace("&#32;", " ")

    text = re.sub(HTML_TAG_PATTERN, " ", text)
    text = re.sub(URL_PATTERN, "[URL]", text)

    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = text.strip()

    if len(text) > 12000:
        text = text[:12000] + "...(truncated)"

    return text


def load_entries(path: Path) -> List[Any] | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            print(f"Expected list in {path}")
            return None
        return data
    except Exception as exc:
        print(f"Failed to read {path}: {exc}")
        return None


def process_file(path: Path, output_dir: Path) -> tuple[int, int, int, bool]:
    entries = load_entries(path)
    if entries is None:
        return 0, 0, 0, False

    cleaned_entries: List[dict[str, Any]] = []
    cleaned_count = 0
    skipped_count = 0
    failed_count = 0

    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            skipped_count += 1
            continue

        processed = entry.copy()
        try:
            source = str(processed.get("source", ""))
            processed["headline_clean"] = clean_text(processed.get("headline"), source)
            processed["text_clean"] = clean_text(processed.get("text"), source)
            processed["cleaning_version"] = "v1"
            cleaned_count += 1
        except Exception as exc:
            print(f"Failed to clean entry {idx} in {path.name}: {exc}")
            processed.setdefault("headline_clean", "")
            processed.setdefault("text_clean", "")
            processed.setdefault("cleaning_version", "v1")
            failed_count += 1

        cleaned_entries.append(processed)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"cleaned_{path.name}"
    try:
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(cleaned_entries, f, ensure_ascii=False, indent=2)
        print(
            f"Processed {path.name}: cleaned {cleaned_count}, "
            f"skipped {skipped_count}, failed {failed_count}"
        )
        return cleaned_count, skipped_count, failed_count, True
    except Exception as exc:
        print(f"Failed to write cleaned file for {path.name}: {exc}")
        return cleaned_count, skipped_count, failed_count, False


def main() -> None:
    project_root = get_project_root()
    processed_dir = project_root / "data" / "processed"
    output_dir = project_root / "data" / "processed_clean"

    normalized_files = sorted(processed_dir.glob("normalized_*.json"))
    if not normalized_files:
        print("No normalized_*.json files found to clean.")
        return

    files_processed = 0
    files_skipped = 0
    files_failed = 0

    for normalized_file in normalized_files:
        output_path = output_dir / f"cleaned_{normalized_file.name}"
        if output_path.exists():
            print(f"Skipping {normalized_file.name} (cleaned file already exists)")
            files_skipped += 1
            continue

        cleaned_count, skipped_count, failed_count, success = process_file(normalized_file, output_dir)
        if success:
            files_processed += 1
        else:
            files_failed += 1

    print(
        f"Text cleaning complete: {files_processed} files processed, "
        f"{files_skipped} skipped, {files_failed} failed."
    )


if __name__ == "__main__":
    main()
