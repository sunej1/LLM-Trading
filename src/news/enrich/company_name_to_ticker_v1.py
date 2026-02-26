"""Resolve tickers by matching company names in cleaned articles using a local mapping."""
from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

HEADLINE_WEIGHT = 3
BODY_WEIGHT = 1
MIN_ACCEPT_SCORE = 3
HEADLINE_MATCH_CAP = 3
BODY_MATCH_CAP = 5


def get_project_root() -> Path:
    """Return repository root inferred from script location."""
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent.parent.parent


def normalize_company_name(raw: str) -> str:
    """Trim whitespace/quotes and collapse internal spaces for consistent regex building."""
    name = raw.strip().strip('"').strip("'")
    name = re.sub(r"\s+", " ", name)
    return name


def build_name_pattern(name: str) -> re.Pattern[str]:
    """Build a case-insensitive pattern with flexible spacing and optional apostrophes/periods."""
    parts: List[str] = []
    for ch in name:
        if ch.isspace():
            parts.append(r"\s+")
        elif ch in {"'", "’"}:
            parts.append(r"[’']?")
        elif ch == ".":
            parts.append(r"\.?")
        else:
            parts.append(re.escape(ch))
    pattern_str = r"\b" + "".join(parts) + r"\b"
    return re.compile(pattern_str, re.IGNORECASE)


def load_company_mapping(path: Path) -> Dict[str, List[Tuple[str, re.Pattern[str]]]]:
    """Load ticker-to-company names (full/short); raise if file missing or malformed."""
    if not path.exists():
        raise FileNotFoundError(
            f"Company ticker mapping not found at {path}. Please create and populate config/company_tickers.csv."
        )

    patterns: Dict[str, List[Tuple[str, re.Pattern[str]]]] = defaultdict(list)

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required_columns = {"ticker", "company_full", "company_short"}
        missing_columns = required_columns - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"Missing required column(s) in {path}: {', '.join(sorted(missing_columns))}"
            )

        for idx, row in enumerate(reader, start=2):  # header is line 1
            ticker = (row.get("ticker") or "").strip().upper()
            company_full = normalize_company_name(row.get("company_full") or "")
            company_short = normalize_company_name(row.get("company_short") or "")
            if not ticker or (not company_full and not company_short):
                print(f"Skipping line {idx}: ticker and at least one company name are required.")
                continue

            names: List[str] = []
            seen_names = set()

            if company_full:
                names.append(company_full)
                seen_names.add(company_full.lower())

            if company_short and company_short.lower() not in seen_names:
                names.append(company_short)
                seen_names.add(company_short.lower())

            for name in names:
                pattern = build_name_pattern(name)
                patterns[ticker].append((name, pattern))

    if not patterns:
        raise ValueError(f"No valid mappings found in {path}. Populate it with ticker,company_full/company_short rows.")

    return patterns


def is_junk_headline(headline: str) -> bool:
    """Identify obvious non-article headlines."""
    lower = headline.strip().lower()
    if lower.startswith("watch:") or lower.startswith("live:"):
        return True
    if "abc news live" in lower or "news live" in lower:
        return True
    return False


def score_ticker(
    headline: str, body: str, patterns: List[Tuple[str, re.Pattern[str]]]
) -> Tuple[int, List[str]]:
    """Score one ticker based on name matches in headline/body."""
    score = 0
    matched_names: List[str] = []

    for name, pattern in patterns:
        headline_matches = len(pattern.findall(headline))
        body_matches = len(pattern.findall(body))
        if headline_matches or body_matches:
            matched_names.append(name)

        score += min(headline_matches, HEADLINE_MATCH_CAP) * HEADLINE_WEIGHT
        score += min(body_matches, BODY_MATCH_CAP) * BODY_WEIGHT

    return score, matched_names


def resolve_primary_ticker(
    headline: str, body: str, patterns: Dict[str, List[Tuple[str, re.Pattern[str]]]]
) -> Tuple[str, List[Dict[str, Any]], List[str], str]:
    """Resolve the primary ticker from name matches."""
    if is_junk_headline(headline):
        return "", [], [], "no_match"

    scored: List[Tuple[str, int, List[str]]] = []
    for ticker, ticker_patterns in patterns.items():
        score, matched_names = score_ticker(headline, body, ticker_patterns)
        if score > 0:
            scored.append((ticker, score, matched_names))

    if not scored:
        return "", [], [], "no_match"

    scored.sort(key=lambda x: x[1], reverse=True)
    top_ticker, top_score, top_names = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else 0

    if top_score < MIN_ACCEPT_SCORE:
        return "", [{"ticker": t, "score": s, "matched_names": names} for t, s, names in scored], [t for t, _, _ in scored], "no_match"

    if len(scored) == 1:
        reason = "unique_match"
        primary = top_ticker
    elif top_score >= 2 * second_score:
        reason = "dominant_match"
        primary = top_ticker
    else:
        reason = "ambiguous"
        primary = ""

    name_ticker_scores = [
        {"ticker": t, "score": s, "matched_names": names} for t, s, names in scored
    ]
    name_tickers_all = [t for t, _, _ in scored]

    return primary, name_ticker_scores, name_tickers_all, reason


def process_file(
    path: Path,
    patterns: Dict[str, List[Tuple[str, re.Pattern[str]]]],
    accepted_dir: Path,
    rejected_dir: Path,
) -> Tuple[int, int, int]:
    """Process one cleaned file, writing accepted and rejected outputs."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"Failed to read {path}: {exc}")
        return 0, 0, 0

    if not isinstance(data, list):
        print(f"Expected list in {path}")
        return 0, 0, 0

    accepted: List[dict[str, Any]] = []
    rejected: List[dict[str, Any]] = []

    rejected_no_match = 0
    rejected_ambiguous = 0

    for entry in data:
        if not isinstance(entry, dict):
            rejected_no_match += 1
            rejected.append(
                {
                    "name_tickers_all": [],
                    "primary_ticker_name": "",
                    "name_ticker_scores": [],
                    "name_ticker_resolution_version": "v1",
                    "name_ticker_resolution_reason": "no_match",
                }
            )
            continue

        headline = str(entry.get("headline_clean") or entry.get("headline") or "")
        body = str(entry.get("text_clean") or entry.get("text") or "")

        primary, scores, tickers_all, reason = resolve_primary_ticker(headline, body, patterns)

        enriched = entry.copy()
        enriched["name_tickers_all"] = tickers_all
        enriched["primary_ticker_name"] = primary
        enriched["name_ticker_scores"] = scores
        enriched["name_ticker_resolution_version"] = "v1"
        enriched["name_ticker_resolution_reason"] = reason

        if not enriched.get("primary_ticker") and primary:
            enriched["primary_ticker"] = primary

        if primary:
            accepted.append(enriched)
        else:
            rejected.append(enriched)
            if reason == "ambiguous":
                rejected_ambiguous += 1
            else:
                rejected_no_match += 1

    accepted_dir.mkdir(parents=True, exist_ok=True)
    rejected_dir.mkdir(parents=True, exist_ok=True)

    accepted_path = accepted_dir / f"name_primary_{path.name}"
    rejected_path = rejected_dir / f"name_rejected_{path.name}"

    with accepted_path.open("w", encoding="utf-8") as f:
        json.dump(accepted, f, ensure_ascii=False, indent=2)
    with rejected_path.open("w", encoding="utf-8") as f:
        json.dump(rejected, f, ensure_ascii=False, indent=2)

    print(
        f"{path.name}: total {len(data)}, accepted {len(accepted)}, "
        f"rejected_no_match {rejected_no_match}, rejected_ambiguous {rejected_ambiguous}"
    )

    return len(data), len(accepted), rejected_ambiguous


def main() -> None:
    """Iterate cleaned files, resolve tickers by company names, and write outputs."""
    project_root = get_project_root()
    cleaned_dir = project_root / "data" / "processing" / "processed_clean"
    accepted_dir = project_root / "data" / "processing" / "processed_primary_name"
    rejected_dir = project_root / "data" / "processing" / "rejected_name"
    mapping_path = project_root / "config" / "company_tickers.csv"

    patterns = load_company_mapping(mapping_path)

    cleaned_files = sorted(cleaned_dir.glob("cleaned_normalized_*.json"))
    if not cleaned_files:
        print("No cleaned_normalized_*.json files found to process.")
        return

    total_records = 0
    total_accepted = 0
    total_rejected_ambiguous = 0
    files_processed = 0

    for cleaned_file in cleaned_files:
        total, accepted, rejected_ambiguous = process_file(
            cleaned_file, patterns, accepted_dir, rejected_dir
        )
        if total > 0:
            files_processed += 1
            total_records += total
            total_accepted += accepted
            total_rejected_ambiguous += rejected_ambiguous

    print(
        f"Name-based ticker resolution complete: files {files_processed}, total_records {total_records}, "
        f"accepted {total_accepted}, rejected_ambiguous {total_rejected_ambiguous}, "
        f"rejected_no_match {total_records - total_accepted - total_rejected_ambiguous}."
    )


if __name__ == "__main__":
    main()
