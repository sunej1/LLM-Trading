"""Enrich stage: extract ticker candidates, resolve a primary ticker, and split keep/reject outputs."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def get_project_root() -> Path:
    """Return repository root inferred from script location."""
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent.parent


CASHTAG_PATTERN = re.compile(r"\$([A-Z]{1,5})")
EXCHANGE_PATTERN = re.compile(r"\b(?:NASDAQ|NYSE|OTC|TSX):([A-Z]{1,5})\b", re.IGNORECASE)
YAHOO_QUOTE_PATTERN = re.compile(r"finance\.yahoo\.com/quote/([A-Za-z]{1,5})")
ALL_CAPS_TOKEN_PATTERN = re.compile(r"\b([A-Z]{2,5})\b")

STOPLIST = {
    "THE",
    "AND",
    "FOR",
    "WITH",
    "THIS",
    "FROM",
    "THAT",
    "HAVE",
    "WILL",
    "YOUR",
    "YOU",
    "ARE",
    "WAS",
    "HAS",
    "NEW",
    "NEWS",
    "POST",
    "LINK",
    "URL",
    "HTTP",
    "HTTPS",
    "WWW",
    "EDIT",
    "NYSE",
    "NASDAQ",
    "OTC",
    "TSX",
    "AI",
    "NBC",
    "ABC",
    "CBS",
    "FDA",
    "DOJ",
    "TODAY",
    "SEC",
    "IRS",
    "CDC",
    "WHO",
    "NATO",
    "UN",
    "EU"
}


def dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    """Remove duplicates while preserving original order."""
    seen = set()
    ordered = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def extract_tickers(text: str) -> List[str]:
    """Collect ticker candidates via cashtags, exchange prefixes, Yahoo quotes, and capped ALL_CAPS tokens."""
    if not text:
        return []

    candidates: List[str] = []

    candidates.extend([m.upper() for m in CASHTAG_PATTERN.findall(text)])
    candidates.extend([m.upper() for m in EXCHANGE_PATTERN.findall(text)])
    candidates.extend([m.upper() for m in YAHOO_QUOTE_PATTERN.findall(text)])

    candidates.extend(
        [
            token
            for token in ALL_CAPS_TOKEN_PATTERN.findall(text)
            if token.upper() not in STOPLIST
        ]
    )

    normalized = [c.upper() for c in candidates if 1 <= len(c) <= 5]
    return dedupe_preserve_order(normalized)


def resolve_primary_ticker(headline: str, text: str, url: str, candidates: List[str]) -> Tuple[str, Dict[str, int], str]:
    """Score candidates and choose a single primary ticker, returning ticker, score map, and reason."""
    scores: Dict[str, int] = {}
    for ticker in candidates:
        score = 0

        if re.search(rf"/quote/{re.escape(ticker)}\b", url, flags=re.IGNORECASE):
            score += 12  # strong signal from dedicated quote page

        if re.search(rf"\${re.escape(ticker)}\b", headline, flags=re.IGNORECASE):
            score += 10  # explicit cashtag in headline

        if re.search(rf"\b{re.escape(ticker)}\b", headline, flags=re.IGNORECASE):
            score += 6  # plain ticker mention in headline

        headline_words = headline.split()
        topic_zone = " ".join(headline_words[:12])
        if re.search(rf"\b{re.escape(ticker)}\b", topic_zone, flags=re.IGNORECASE):
            score += 3  # early headline position bias

        body_prefix = text[:300]
        if re.search(rf"\b{re.escape(ticker)}\b", body_prefix, flags=re.IGNORECASE):
            score += 2  # early body presence

        freq_count = len(re.findall(rf"\b{re.escape(ticker)}\b", text, flags=re.IGNORECASE))
        score += min(3, freq_count // 2)  # cap repeated-body boost

        scores[ticker] = score

    if not scores:
        return "", scores, "no_candidates"

    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_ticker, best_score = ordered[0]
    second_score = ordered[1][1] if len(ordered) > 1 else 0

    if len(candidates) > 5 and best_score < 12:
        return "", scores, "too_many_candidates"

    if best_score >= 10:
        return best_ticker, scores, "strong_primary"

    if best_score >= 8 and (best_score - second_score) >= 3:
        return best_ticker, scores, "clear_margin"

    return "", scores, "ambiguous_or_low_confidence"


def process_file(path: Path, kept_dir: Path, rejected_dir: Path) -> tuple[int, int, int, int, bool]:
    """Process one cleaned file, keep confident single-ticker entries, and write keep/reject outputs."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"Failed to read {path}: {exc}")
        return 0, 0, 0, 0, False

    if not isinstance(data, list):
        print(f"Expected list in {path}")
        return 0, 0, 0, 0, False

    accepted: List[dict[str, Any]] = []
    rejected: List[dict[str, Any]] = []
    total_entries = 0
    rejected_no_ticker = 0
    rejected_ambiguous = 0

    for entry in data:
        if not isinstance(entry, dict):
            rejected_ambiguous += 1
            continue

        headline = entry.get("headline_clean") or entry.get("headline") or ""
        text = entry.get("text_clean") or entry.get("text") or ""
        url = entry.get("url") or ""

        combined_text = f"{headline}\n{text}\n{url}"
        candidates = extract_tickers(combined_text)
        primary, scores, reason = resolve_primary_ticker(headline, text, url, candidates)

        enriched = entry.copy()
        enriched["tickers_all"] = candidates
        enriched["ticker_scores"] = [{"ticker": t, "score": s} for t, s in sorted(scores.items(), key=lambda x: x[1], reverse=True)]
        enriched["ticker_resolution_version"] = "v1"
        enriched["ticker_resolution_reason"] = reason

        if primary:
            enriched["primary_ticker"] = primary
            accepted.append(enriched)
        else:
            enriched["primary_ticker"] = ""
            rejected.append(enriched)
            if not candidates:
                rejected_no_ticker += 1
            else:
                rejected_ambiguous += 1

        total_entries += 1

    kept_path = kept_dir / f"primary_{path.name}"
    rejected_path = rejected_dir / f"rejected_{path.name}"

    try:
        kept_dir.mkdir(parents=True, exist_ok=True)
        rejected_dir.mkdir(parents=True, exist_ok=True)

        with kept_path.open("w", encoding="utf-8") as f:
            json.dump(accepted, f, ensure_ascii=False, indent=2)
        with rejected_path.open("w", encoding="utf-8") as f:
            json.dump(rejected, f, ensure_ascii=False, indent=2)

        print(
            f"{path.name}: total {total_entries}, accepted {len(accepted)}, "
            f"rejected_no_ticker {rejected_no_ticker}, rejected_ambiguous {rejected_ambiguous}"
        )
        return total_entries, len(accepted), rejected_no_ticker, rejected_ambiguous, True
    except Exception as exc:
        print(f"Failed to write outputs for {path.name}: {exc}")
        return total_entries, len(accepted), rejected_no_ticker, rejected_ambiguous, False


def main() -> None:
    """Entry point: iterate cleaned files, resolve primary tickers, and emit keep/reject datasets."""
    project_root = get_project_root()
    cleaned_dir = project_root / "data" / "processed_clean"
    kept_dir = project_root / "data" / "processed_primary"
    rejected_dir = project_root / "data" / "rejected"

    cleaned_files = sorted(cleaned_dir.glob("cleaned_normalized_*.json"))
    if not cleaned_files:
        print("No cleaned_normalized_*.json files found to process.")
        return

    total_entries = 0
    total_accepted = 0
    total_rejected_no_ticker = 0
    total_rejected_ambiguous = 0
    files_skipped = 0

    for cleaned_file in cleaned_files:
        kept_path = kept_dir / f"primary_{cleaned_file.name}"
        rejected_path = rejected_dir / f"rejected_{cleaned_file.name}"
        if kept_path.exists() and rejected_path.exists():
            print(f"Skipping {cleaned_file.name} (outputs already exist)")
            files_skipped += 1
            continue

        processed, accepted, rej_no_ticker, rej_ambiguous, success = process_file(cleaned_file, kept_dir, rejected_dir)
        if success:
            total_entries += processed
            total_accepted += accepted
            total_rejected_no_ticker += rej_no_ticker
            total_rejected_ambiguous += rej_ambiguous

    print(
        f"Primary ticker filtering complete: total {total_entries}, accepted {total_accepted}, "
        f"rejected_no_ticker {total_rejected_no_ticker}, rejected_ambiguous {total_rejected_ambiguous}, "
        f"files_skipped {files_skipped}."
    )


if __name__ == "__main__":
    main()
