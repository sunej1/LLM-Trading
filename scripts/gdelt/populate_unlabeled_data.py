import csv
import random
import string
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sys
import requests
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.news.enrich.ticker_extract_v1 import extract_tickers, resolve_primary_ticker


COMPANIES_CSV = REPO_ROOT / "config" / "company_tickers.csv"
OUTPUT_TRAIN = REPO_ROOT / "data" / "training" / "unlabeled_data.csv"
OUTPUT_BACKTEST = REPO_ROOT / "data" / "training" / "backtest.csv"

GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_PARAMS = {
    "mode": "ArtList",
    "format": "json",
    "timespan": "1y",
    "maxrecords": "250",
    "sort": "datedesc",
    "sourcelang": "english",
}

OUTPUT_FIELDS = [
    "event_id",
    "timestamp",
    "headline",
    "text",
    "ticker",
    "label_severity",
    "label_direction",
    "notes",
]

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; LLMTrading/1.0; +https://example.com)"
    }
)

MAX_ARTICLES_PER_COMPANY = 20
PROGRESS_EVERY = 5


def read_companies(path: Path) -> List[Dict[str, str]]:
    companies: List[Dict[str, str]] = []
    if not path.exists():
        return companies

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = (row.get("ticker") or "").strip()
            company_full = (row.get("company_full") or "").strip()
            company_short = (row.get("company_short") or "").strip()
            company_name = company_full or company_short
            if company_name and ticker:
                companies.append(
                    {
                        "company_name": company_name,
                        "company_short": company_short,
                        "ticker": ticker,
                    }
                )
    return companies


def seendate_to_iso(raw: str) -> str:
    if not raw:
        return ""
    try:
        dt = datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


def fetch_article_text(url: str) -> Optional[str]:
    try:
        resp = SESSION.get(url, timeout=(5, 30))
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = " ".join(soup.get_text(separator=" ").split())
    return text.strip() if text else None


def build_query_candidates(company_name: str, company_short: str, ticker: str) -> List[str]:
    candidates: List[str] = []

    def add(query: str) -> None:
        if query and query not in candidates:
            candidates.append(query)

    name = company_name.strip()
    short = company_short.strip()

    # Prefer ticker first for very short names to avoid GDELT errors.
    if len(name) < 3:
        add(ticker)
        add(f"\"{name}\"")
        return candidates

    if any(ch in name for ch in " .,&-"):
        add(f"\"{name}\"")
    add(name)

    if short and short != name:
        if len(short) >= 3 and any(ch in short for ch in " .,&-"):
            add(f"\"{short}\"")
        if len(short) >= 3:
            add(short)

    add(ticker)
    return candidates


def resolve_ticker(headline: str, text: str, url: str) -> str:
    combined_text = f"{headline}\n{text}\n{url}"
    candidates, _sources = extract_tickers(combined_text)
    primary, _scores, _reason = resolve_primary_ticker(headline, text, url, candidates)
    if primary:
        return primary
    return candidates[0] if candidates else ""


def is_corrupted(text: str) -> bool:
    if not text:
        return True
    bad_chars = sum(1 for ch in text if ch not in string.printable)
    if "�" in text:
        bad_chars += 5
    return (bad_chars / max(len(text), 1)) > 0.45


def main() -> None:
    companies = read_companies(COMPANIES_CSV)
    if not companies:
        print("No companies found; check config/company_tickers.csv")
        return

    print(f"Starting: {len(companies)} companies", flush=True)

    OUTPUT_TRAIN.parent.mkdir(parents=True, exist_ok=True)

    for path in (OUTPUT_TRAIN, OUTPUT_BACKTEST):
        if path.exists():
            path.unlink()
    fieldnames = OUTPUT_FIELDS

    seen_urls = set()
    total_written = 0
    total_duplicates = 0
    total_failures = 0
    total_corrupted = 0
    total_no_timestamp = 0

    with OUTPUT_TRAIN.open("w", newline="", encoding="utf-8") as train_f, OUTPUT_BACKTEST.open(
        "w", newline="", encoding="utf-8"
    ) as backtest_f:
        train_writer = csv.DictWriter(train_f, fieldnames=fieldnames)
        backtest_writer = csv.DictWriter(backtest_f, fieldnames=fieldnames)
        train_writer.writeheader()
        backtest_writer.writeheader()

        for idx, company in enumerate(companies, start=1):
            company_name = company["company_name"]
            company_short = company.get("company_short") or ""
            ticker = company["ticker"]
            print(f"Processing {idx}/{len(companies)}: {company_name}", flush=True)

            query_candidates = build_query_candidates(company_name, company_short, ticker)

            articles: List[Dict[str, Any]] = []
            last_status: Optional[int] = None
            last_snippet: str = ""
            for query in query_candidates:
                params = dict(GDELT_PARAMS)
                params["query"] = query
                try:
                    resp = SESSION.get(GDELT_ENDPOINT, params=params, timeout=(5, 30))
                    resp.raise_for_status()
                    last_status = resp.status_code
                    if not resp.content:
                        continue
                    try:
                        data = resp.json()
                    except ValueError:
                        last_snippet = resp.text[:200]
                        continue
                except Exception:
                    time.sleep(0.3)
                    continue

                candidate = data.get("articles") if isinstance(data, dict) else None
                if isinstance(candidate, list) and candidate:
                    articles = candidate
                    break

            if not articles:
                time.sleep(0.3)
                if last_status is not None and last_snippet:
                    print(
                        f"{company_name}: gdelt_articles=0, status={last_status}, "
                        f"snippet={last_snippet}"
                    )
                else:
                    print(f"{company_name}: gdelt_articles=0, fetch_failures=0, written=0")
                continue

            company_fetch_failures = 0
            company_written = 0
            company_rows: List[Dict[str, str]] = []
            for article_idx, article in enumerate(articles[:MAX_ARTICLES_PER_COMPANY], start=1):
                if not isinstance(article, dict):
                    continue
                url = str(article.get("url") or "").strip()
                if not url:
                    continue
                if url in seen_urls:
                    total_duplicates += 1
                    continue

                headline = str(article.get("title") or "").strip()
                # Temporarily allow empty timestamps to diagnose empty output.
                raw_seendate = article.get("seendate")
                timestamp = seendate_to_iso(str(raw_seendate)) if raw_seendate else ""
                if not timestamp:
                    total_no_timestamp += 1
                    timestamp = ""

                text = fetch_article_text(url)
                if not text:
                    total_failures += 1
                    company_fetch_failures += 1
                    if article_idx % PROGRESS_EVERY == 0:
                        print(
                            f"{company_name}: processed {article_idx}/{MAX_ARTICLES_PER_COMPANY} articles",
                            flush=True,
                        )
                    continue

                if is_corrupted(headline) or is_corrupted(text):
                    total_corrupted += 1
                    total_failures += 1
                    continue

                ticker = resolve_ticker(headline, text, url)
                if not ticker:
                    continue

                row = {
                    "event_id": str(uuid.uuid4()),
                    "timestamp": timestamp,
                    "headline": headline,
                    "text": text,
                    "ticker": ticker,
                    "label_severity": "",
                    "label_direction": "",
                    "notes": "",
                }

                company_rows.append(row)
                seen_urls.add(url)

                if article_idx % PROGRESS_EVERY == 0:
                    print(
                        f"{company_name}: processed {article_idx}/{MAX_ARTICLES_PER_COMPANY} articles",
                        flush=True,
                    )

            if company_rows:
                random.shuffle(company_rows)
                split_idx = max(1, int(len(company_rows) * 0.2))
                backtest_rows = company_rows[:split_idx]
                train_rows = company_rows[split_idx:]

                for row in train_rows:
                    train_writer.writerow(row)
                    total_written += 1
                    company_written += 1

                for row in backtest_rows:
                    backtest_writer.writerow(row)
                    total_written += 1
                    company_written += 1

            print(
                f"{company_name}: gdelt_articles={len(articles)}, "
                f"fetch_failures={company_fetch_failures}, written={company_written}",
                flush=True,
            )
            time.sleep(0.3)

    print(f"total companies processed: {len(companies)}")
    print(f"total articles written: {total_written}")
    print(f"total duplicates skipped: {total_duplicates}")
    print(f"total fetch failures: {total_failures}")
    print(f"total missing timestamp: {total_no_timestamp}")
    print(f"total corrupted filtered: {total_corrupted}")


if __name__ == "__main__":
    main()
