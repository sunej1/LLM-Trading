"""Ingest stage: fetch configured RSS feeds and store raw entries into data/processing/raw as JSON."""
from __future__ import annotations

import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

import feedparser
import requests
import yaml


GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_GDELT_PARAMS = {
    "mode": "ArtList",
    "format": "json",
    "timespan": "7d",
    "maxrecords": "100",
    "sort": "datedesc",
    "sourcelang": "english",
}
GDELT_TIMEOUT = (10, 60)
GDELT_SOURCE_DELAY_SECONDS = 1.0

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; LLMTrading/1.0; +https://example.com)"
    }
)


def load_config(config_path: Path) -> dict[str, Any] | None:
    """Read YAML config of RSS sources; return parsed dict or None on error."""
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
    """Download RSS feed content for a source; return raw bytes or None on HTTP failure."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        print(f"Failed to fetch RSS feed for '{source_name}' ({url}): {exc}")
        return None


def seendate_to_rfc822(raw: str) -> str:
    """Convert GDELT seendate values into RSS-style RFC 822 timestamps."""
    if not raw:
        return ""

    raw = raw.strip()
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%dT%H%M%SZ"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            continue
    return ""


def gdelt_article_to_feed_entry(article: dict[str, Any]) -> dict[str, Any]:
    """Map a GDELT article into the raw RSS-like fields used by normalize_rss.py."""
    title = str(article.get("title") or "").strip()
    url = str(article.get("url") or "").strip()
    domain = str(article.get("domain") or article.get("sourceCommonName") or "").strip()
    seendate = str(article.get("seendate") or article.get("seenDate") or "").strip()

    # GDELT ArtList does not provide article body text. Keep summary lightweight;
    # downstream article excerpt fetching still uses the URL during CSV export.
    summary_parts = [title]
    if domain:
        summary_parts.append(f"Source: {domain}")

    return {
        "title": title,
        "summary": " ".join(part for part in summary_parts if part),
        "description": "",
        "link": url,
        "published": seendate_to_rfc822(seendate),
        "gdelt_seendate": seendate,
        "gdelt_domain": domain,
        "gdelt_source_country": article.get("sourceCountry", ""),
        "gdelt_language": article.get("language", ""),
    }


def fetch_gdelt_articles(source: dict[str, Any]) -> List[dict[str, Any]] | None:
    """Fetch one configured GDELT query and return RSS-like raw entries."""
    source_name = source.get("name", "gdelt_source")
    query = str(source.get("query") or "").strip()
    if not query:
        print(f"Skipping GDELT source '{source_name}': missing query.")
        return None

    params = dict(DEFAULT_GDELT_PARAMS)
    for field in ("timespan", "maxrecords", "sort", "sourcelang"):
        if source.get(field) is not None:
            params[field] = str(source[field])
    params["query"] = query

    try:
        response = SESSION.get(GDELT_ENDPOINT, params=params, timeout=GDELT_TIMEOUT)
        if response.status_code == 429:
            time.sleep(2.0)
            response = SESSION.get(GDELT_ENDPOINT, params=params, timeout=GDELT_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        print(f"Failed to fetch GDELT source '{source_name}': {exc}")
        return None
    except ValueError as exc:
        print(f"Failed to parse GDELT JSON for '{source_name}': {exc}")
        return None

    articles = data.get("articles") if isinstance(data, dict) else None
    if not isinstance(articles, list):
        print(f"GDELT source '{source_name}' returned no article list.")
        return []

    entries = []
    seen_urls = set()
    for article in articles:
        if not isinstance(article, dict):
            continue
        entry = gdelt_article_to_feed_entry(article)
        url = entry.get("link")
        if not url or url in seen_urls:
            continue
        entries.append(entry)
        seen_urls.add(url)
    return entries


def read_companies(path: Path, max_companies: Optional[int] = None) -> List[dict[str, str]]:
    """Load company names from the existing ticker mapping for GDELT watchlist queries."""
    companies: List[dict[str, str]] = []
    if not path.exists():
        print(f"Company ticker config not found: {path}")
        return companies

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = (row.get("ticker") or "").strip()
            company_full = (row.get("company_full") or "").strip()
            company_short = (row.get("company_short") or "").strip()
            company_name = company_full or company_short
            if ticker and company_name:
                companies.append(
                    {
                        "ticker": ticker,
                        "company_name": company_name,
                        "company_short": company_short,
                    }
                )
            if max_companies is not None and len(companies) >= max_companies:
                break
    return companies


def build_query_candidates(company_name: str, company_short: str, ticker: str) -> List[str]:
    """Build GDELT query candidates using the same idea as the training data collector."""
    candidates: List[str] = []

    def add(query: str) -> None:
        if query and query not in candidates:
            candidates.append(query)

    name = company_name.strip()
    short = company_short.strip()
    if len(name) < 3:
        add(ticker)
        add(f'"{name}"')
        return candidates

    add(f'"{name}"' if any(ch in name for ch in " .,&-") else name)
    if short and short != name and len(short) >= 3:
        add(f'"{short}"' if any(ch in short for ch in " .,&-") else short)
    add(ticker)
    return candidates


def fetch_gdelt_company_watchlist(
    source: dict[str, Any],
    project_root: Path,
) -> List[dict[str, Any]] | None:
    """Fetch recent GDELT articles for companies from config/company_tickers.csv."""
    source_name = source.get("name", "gdelt_company_watchlist")
    max_companies_raw = source.get("max_companies")
    max_companies = int(max_companies_raw) if max_companies_raw else None
    maxrecords_per_query = int(source.get("maxrecords_per_query", 5))
    companies = read_companies(project_root / "config" / "company_tickers.csv", max_companies)
    if not companies:
        return []

    all_entries: List[dict[str, Any]] = []
    seen_urls = set()
    for company in companies:
        articles: List[dict[str, Any]] = []
        for query in build_query_candidates(
            company["company_name"],
            company["company_short"],
            company["ticker"],
        ):
            candidate_source = dict(source)
            candidate_source["name"] = source_name
            candidate_source["query"] = query
            candidate_source["maxrecords"] = maxrecords_per_query
            fetched = fetch_gdelt_articles(candidate_source)
            if fetched:
                articles = fetched
                break

        for entry in articles:
            url = entry.get("link")
            if not url or url in seen_urls:
                continue
            entry["gdelt_query_ticker"] = company["ticker"]
            entry["gdelt_query_company"] = company["company_name"]
            all_entries.append(entry)
            seen_urls.add(url)

    return all_entries


def _json_fallback(obj: Any) -> str:
    """Serialize otherwise non-JSONable objects by stringifying them."""
    return str(obj)


def main() -> None:
    """Load source config, fetch each source, and write raw JSON entries to data/processing/raw."""
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent.parent
    config_path = project_root / "config" / "rss_sources.yaml"

    config = load_config(config_path)
    if not config:
        return

    sources = config.get("rss_sources") or []
    if not sources:
        print("No sources found in config.")
        return

    output_dir = project_root / "data" / "processing" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    success_count = 0
    failure_count = 0
    files_written = 0

    for source in sources:
        source_name = source.get("name", "source")
        source_type = str(source.get("source_type") or source.get("type") or "rss").lower()

        if source_type == "gdelt":
            entries = fetch_gdelt_articles(source)
            if entries is None:
                failure_count += 1
                continue
        elif source_type == "gdelt_company_watchlist":
            entries = fetch_gdelt_company_watchlist(source, project_root)
            if entries is None:
                failure_count += 1
                continue
        else:
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
            if source_type.startswith("gdelt"):
                time.sleep(GDELT_SOURCE_DELAY_SECONDS)
        except Exception as exc:
            print(f"Failed to write output file for '{source_name}' ({output_path}): {exc}")
            failure_count += 1

    print(
        f"Summary: {success_count} sources processed successfully, "
        f"{failure_count} failed, {files_written} JSON files written."
    )


if __name__ == "__main__":
    main()
