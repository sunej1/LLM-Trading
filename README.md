# Project Overview
News-driven pipeline for identifying idiosyncratic corporate shock events. Built to study short-horizon overreaction/underreaction using rule-based extraction, LLM labeling, and downstream pricing models. Emphasizes precision over recall to avoid false signals.

# High-Level Pipeline
1. RSS ingestion → fetch configured feeds, write raw JSON to `data/raw/`.
2. Normalization → standardize raw entries into a uniform schema in `data/processed/`.
3. Text cleaning → strip HTML/URLs/boilerplate, write cleaned copies to `data/processed_clean/`.
4. Explicit ticker extraction → cashtags/exchange-form tickers; accepted to `data/processed_primary/`, rejects to `data/rejected/`.
5. Company-name → ticker resolution → map company mentions to tickers (fills blanks); accepted to `data/processed_primary_name/`, rejects to `data/rejected_name/`.
6. CSV export → combine enriched records into `data/combined/combined_<timestamp>.csv`.

# Folder Structure
- `src/ingest/` — ingestion scripts; read configs and write raw feeds.
- `src/clean/` — normalization and text cleaning; read raw, write processed and processed_clean.
- `src/enrich/` — enrichment (explicit ticker extraction, name-to-ticker mapping); read cleaned inputs, write processed_primary/processed_primary_name plus rejects.
- `src/export/` — export utilities; combine processed data to CSV.
- `config/` — YAML/CSV configs (`rss_sources.yaml`, `company_tickers.csv`).
- `data/raw/` — raw RSS dumps from ingestion.
- `data/processed/` — normalized JSON files.
- `data/processed_clean/` — cleaned JSON files with `headline_clean`/`text_clean`.
- `data/processed_primary/` — accepted records from explicit ticker extraction.
- `data/processed_primary_name/` — accepted records from name-based ticker mapping.
- `data/rejected*/` — rejected records (`data/rejected/` for explicit extraction, `data/rejected_name/` for name mapping).
- `data/combined/` — consolidated CSV outputs.

# Key Scripts
- `rss_ingest.py` — filter; ingests RSS feeds to `data/raw/`.
- `normalize_rss.py` — filter; normalizes raw entries to `data/processed/`.
- `text_cleaning_v1.py` — filter; cleans headline/text and writes to `data/processed_clean/`.
- `ticker_extract_v1.py` — enrich/filter; extracts explicit tickers, keeps confident items, rejects ambiguous/no-ticker.
- `company_name_to_ticker_v1.py` — enrich/filter; maps company names to tickers to fill missing primaries, separates accepted/rejected.
- `build_csv.py` — export; combines enriched JSON to a timestamped CSV in `data/combined/`.
- `run_pipeline.py` — orchestrator; optional cleanup then runs all stages in order.

# Ticker Resolution Logic
- Mainstream news often omits explicit tickers. The pipeline first attempts explicit extraction (cashtags, exchange prefixes, quote URLs).
- Remaining blanks rely on company-name mapping: headline/body mentions matched against known company names produce a primary ticker when confident.
- Rejected files capture ambiguous or no-match cases. High precision is intentional; many articles are dropped to avoid false positives.

# Configuration Files
- `rss_sources.yaml` — list of RSS sources; update URLs/names to adjust ingestion coverage.
- `company_tickers.csv` — columns `ticker`, `company_full`, `company_short`; expand with additional firms for name-based resolution. Keep headers intact and ensure names are accurate.

# How to Run
```
python run_pipeline.py [--clean | --clean-force]
```
- `--clean` prompts before clearing derived outputs; `--clean-force` skips confirmation. Cleanup affects processed/cleaned/enriched/combined outputs, not raw or configs.
- Expected outputs: raw JSON in `data/raw/`, normalized in `data/processed/`, cleaned in `data/processed_clean/`, primary ticker outputs in `data/processed_primary/`, name-based fills in `data/processed_primary_name/`, rejects in `data/rejected/` and `data/rejected_name/`, final CSV in `data/combined/combined_<timestamp>.csv`.
- Success: step-by-step completion messages ending with “Pipeline complete. Combined CSV is ready.”

# Current Limitations
- Free RSS sources can have latency and gaps.
- Company mapping is incomplete; coverage grows as `company_tickers.csv` expands.
- Price data not yet integrated.
- Precision-first design trades recall for cleaner signals.

# Future Work
- Price-reaction integration.
- LLM labeling.
- Automated backtests.
- Improved name disambiguation.
