PROJECT OVERVIEW
News-driven pipeline for identifying idiosyncratic corporate shock events. Built to study short-horizon overreaction/underreaction using rule-based extraction, LLM labeling, and downstream pricing models. Emphasizes precision over recall to avoid false signals.

HIGH-LEVEL PIPELINE
RSS ingestion -> normalization -> text cleaning -> explicit ticker extraction -> company-name -> ticker resolution -> CSV export
1) RSS ingestion: fetch configured feeds and write raw JSON snapshots to data/raw.
2) Normalization: standardize raw entries into a uniform schema under data/processed.
3) Text cleaning: strip HTML/URLs/boilerplate and add cleaned fields to data/processed_clean.
4) Explicit ticker extraction: pull cashtags/exchange-form tickers and keep confident rows in data/processed_primary; reject ambiguous/no-ticker rows.
5) Company-name -> ticker resolution: map company mentions to tickers (fills blanks) and write accepted to data/processed_primary_name and rejects to data/rejected_name.
6) CSV export: combine normalized records into a single CSV snapshot under data/combined.

FOLDER STRUCTURE
src/ingest/ - ingestion scripts; read configs and write raw feeds.
src/clean/ - normalization and text cleaning; read raw, write processed and processed_clean.
src/enrich/ - enrichment steps (explicit ticker extraction, name-to-ticker mapping); read cleaned inputs, write processed_primary and processed_primary_name plus rejects.
src/export/ - export utilities; combine processed data to CSV.
config/ - YAML/CSV configs (RSS sources, company-to-ticker mapping).
data/raw/ - raw RSS dumps from ingestion.
data/processed/ - normalized JSON files.
data/processed_clean/ - cleaned JSON files with headline_clean/text_clean.
data/processed_primary/ - accepted records from explicit ticker extraction.
data/processed_primary_name/ - accepted records from name-based ticker mapping.
data/rejected*/ - rejected records (ticker_extract_v1 to data/rejected, name mapping to data/rejected_name).
data/combined/ - consolidated CSV outputs.

KEY SCRIPTS
rss_ingest.py - filter; ingests RSS feeds to data/raw.
normalize_rss.py - filter; normalizes raw entries to data/processed.
text_cleaning_v1.py - filter; cleans headline/text and writes to data/processed_clean.
ticker_extract_v1.py - enrich/filter; extracts explicit tickers, keeps confident items, rejects ambiguous/no-ticker.
company_name_to_ticker_v1.py - enrich/filter; maps company names to tickers to fill missing primaries, separates accepted/rejected.
build_csv.py - export; combines normalized JSON to a timestamped CSV.
run_pipeline.py - orchestrator; runs all stages in order.

TICKER RESOLUTION LOGIC
Most mainstream news omits explicit tickers. The pipeline first attempts explicit extraction (cashtags, exchange prefixes, quote URLs). Remaining blanks rely on company-name mapping: headline/body mentions matched against known company names produce a primary ticker when confident. Rejected files capture ambiguous or no-match cases. High precision is intentional; many articles are dropped to avoid false positives.

CONFIGURATION FILES
rss_sources.yaml - list of RSS sources; update URLs/names to adjust ingestion coverage.
company_tickers.csv - ticker, company_full, company_short columns; expand with additional firms/aliases for name-based resolution. Keep headers intact and ensure names are accurate.

HOW TO RUN THE PROJECT
Command: python run_pipeline.py
Expected outputs: raw JSON in data/raw, normalized in data/processed, cleaned in data/processed_clean, primary ticker outputs in data/processed_primary, name-based fills in data/processed_primary_name, rejects in data/rejected and data/rejected_name, final CSV in data/combined (timestamped). Success: pipeline prints step-by-step completion messages and ends with “Pipeline complete. Combined CSV is ready.”; final CSV appears in data/combined.

CURRENT LIMITATIONS
Free RSS latency and occasional gaps; company mapping is incomplete; price data not yet integrated; precision-first design trades recall for cleaner signals.

FUTURE WORK
Price-reaction integration; LLM labeling; automated backtests; improved name disambiguation.
