# Project Overview
This project builds an event-driven dataset from financial and general news. Articles flow through an ingestion pipeline into a structured CSV and are enriched with extracted article text (when available) plus event-level labels such as category, severity, and direction. Labeling is automated with a language model, and the focus is currently on data generation and labeling rather than trading execution.

# Pipeline Overview
1. Ingest news articles into a combined CSV.
2. Fetch readable article text from URLs using a reader-style extractor (trafilatura).
3. Use a language model to label events in a structured format.
4. Write enriched results to a new CSV for downstream analysis.
5. Future work: model selection, fine-tuning, backtesting, strategy design.

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
- `run_pipeline.py` — orchestrator; optional cleanup then runs all stages in order.
- `rss_ingest.py` — ingest RSS feeds to `data/raw/`.
- `normalize_rss.py` — normalize raw entries to `data/processed/`.
- `text_cleaning_v1.py` — clean headline/text and write to `data/processed_clean/`.
- `ticker_extract_v1.py` — extract explicit tickers, keep confident items, reject ambiguous/no-ticker.
- `company_name_to_ticker_v1.py` — map company names to tickers to fill missing primaries, separate accepted/rejected.
- `build_csv.py` — combine enriched JSON to a timestamped CSV in `data/combined/`.

# Dependencies
- Python 3.9+
- trafilatura
- A language model backend (provider not yet finalized)

Install core dependency:
```bash
python3 -m pip install trafilatura
```

# Model Backend
The project is designed to be model-agnostic. The labeling step can use either a hosted API model or a locally run open-source model. Model choice will be evaluated later based on cost, performance, and reproducibility.

# Current Limitations
- No fine-tuning has been performed yet.
- Labels are generated automatically and may require review.
- Article extraction may fail on some sources (paywalls, JS-heavy sites).
- This stage focuses on building labeled datasets, not live trading.
