# Overview
This repository builds an end-to-end news event pipeline: ingest live RSS news, normalize/clean text, enrich with tickers and metadata, and label events with structured LLM outputs (category, severity, direction, time horizons). The output is a combined CSV designed for downstream modeling. The long-term goal is a strategy pipeline that supports backtesting and, eventually, live trading. Current focus is data generation, enrichment, and labeling.

# Pipeline Diagram
- Ingest RSS feeds -> raw JSON (`src/news/ingest/rss_ingest.py`)
- Normalize schema -> normalized JSON (`src/news/clean/normalize_rss.py`)
- Clean text -> cleaned JSON (`src/news/clean/text_cleaning_v1.py`)
- Ticker extraction -> primary/rejected JSON (`src/news/enrich/ticker_extract_v1.py`)
- Name-based ticker resolution -> primary/rejected JSON (`src/news/enrich/company_name_to_ticker_v1.py`)
- Export combined CSV (+ article excerpt) (`src/news/export/build_csv.py`)
- LLM labeling Stage A -> labeled CSV (`src/llm/label_csv_stage_a.py`)

# Repository Layout
```
.
├── config
│   ├── company_tickers.csv
│   └── rss_sources.yaml
├── data
│   ├── combined
│   └── processing
├── src
│   ├── llm
│   │   ├── __init__.py
│   │   ├── label_csv_stage_a.py
│   │   └── llm_backend.py
│   └── news
│       ├── clean
│       │   ├── normalize_rss.py
│       │   └── text_cleaning_v1.py
│       ├── enrich
│       │   ├── company_name_to_ticker_v1.py
│       │   └── ticker_extract_v1.py
│       ├── export
│       │   ├── build_csv.py
│       └── ingest
│           └── rss_ingest.py
├── utils
│   └── article_extraction.py
├── run_pipeline.py
└── README.md
```

# Setup
Python version: 3.9+

Create a venv:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:
```bash
python3 -m pip install -r requirements.txt
```

Notes:
- The LLM labeling stage calls a local `llama-cli` binary (llama.cpp). Ensure it is installed and in `PATH`.

# Quickstart (End-to-end)
Run the full news pipeline (ingest -> clean -> enrich -> export):
```bash
python3 run_pipeline.py
```

Optional cleanup:
```bash
python3 run_pipeline.py --clean
# or
python3 run_pipeline.py --clean-force
```

Then run LLM labeling Stage A:
```bash
python3 src/llm/label_csv_stage_a.py \
  --in data/combined/combined.csv \
  --out data/combined/combined_labeled.csv
```

# How to Run Each Stage
These scripts can be run individually if you want to inspect intermediate outputs:
```bash
python3 src/news/ingest/rss_ingest.py
python3 src/news/clean/normalize_rss.py
python3 src/news/clean/text_cleaning_v1.py
python3 src/news/enrich/ticker_extract_v1.py
python3 src/news/enrich/company_name_to_ticker_v1.py
python3 src/news/export/build_csv.py
```

# LLM Labeling Stage A
Input file: `data/combined/combined.csv`

Expected input columns include:
- event_id, timestamp, source, headline, text, url
- ticker, ticker_confidence, source_credibility
- category, label_severity, label_direction
- label_time_horizon_1_min, label_time_horizon_2_min
- article_excerpt, article_char_count, article_fetch_status

Stage A output fills:
- category
- label_severity
- label_direction
- label_time_horizon_1_min
- label_time_horizon_2_min
- label_confidence
- label_needs_review

The labeling script sends a compact prompt and expects a strict JSON object with keys:
`category`, `label_severity`, `label_direction`, `label_time_horizon_1_min`, `label_time_horizon_2_min`, `confidence`, `needs_review`.

# Data & Models (Git Hygiene)
Large artifacts are local-only and gitignored:
- `data/` (processing outputs, combined exports, labeled datasets)
- `models/` (GGUF model files)
- `*.gguf`, `*.bin`, `*.pt`, `*.pth`, `*.safetensors`

Download the GGUF model separately and place it under `models/` (e.g., `models/llama-3.1-8b.gguf`).
Current local model: LLaMA 3.1 8B Q5 GGUF.

# Backtesting vs Live Trading
Implemented now:
- RSS ingestion, normalization, text cleaning
- Ticker enrichment (explicit + name-based)
- CSV export with article excerpts
- LLM labeling Stage A

Planned:
- Backtesting pipeline and evaluation
- Strategy design and execution

## Live Trading (Planned)
Live trading is not implemented yet. The goal is to support live signal generation after backtesting and validation.

# Roadmap / Next Steps
- Stabilize LLM labeling outputs and schema validation
- Add backtesting utilities and metrics
- Expand company ticker mappings and source coverage
- Add live trading integration after backtesting results

# Notes / Troubleshooting
- `data/` and `models/` are intentionally ignored; do not expect them to show up in git status.
- If you push to GitHub, large files won’t be included; download models locally.
- If Stage A labels are empty, verify that `llama-cli` is producing JSON for the prompt.
