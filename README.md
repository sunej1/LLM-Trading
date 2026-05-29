# Overview
This repository builds an end-to-end news event pipeline for CAPM/news event research:
ingest market news, normalize/clean text, enrich with tickers, label events with a local
LLM, compute CAPM + news-adjusted mispricing, and run held-out intraday backtests.

Live trading is not implemented. The current focus is data preparation, event-level CAPM
modeling, and backtesting.

# Pipeline Diagram
- Ingest configured news sources -> raw JSON (`src/news/ingest/rss_ingest.py`)
- Normalize schema -> normalized JSON (`src/news/clean/normalize_rss.py`)
- Clean text -> cleaned JSON (`src/news/clean/text_cleaning_v1.py`)
- Ticker extraction -> primary/rejected JSON (`src/news/enrich/ticker_extract_v1.py`)
- Name-based ticker resolution -> primary/rejected JSON (`src/news/enrich/company_name_to_ticker_v1.py`)
- Export combined CSV (+ article excerpt) (`src/news/export/build_csv.py`)
- LLM labeling Stage A -> labeled CSV (`src/llm/label_csv_stage_a.py`)
- CAPM + news-adjusted mispricing -> event-level CSV (`src/modeling/capm_news_mispricing.py`)
- Optional held-out backtest (`src/backtesting/label_backtest_events.py`, `src/backtesting/capm_backtest.py`)

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
│   ├── backtesting
│   │   ├── capm_backtest.py
│   │   ├── event_backtest.py
│   │   └── label_backtest_events.py
│   ├── llm
│   │   ├── __init__.py
│   │   ├── label_csv_stage_a.py
│   │   └── llm_backend.py
│   ├── modeling
│   │   └── capm_news_mispricing.py
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

# Quickstart
Run the full live-data preparation pipeline:
```bash
python3 run_pipeline.py
```

This creates:
- `data/combined/combined.csv`
- `data/combined/combined_labeled.csv`
- `data/combined/capm_event_level_mispricing.csv`

Optional cleanup:
```bash
python3 run_pipeline.py --clean
# or
python3 run_pipeline.py --clean-force
```

To run the live pipeline and then the held-out backtest:
```bash
python3 run_pipeline.py --run-backtest
```

If the held-out backtest file has not been labeled yet:
```bash
python3 run_pipeline.py --run-backtest --label-backtest
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
python3 src/llm/label_csv_stage_a.py --in data/combined/combined.csv --out data/combined/combined_labeled.csv
python3 src/modeling/capm_news_mispricing.py
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

The script prints a start/finish line for each row so long labeling runs show progress.

# CAPM + News Mispricing
Input file: `data/combined/combined_labeled.csv`

Output file:
```text
data/combined/capm_event_level_mispricing.csv
```

The model computes one output row per labeled event row. It does not aggregate by date and
does not edit the source CSV.

For each event, it:
- finds the most recent trading bar before the event
- finds the first trading bar after the event
- estimates CAPM beta using prior price history
- computes CAPM fair return
- applies the event severity/direction adjustment
- computes news-adjusted fair price and mispricing

Severity midpoint mapping:
- `1 -> 0.005`
- `2 -> 0.02`
- `3 -> 0.05`
- `4 -> 0.11`
- `5 -> 0.18`

# Backtesting
Backtesting is split into two steps so the slow LLM pass does not need to be rerun every
time the CAPM trade logic changes.

Step 1: label held-out backtest events:
```bash
python3 src/backtesting/label_backtest_events.py
```

Input:
```text
data/training/backtest.csv
```

Output:
```text
data/training/backtest_labeled.csv
```

Step 2: run the CAPM backtest:
```bash
python3 src/backtesting/capm_backtest.py
```

Outputs:
```text
data/training/backtest_event_mispricing.csv
data/training/backtest_event_results.csv
data/training/backtest_summary.csv
```

Default trade timing:
- Reference bar: last 15-minute bar before the news timestamp
- Entry bar: first 15-minute bar after the news timestamp
- Exit: fair value hit, stop loss, or max-hold expiry

Current default trade parameters:
- `--price-interval 15m`
- `--stop-loss-pct 0.05`
- `--max-hold-bars 52`
- `--min-edge-pct 0.001`
- `--max-target-distance-pct 0.10`

Example with a shorter Yahoo interval:
```bash
python3 src/backtesting/capm_backtest.py --price-interval 5m
```

Yahoo intraday data is limited. If Yahoo Finance does not have the requested intraday
data for a ticker or event window, the row is marked `trade_side=skipped` and ignored in
strategy returns.

## Parameter Tuning
Run chronological k-fold cross-validation over the trade parameter grid:
```bash
python3 src/backtesting/capm_backtest.py --tune-params
```

Output:
```text
data/training/backtest_param_tuning.csv
```

Useful options:
```bash
python3 src/backtesting/capm_backtest.py \
  --tune-params \
  --cv-folds 5 \
  --min-cv-trades 5
```

The current default trade parameters were selected from this tuning mode on the current
`data/training/backtest_labeled.csv`. Retune when the backtest dataset changes materially.

# Data & Models (Git Hygiene)
Large artifacts are local-only and gitignored:
- `data/` (processing outputs, combined exports, labeled datasets)
- `models/` (GGUF model files)
- `*.gguf`, `*.bin`, `*.pt`, `*.pth`, `*.safetensors`

Download the GGUF model separately and place it under `models/` (e.g., `models/llama-3.1-8b.gguf`).
Current local model: LLaMA 3.1 8B Q5 GGUF.

# Implemented vs Live Trading
Implemented now:
- News ingestion, normalization, text cleaning
- Ticker enrichment (explicit + name-based)
- CSV export with article excerpts
- LLM labeling Stage A
- Event-level CAPM + news-adjusted mispricing
- Held-out CAPM intraday backtesting
- K-fold parameter tuning

## Live Trading (Planned)
Live trading is not implemented yet. The goal is to support live signal generation after backtesting and validation.

# Roadmap / Next Steps
- Stabilize LLM labeling outputs and schema validation
- Add richer backtesting metrics and risk controls
- Expand company ticker mappings and source coverage
- Add live trading integration after backtesting results

# Notes / Troubleshooting
- `data/` and `models/` are intentionally ignored; do not expect them to show up in git status.
- If you push to GitHub, large files won’t be included; download models locally.
- If Stage A labels are empty, verify that `llama-cli` is producing JSON for the prompt.
