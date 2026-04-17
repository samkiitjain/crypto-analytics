# CryptoLens
 
The pipeline pulls daily crypto price data from CoinGecko and financial news from NewsAPI, stores
everything in Google Cloud Storage as partitioned parquet files, loads it into BigQuery, and
transforms it through a medallion architecture using dbt. The Streamlit dashboard puts price
movement and news sentiment side by side so you can see whether the market was reacting to
news or ignoring it.
 
It runs fully automated on Airflow 3.0. The transformation step only fires when both the price
and news ingestion jobs have finished successfully — not on a fixed schedule, but triggered by
data availability. That felt like the right way to do it.
 
---
 
## Architecture
 
![Architecture](image-1.png)
 
---
 
## Stack
 
| Layer | Technology |
|---|---|
| Infrastructure | Terraform |
| Cloud | Google Cloud Platform |
| Data lake | Google Cloud Storage |
| Data warehouse | BigQuery |
| Ingestion | Python 3.11 |
| Orchestration | Apache Airflow 3.0 |
| Transformation | dbt Core + dbt-bigquery |
| Sentiment | VADER |
| Dashboard | Streamlit |
| Package management | uv |
| Containerisation | Docker + Docker Compose |
 
---
 
## Project structure
 
```
CRYPTO-ANALYSIS/
├── terraform/                    # Infrastructure provisioning
│   ├── main.tf                   # GCS bucket + BigQuery datasets via for_each
│   ├── variables.tf
│   │── provider.tf
│   └── envs/
│       ├── dev.tfvars
│       ├── staging.tfvars
│       └── prod.tfvars
│
├── python/                       # Ingestion layer
│   ├── config/settings.py        # Centralised config, env-driven
│   ├── utils/                    # Logger, file writer, GCS writer, factory
│   └── ingestion/                # CoinGecko client, NewsAPI client, orchestrators
│
├── airflow/                      # Orchestration
│   ├── dags/
│   │   ├── assets.py             # Asset definitions (event-driven scheduling)
│   │   ├── crypto_prices_daily.py
│   │   ├── crypto_news_daily.py
│   │   └── crypto_transform.py
│   ├── Dockerfile                # Installs dependencies only, code via volume mount
│   └── docker-compose.yml        # All services — Airflow, Postgres, Streamlit
│
├── dbt/                          # Transformation layer
│   ├── models/
│   │   ├── staging/              # Bronze — type casting, surrogate keys, source checks
│   │   │   ├── stg_crypto_prices.sql
│   │   │   └── stg_crypto_news.sql
│   │   ├── intermediate/         # Silver — deduplication, cleaning
│   │   │   ├── int_crypto_prices_cleaned.sql
│   │   │   └── int_crypto_news_cleaned.sql
│   │   └── marts/                # Gold — dimensional model
│   │       ├── dim_coins.sql
│   │       ├── fact_prices.sql
│   │       ├── fact_news.sql
│   │       └── fact_price_sentiment.sql
│   ├── macros/
│   │   └── generate_schema_name.sql  # Mirrors Terraform dataset naming convention
│   ├── dbt_project.yml
│   └── profiles.yml
│
└── streamlit-dashboard/          # Dashboard layer
    ├── Dockerfile
    ├── pyproject.toml
    ├── Home.py                   # Landing page — coin selector, date range, KPI cards
    ├── utils/
    │   └── bq_client.py          # BigQuery query functions with caching
    └── pages/
        ├── 1_Price_Trends.py     # OHLC candlestick chart
        ├── 2_Sentiment.py        # Dual-axis price vs sentiment chart
        └── 3_Correlation.py      # Scatter plot with quadrant analysis
```
 
---
 
## dbt model layers
 
**Bronze (staging)** — views on top of raw tables. Explicit type casts, surrogate keys,
source freshness checks. No business logic.
 
**Silver (intermediate)** — deduplication using ROW_NUMBER() window functions. One clean
row per coin per timestamp for prices, one per article for news.
 
**Gold (marts)** — dimensional model following Kimball conventions:
 
- `dim_coins` — dimension table, one row per coin
- `fact_prices` — incremental fact table, OHLC by coin and timestamp, partitioned by timestamp, clustered by coin
- `fact_news` — incremental fact table, one row per article with VADER sentiment scores
- `fact_price_sentiment` — joined table of price and sentiment for dashboard consumption
Incremental models use `ingested_at` as the watermark — only new rows are processed
on each run, not the full table.
 
---
 
## Dashboard
 
The Streamlit dashboard reads from the gold layer (`crypto_analytics_dev_marts`) and runs
as a Docker container alongside the Airflow stack.
 
**Pages:**
 
- **Home** — coin selector and date range in the sidebar, three KPI cards (latest close,
  period high, period low) loaded from `fact_prices`
- **Price Trends** — OHLC candlestick chart for the selected coin and date range
- **Sentiment** — dual-axis chart overlaying daily close price against average VADER
  sentiment score; sentiment breakdown metrics (positive/negative/neutral day counts)
- **Correlation** — scatter plot of daily sentiment score vs price change %, Pearson
  correlation coefficient, and a quadrant analysis showing directional accuracy
Authentication to BigQuery uses Application Default Credentials — the same gcloud
credentials directory used by Airflow is mounted into the Streamlit container.
 
---
 
## Decisions worth explaining
 
**Terraform owns all infrastructure, dbt owns nothing.**
BigQuery datasets are created by Terraform before any pipeline code runs. dbt is configured to
write into datasets that already exist — if they don't, it fails loudly. This separation keeps
infrastructure auditable, promotable across environments, and decoupled from pipeline execution.
 
**Asset-based scheduling instead of time-based polling.**
The transformation DAG doesn't run on a cron. It listens for completion signals from the two
ingestion DAGs and fires only when both have succeeded. This means the transform layer always
works on complete data — no race condition, no sensor tasks burning resources. Airflow 3.0
makes this clean with its Asset model.
 
**VADER for sentiment instead of a cloud NLP API.**
No third-party dependency on the critical path beyond the two data sources the pipeline already
relies on. VADER runs locally in the ingestion layer, adds no per-call cost, and has no failure
mode beyond the library itself.
 
**Dataset naming mirrors Terraform exactly.**
dbt's `generate_schema_name` macro is overridden to produce dataset names that match what
Terraform creates — `crypto_analytics_{env}_{layer}`. Switching between dev and prod is a
single `--target` flag. No hardcoded environment references exist anywhere in the SQL.
 
**Hive partitioning in GCS.**
Parquet files are written with `year=/month=/day=/` path structure. BigQuery applies partition
pruning on date-filtered queries — only the relevant partitions are scanned. This costs nothing
upfront and avoids pain as data grows.
 
---
 
## Environment promotion
 
The pipeline promotes from dev to prod by changing one flag. Terraform has separate variable
files per environment. dbt routes to the correct datasets based on `--target`.
 
```
dev   → crypto_analytics_dev_raw / _staging / _marts
prod  → crypto_analytics_prod_raw / _staging / _marts
```
 
---
 
## Data quality
 
dbt tests run after every transformation as part of the Airflow DAG:
 
- Uniqueness and not-null checks on all primary keys
- Accepted value validation on sentiment labels
- Source freshness checks — warns if raw data is older than 1 day, errors if older than 2 days
---
 
## Running it
 
```bash
# Provision infrastructure
cd terraform
terraform apply -var-file="envs/dev.tfvars"
 
# Run ingestion locally
cd python
uv run python -m ingestion.ingest_prices
uv run python -m ingestion.ingest_news
 
# Start the full stack — Airflow + Streamlit dashboard
cd airflow
docker compose up -d
 
# Airflow UI        → http://localhost:8080
# Streamlit dashboard → http://localhost:8501
 
# Run dbt manually
cd dbt
dbt deps
dbt run --target dev --profiles-dir . --project-dir .
dbt test --target dev --profiles-dir . --project-dir .
```
 
---
 
## Status
 
| Component | Status |
|---|---|
| Terraform infrastructure | ✅ Complete |
| Python ingestion layer | ✅ Complete |
| Airflow 3.0 orchestration | ✅ Complete |
| dbt transformation layer | ✅ Complete |
| Streamlit dashboard | ✅ Complete |
 
---