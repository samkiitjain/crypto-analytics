from airflow.sdk import Asset

prices_bq_asset = Asset("bigquery://crypto-analytics-2026/crypto_analytics_dev_raw/prices")
news_bq_asset   = Asset("bigquery://crypto-analytics-2026/crypto_analytics_dev_raw/news")