from datetime import datetime, timezone
import uuid
from airflow.decorators import dag, task
import os
from assets import news_bq_asset
from airflow.sdk import get_current_context

GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
BQ_DATASET_NAME = os.environ['BQ_DATASET']
BQ_NEWS_TABLE = os.environ['BQ_NEWS_TABLE']

@dag(
    dag_id="crypto_news_dag",
    schedule="30 2 * * *", # daily 02:30 AM
    start_date=datetime(2026, 4, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "news", "ingestion"]
)
def crypto_news_dag():
    @task
    def ingest_crypto_news() -> str :
        """
            Pull the news for crypto coins from NewsAPI for current date.
            
            Args:
                The list of coins for which articles will be pulled is defined in config/Setting.py
                target_date : This date is used to find the news articles from-date.
            Return:
                Return the GCS URI for file where articles and corresponding sentiments are stored for all coins.

        """
        from ingestion.ingest_news_articles import run # type: ignore

        file_url = run(lookup_days=30)
        return file_url
    

    @task(outlets = [news_bq_asset])
    def load_to_bq(gcs_url : str)-> None:
        """
            This task loads the article file from previous steps to bq dataset.
            This task is marked with outlet Asset which indicates airflow to trigger the crypto_transform dag.
        """

        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_NEWS_TABLE}"
        context = get_current_context()
        run_date = context['logical_date'].strftime("%Y%m%d%H%M%S")
        job_id = f"load_crypto_price_{run_date}_{uuid.uuid4().hex[:8]}" # unique job id for each run.

        load_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.PARQUET,
            autodetect = True,
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        )

        load_job = client.load_table_from_uri(
            job_config=load_config,
            source_uris=gcs_url,
            destination= table_ref,
            job_id=job_id
        )

        load_job.result()
    
    #Wiring tasks
    gcs_uri = ingest_crypto_news()
    load_to_bq(gcs_url=gcs_uri)


crypto_news_dag()








