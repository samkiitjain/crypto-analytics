from airflow.decorators import dag, task
from datetime import datetime, timezone
from airflow.sdk import get_current_context
from pandas_gbq import context
from assets import prices_bq_asset
import os
import uuid


GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
BQ_DATASET_NAME = os.environ['BQ_DATASET']
BQ_PRICES_TABLE_NAME = os.environ['BQ_PRICES_TABLE']

@dag(
    dag_id = "crypto_prices_dag",
    description = "Dag that pull the crypto prices from CoinGecko and process them daily",
    schedule = "0 2 * * *",
    start_date=datetime(2026, 4, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "prices"],
)
def process_crypto_price():
    @task
    def ingest_prices() -> str:
        """
            Pull the prices for crypto coins from CoinGecko for current date.
            
            Args:
                The list of coins for which prices will be pulled is defined in config/Setting.py
                If we pass target_days param : it will pull for more X number of days in history. This is because free tier API doesn't allow
                    date based look up to do traditional backfill.
                target_date : This date is used for tracking and logging purpose.
            Return:
                Return the GCS URI for file prices stored for all coins.

        """
        from ingestion.ingest_prices import run  # type: ignore
        
        context = get_current_context()
        logical_date = context['logical_date']
        gcs_uri = run(days=30, target_date=logical_date.date())
        return gcs_uri
    
    @task(outlets= [prices_bq_asset])
    def load_to_bigquery(gcs_uri : str) -> None:
        """
            This function loads the prices parquet file in bq dataset. 
            Marking this task with outlet = [prices_bq_asset] to indicate to airflow that the asset has been updated and
            it can trigger crypto_transform dag.
        """

        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_PRICES_TABLE_NAME}"
        
        
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
            job_id=job_id,
            source_uris=gcs_uri,
            destination= table_ref,
            job_config=load_config,
        )

        load_job.result() #wait untill job completes.

    #Xcom wiring
    gcs_url = ingest_prices()
    load_to_bigquery(gcs_uri=gcs_url)

process_crypto_price()


