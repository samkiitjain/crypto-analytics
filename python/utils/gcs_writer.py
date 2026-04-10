import datetime
from google.cloud import storage
import pandas as pd
from pathlib import Path
from utils.logger import get_logger
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings import GCP_PROJECT_ID, GCP_BUCKET_NAME


log = get_logger("GCSWriter")
_client: storage.Client | None = None

def get_client() -> storage.Client:
    """
    Get a Google Cloud Storage client instance.

    Returns:
        storage.Client: An instance of the GCS client.
    """
    global _client
    if _client is None:
        _client = storage.Client(project=GCP_PROJECT_ID)
        log.info(f"Initialized new GCS client for project: {GCP_PROJECT_ID}")

    return _client

def upload_to_gcs(df: pd.DataFrame, base_path: Path, partition_date : datetime, file_name: str) -> str:
    """
      This function takes the dataframe and stream it to GCS storage bucket path.
      It first build the object name using the file name and base path used in the flow
      f.x : 
            base_path = "C:/data/raw/prices"
            file_name = "bitcoin"
            partition_date = 2024-01-01
            object_name would be "prices/bitcoin/year=2024/month=01/day=01/bitcoin.parquet"
        Args:
            df (pd.DataFrame): The DataFrame to write.
            base_path (Path): The base path where the Parquet file will be written.
            partition_date (datetime): The date to use for partitioning the data.
            file_name (str): The name of the Parquet file to write.
        Returns:
            str: The GCS path to the uploaded Parquet file.
    """
    local_path_marker = "data/"

    path = base_path.as_posix()
    if local_path_marker not in path:
        raise ValueError(
            f"base_path '{base_path}' does not contain expected marker '{local_path_marker}'. "
            f"Cannot derive GCS object path."
        )

    relative_name = path.split(local_path_marker, 1)[1]

    blob_file_name = f"{relative_name}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/{file_name}.parquet"

    gcs_client = get_client()
    blob = gcs_client.bucket(bucket_name=GCP_BUCKET_NAME).blob(blob_name=blob_file_name)

    # Streams directly to GCS — no local temp file needed.
    # NOTE: pa.Table.from_pandas() holds the full table in memory before streaming.
    # Acceptable for this pipeline (daily OHLCV, small volume).
    # For multi-GB DataFrames, switch to chunked ParquetWriter.
    table = pa.Table.from_pandas(df)
    
    try:
        with blob.open("wb") as f:
            pq.write_table(table, f)
    except Exception as e:
        log.error(f"Failed to upload to GCS | path={blob_file_name} | error={e}")
        raise RuntimeError(f"Failed to upload to GCS: {e}")
    
    gcs_uri = f"gs://{GCP_BUCKET_NAME}/{blob_file_name}"
    log.info("Uploaded to GCS", gcs_uri=gcs_uri, rows=len(df))
    return gcs_uri


