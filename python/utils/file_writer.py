import pandas as pd
from pathlib import Path
from datetime import datetime
from utils.logger import get_logger

logger = get_logger("FileWriter")

def write_parquet(df: pd.DataFrame, base_path: Path, partition_date : datetime, file_name: str) -> Path:
    """Write a DataFrame to parquet using Hive-style date partitioning..
       Output structure mirrors future GCS bucket layout:
       {base_path}/year=YYYY/month=MM/day=DD/{filename}.parquet
    Args:
        df (pd.DataFrame): The DataFrame to write.
        base_path (Path): The base path where the Parquet file will be written.
        partition_date (datetime): The date to use for partitioning the data.
        file_name (str): The name of the Parquet file to write.
    Returns:
        file_path (Path): The path to the written Parquet file.
    """
    try:
        output_dir = (base_path/
                     f"year={partition_date.year}" /
                     f"month={partition_date.month:02d}" / 
                     f"day={partition_date.day:02d}")

        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"{file_name}.parquet"
        df.to_parquet(file_path, index=False, engine="pyarrow")
        logger.info(f"File Written | rows= {len(df)} | path={file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed to write data to Parquet: {e}")
        raise RuntimeError(f"Failed to write data to Parquet: {e}")