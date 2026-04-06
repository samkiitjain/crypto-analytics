from ingestion.coingecko_client import fetch_ohlcv
from datetime import date, datetime, timezone
import pandas as pd
from utils.file_writer import write_parquet
from utils.logger import get_logger
from config.settings import (
    COINS,
    PRICES_PATH,
    INCREMENTAL_DAYS,
    BACKFILL_DAYS
)

log = get_logger("ingest_prices")

def parse_ohlcv(data : list, coin_id: str, ingested_at: date) -> pd.DataFrame:
    """
        This function parses the data recieved from prices API from CoinGecko and generate corresponding parquet file for corresponding day.

        Args:
            data : data from Prices API response.
            coin_id : coin id for CoinGecko
            ingested_at: Ingestion run date for audit purpose.
        
        Return:
            pd.DataFrame : Dataframe consisting of response data which will be written to parquet files.
    """

    records = []
    for row in data:
        records.append({
                "coin_id":        coin_id,
                "timestamp_utc":  pd.to_datetime(row[0], unit="ms", utc=True),
                "open_usd": row[1],
                "high_usd": row[2],
                "low_usd" : row[3],
                "close_usd": row[4],
                "ingested_at": ingested_at,
                "source":"coingecko"
        })
    return pd.DataFrame(records)

def run(coins: list = COINS, days : int = INCREMENTAL_DAYS, target_date: date = None):
    """"
        This is the main function which will be called from Airflow DAG. It will call the price API for each coin and write the response to parquet file
        which is Hive-style partitioned by date. The output structure mirrors future GCS bucket layout.
        {base_path}/year=YYYY/month=MM/day=DD/{filename}.parquet

        Args:
            coins : list of coins to fetch price data for.
            days : whether to fetch value for single days or multiple days.
            target_date: Date for which to fetch price data. If not provided, it will use current date.
    
    """
    target_date = target_date or date.today()
    ingested_at = datetime.now(tz=timezone.utc)
    log.info(f"Starting price ingestion | date={target_date} | days={days}")

    all_frame = []
    for coin in coins:
        try: 
            data = fetch_ohlcv(coin_id=coin, target_date=target_date, days=days)
            df = parse_ohlcv(data=data, coin_id=coin, ingested_at=ingested_at)
            all_frame.append(df)
            log.info(f"Parsed | coin={coin} | rows = {len(df)}")
        except Exception as exception:
            log.error(f"Failed to ingest for coin {coin} at time {ingested_at} due to error {exception}")
    
    if not all_frame:
        log.error(f"No records fetched from Price API, please check the configuration")
        return
    
    combined = pd.concat(all_frame, ignore_index=True)

    output_path = write_parquet(df=combined, base_path=PRICES_PATH, partition_date=target_date, file_name=f"prices_{target_date.isoformat()}")

    log.info(f"Ingestion completed | total_rows = {len(combined)} | output = {output_path} ")

