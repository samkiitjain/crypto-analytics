from config.settings import COIN_GECKO_API_KEY, COIN_GECKO_API_URL, COIN_GECKO_RATE_LIMIT_PER_SECOND
from utils.logger import get_logger
import requests
from datetime import date
import time


logger = get_logger("Coin_GeckoClient")

def fetch_ohlcv(coin_id : str, target_date: date, days: int = 1) -> list :
    """
        Fetching OHLCV data for a single coin last either last 24hrs or 30 days. It depends on ingestion mode.
        CoinGecko responds with OHLCV values for 4 hr window in each 24 hrs for free tier.
        The response format is as below:
        [timestamp in ms, opening_value, highest_value, lowest_value, close_value]

        Args:
            coin_id : CoinGecko ID for coins. f.x bitcoin
            target_date: date for logging and traceability.
            days : whether to fetch value for single days or multiple days.
        
        Returns:
            List of OHCLV records for given time period.
        
        Raise:
            HTTPError: on any non 200, HTTP error code from API after retry.
    """
    endpoint = f"{COIN_GECKO_API_URL}/coins/{coin_id}/ohlc"
    params = {
        "vs_currency": "usd",
        "days": days
    }

    headers = {
        "x_cg_demo_api_key": COIN_GECKO_API_KEY
    }

    logger.info(f"Fetching OHLCV | coin_id={coin_id} | date={target_date} | days={days}")

    response = requests.get(endpoint, params=params, headers=headers, timeout=10)

    if response.status_code == 429:
        logger.warning(f"Rate limited by API | coin = {coin_id} | sleeping 60s")
        time.sleep(60)
        response = requests.get(endpoint, params=params, headers=headers, timeout=10)
    
    response.raise_for_status()

    data = response.json()
    logger.info(f"Recieved {len(data)} OHCLV records | coin = {coin_id} | days = {days}")

    time.sleep(COIN_GECKO_RATE_LIMIT_PER_SECOND)

    return data
