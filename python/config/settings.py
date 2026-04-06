import os
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()

##API keys
COIN_GECKO_API_KEY = os.getenv("COIN_GECKO_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

## Coins to track
COINS = ["bitcoin"]


##News keywords to track
NEWS_KEYWORDS = {
    "Bitcoin": ["Bitcoin", "BTC"],
    "Ethereum": ["Ethereum", "ETH"],
    "Ripple": ["Ripple", "XRP"],
    "Litecoin": ["Litecoin", "LTC"],
    "Cardano": ["Cardano", "ADA"],
}

## API endpoints
COIN_GECKO_API_URL = "https://api.coingecko.com/api/v3"
NEWS_API_URL = "https://newsapi.org/v2/everything"

## Rate limits
COIN_GECKO_RATE_LIMIT_PER_SECOND = 10  # 2 calls per second
NEWS_API_RATE_LIMIT_PER_SECOND = 20  # 1 call per second

DATA_ROOT = Path(__file__).parent.parent / "data" / "raw"

PRICES_PATH = DATA_ROOT / "prices"
NEWS_PATH = DATA_ROOT / "news"

LOG_LEVEL = "INFO"

# ── Ingestion modes ───────────────────────────────────────
BACKFILL_DAYS       = 30   # used on first run
INCREMENTAL_DAYS    = 1    # used on every daily run after initial backfill