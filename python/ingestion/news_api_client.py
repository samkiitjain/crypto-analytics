from config.settings import NEWS_API_KEY, NEWS_API_URL, NEWS_PAGE_SIZE, NEWS_EXCLUDED_DOMAINS, NEWS_API_RATE_LIMIT_PER_SECOND
from utils.logger import get_logger
import requests
from datetime import date
import time


log = get_logger("news_api_client")

def fetch_news_articles(keyword:str, from_date : date)->list :
    """
     Fetches new articles for crypto coins from NewsAPI. This information will used in sentiment analysis.
     Uses /v2/everything endpoint, which searches across all sources. Auth is via headers.

     Args:
        keyword :search term e.g 'Bitcoin' or 'BTC'
        from_date : Date from which we are looking news article.
    
    Returns : List of articles

    Raise:
        HTTPError for any non-200 responses.
    """
    endpoint = f"{NEWS_API_URL}"
    parameter = {
        "q": keyword,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": NEWS_PAGE_SIZE,
        "from": from_date.isoformat,
        "excludeDomains" : NEWS_EXCLUDED_DOMAINS
    }

    header = {
        "X-Api-Key" : NEWS_API_KEY
    }

    response = requests.get(url=endpoint, params=parameter, headers=header, timeout=10)

    if response.status_code == 429:
        log.warning(f"Rate limit hit for keyword {keyword} at time {from_date}")
        time.sleep(NEWS_API_RATE_LIMIT_PER_SECOND)
        response = requests.get(url=endpoint, params=parameter, headers=header, timeout=10)
    
    response.raise_for_status()

    data = response.json()
    articles = data.get("articles", [])
    total = data.get("totalResults", 0)

    log.info(f"Fetched {len(articles)} articles | keyword = {keyword} | total_available = {total}")
    time.sleep(NEWS_API_RATE_LIMIT_PER_SECOND)
