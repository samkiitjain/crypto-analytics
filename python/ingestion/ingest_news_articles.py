from datetime import date, datetime, timezone, timedelta
from utils.file_writer import write_parquet
from utils.logger import get_logger
from config.settings import (INCREMENTAL_DAYS, NEWS_KEYWORDS, NEWS_PATH, COINS, STORAGE_TARGET)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import hashlib
import pandas as pd
from ingestion.news_api_client import fetch_news_articles

log = get_logger("ingest_news_articles")


##Initializing at module level once.
analyzer = SentimentIntensityAnalyzer()

def score_sentiment(text: str) -> dict :
    """
        Score sentiments using VADER. It return 4 scores.
        -neg : negative sentiment (0.0 to 1.0)
        -nue : neutral sentiment  (0.0 to 1.0)
        -pos : postive sentiment  (0.0 to 1.0)
        -compount : overal score (-1.0 to 1.0)
                    -1.0 = most negative
                     0.0 = neutral
                    +1.0 = most positive

    We use compound as the primary signal in analytics.

    Args:
        text : article name + article description
    
    Returns:
        Dict with compound, neg, pos, neu scores
    """

    score = analyzer.polarity_scores(text=text)
    return {
        "sentiment_compound":score['compound'],
        "sentiment_positive":score['pos'],
        "sentiment_negative":score['neg'],
        "sentiment_neutral": score ['neu'],
        "sentiment_label" : classify_sentiment(score["compound"])
    }

def classify_sentiment(compound : float) -> str :
    """
        Convert compount sentiment scores to more human readable label based on pre-decided thresholds.
          - compount score >= 0.05 -> positive
          - compound score <= -0.05 -> negative
          - else -> neutral

        Args:
        compound: VADER compound score (-1.0 to 1.0)

        Returns:
        'positive', 'negative', or 'neutral'
    """
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"

def hash_url(url:str)-> str :
    """
    Generate hash of URL for article. This will help us with below:
        - De-duplication of articles if we search with multiple keywords.
        - avoiding PII data storage in URLs

    Args:
        url : article URL

    Returns:
        First 16 char of MD5 hash
    """
    return hashlib.md5(url.encode()).hexdigest()[:16]

def parse_articles(articles: list, coin_id: str, keyword: str, ingested_at : datetime) -> pd.DataFrame:
    """
        Parse each article from list received from News API.
        Does the sentiment analysis and recieves score for each article.
        Create dataframe for data to be processed later and save in parquet file.

        Args:
            articles : list of articles from News API for given keywords
            coin_id : coin for which keywords are used to search articles
            keyword : keywords used to search the articles
            ingested_at : timestamp at which ingestion is done.
        
        Returns:
            pd.Dataframe for the data retrieved and calculated.
    """
    records = []

    for article in articles:
        title = article.get("title") or ""
        description = article.get("description") or ""
        url = article.get("url") or ""

        if not title and not description:
            log.warning(f"skipping article with no title and description | url : {url}")
            continue
        
        text_to_score_sentiment = f"{title} {description}".strip()
        sentiment_score = score_sentiment(text_to_score_sentiment)

        records.append({
            "coin": coin_id,
            "keyword" : keyword,
            "source_name": article.get("source", {}).get("name"),
            "description" : article.get("description"),
            "author": article.get("author"),
            "title": article.get("title"),
            "url_hash" : hash_url(article.get("url")),
            "published_at": article.get("publishedAt"),
            "ingested_at" : ingested_at,
            **sentiment_score
            }
        )
    
    return pd.DataFrame(records)

def run(coins: list = COINS, lookup_days : int = INCREMENTAL_DAYS) -> str:
    """
    Main entry point for news ingestion.

    For each coin, fetches articles for all configured keywords,
    scores sentiment, deduplicates by url_hash, and writes
    to parquet partitioned by date.

    Designed to be called by Airflow task or local terminal.

    Args:
        coins:       list of coin slugs to ingest news for
        target_date: date used for partitioning and API filtering
                     defaults to yesterday — news from the past day

    """
    target_date = date.today() - timedelta(days=lookup_days)
    ingested_at = datetime.now(timezone.utc)

    log.info(f"Starting news ingestion | date : {target_date} | coin:{coins}")

    all_frames = []

    for coin in coins:
        coin_keywords = NEWS_KEYWORDS.get(coin, [coin])
        keywords_articles_df = []

        for keyword in coin_keywords:
            try:
                articles = fetch_news_articles(keyword=keyword, from_date=target_date)
                df = parse_articles(articles=articles, coin_id=coin, keyword=keyword, ingested_at=ingested_at)
                keywords_articles_df.append(df)
                log.info(f"Parsed | coin={coin} | keyword={keyword} | rows={len(df)}")
            except Exception as exception:
                log.error(f"Error while fetching article | keyword : {keyword} | coin : {coin} | error : {exception}")
            
        if not keywords_articles_df:
            log.warning(f"No articles found for coin {coin} with keywords {coin_keywords}")
            continue

        coin_df = pd.concat(keywords_articles_df, ignore_index=True)
        before = len(df)

        coin_df = coin_df.drop_duplicates(subset=["url_hash"])
        after = len(coin_df)

        if before != after:
            log.info(f"Deduped | coin={coin} | removed={before - after} duplicates")

        all_frames.append(coin_df)
    
    ## At this point, for each coin, we will have 1 coin_df appended to all_frame df.
    if not all_frames:
        log.error("No articles ingested — check API key and connectivity")
        return
    
    combined = pd.concat(all_frames, ignore_index=True)

    if STORAGE_TARGET=="GCS":
        from utils.gcs_writer import upload_to_gcs
        output_path = upload_to_gcs(
            df=combined,
            base_path=NEWS_PATH,
            partition_date=target_date,
            file_name=f"news_{target_date.isoformat()}"
        )
    else:
        output_path = write_parquet(
            df=combined,
            base_path=NEWS_PATH,
            partition_date=target_date,
            file_name=f"news_{target_date.isoformat()}"
        )

    log.info(f"News ingestion complete | total_rows={len(combined)} | output={output_path}")
    return output_path



