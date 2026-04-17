from google.cloud import bigquery
import streamlit as st


# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

#  client is cache as a resource, so new client is not created everytime
#
# Difference from @st.cache_data:
#   @st.cache_data  — caches the RETURN VALUE (data, dataframes, dicts)
#   @st.cache_resource — caches the OBJECT ITSELF (clients, connections, models)

@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """Return a bq client, authenticated via ADC"""

    bq_client = bigquery.Client()
    return bq_client

## -------------------------------------------------
# Query helpers #
## -------------------------------------------------

# cache the result for 1 hour, as the list of coins is not expected to change frequently.



@st.cache_data(ttl=3600)
def get_coins() -> list[str]:
    """This method fetches the list of coins being tracked from the dim_coins table in the data warehouse.
       This is used to populate the dropdown menu in the sidebar.
    """

    query = """
        SELECT coin_id
        FROM `crypto_analytics_dev_marts.dim_coins`
        ORDER BY coin_id"""
    
    bq_client = get_bq_client()

    # .query() submits the SQL to BigQuery and returns a QueryJob object.
    # .to_dataframe() waits for the job to finish and converts results to pandas.
    # ["coin_id"] selects that single column as a pandas Series.
    # .tolist() converts the Series to a plain Python list — what st.selectbox expects.
    result = bq_client.query(query).to_dataframe()
    coins = result['coin_id'].tolist()
    return coins


@st.cache_data(ttl=3600)
def get_coin_prices(coin_id : str, start_date : str, end_date : str):
    """
    Returns OHLC price data for a given coin and date range.

    Args:
        coin_id : coin-ID selected from the drop down. "bitcoin"
        start_date: ISO string e.g "2026-04-17"
        end_date : ISO string e.g "2026-05-20"
    
    Returns:
        pandas.DataFrame: OHLC price data for the specified coin and date range.
    """

    client = get_bq_client()

    query = f"""
        SELECT value_timestamp, open_usd, high_usd, low_usd, close_usd
        from `crypto_analytics_dev_marts.fact_prices`
        WHERE coin_id = '{coin_id}'
        AND DATE(value_timestamp) BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY value_timestamp"""
    
    result = client.query(query).to_dataframe()
    return result

@st.cache_data(ttl=3600)
def get_sentiments(coin_id : str, start_date : str, end_date : str):
    """
    Returns daily avg sentiment for a given coin and date range.

    Args:
        coin_id : coin-ID selected from the drop down. "bitcoin"
        start_date: ISO string e.g "2026-04-17"
        end_date : ISO string e.g "2026-05-20"
    
    Returns:
        pandas.DataFrame: sentiment for the specified coin and date range.
    """

    client = get_bq_client()

    query = f"""
        SELECT 
        published_at,
        avg(sentiment_compound) as avg_sentiment,
        case
            WHEN COUNTIF(sentiment_label = 'positive') > COUNTIF(sentiment_label = 'negative')
            THEN 'positive'
            WHEN COUNTIF(sentiment_label = 'negative') > COUNTIF(sentiment_label = 'positive')
            THEN 'negative'
            ELSE 'nuetral'
        END as sentiment_label
        from `crypto_analytics_dev_marts.fact_news`
        WHERE coin = '{coin_id}'
        AND DATE(published_at) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY published_at
        ORDER BY published_at"""
    
    result = client.query(query).to_dataframe()
    return result

@st.cache_data(ttl=3600)
def get_price_sentiment(coin_id: str, start_date: str, end_date: str):
    """
    Returns the pre-joined price + sentiment table for a given coin and date range.
    This is the primary table for the correlation view.

    Returns:
        pandas DataFrame with all columns from fact_price_sentiment
    """
    client = get_bq_client()

    query = f"""
        SELECT *
        FROM `crypto_analytics_dev_marts.fact_price_sentiment`
        WHERE coin_id = '{coin_id}'
          AND DATE(value_timestamp) BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY value_timestamp
    """

    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def get_recent_news(coin_id: str, limit: int = 20):
    """
    Returns the most recent news articles for a given coin.
    Used for the news feed table on the dashboard.

    Args:
        coin_id: e.g. "bitcoin"
        limit:   number of articles to return (default 20)

    Returns:
        pandas DataFrame with columns:
            published_date, title, source_name, sentiment_label, sentiment_compound
    """
    client = get_bq_client()

    query = f"""
        SELECT
            published_at,
            title,
            source_name,
            sentiment_label,
            sentiment_compound
        FROM `crypto_analytics_dev_marts.fact_news`
        WHERE coin = '{coin_id}'
        ORDER BY published_date DESC
        LIMIT {limit}
    """

    return client.query(query).to_dataframe()