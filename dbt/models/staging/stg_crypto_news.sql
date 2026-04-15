{{
    config(
    materialized = 'view',
    tags = ['bronze', 'staging', 'news']
    )
}}

with sources as (
    select * from {{ source('crypto_analytics', 'news') }}
),

with staged as (
    select
      --- Key ---------------------
      {{dbt_utils.generate_surrogate_key(['coin', 'url_hash'])}} as news_id,

      ---- dimension---------------
      cast(coin as string) as coin,
      cast(published_at as timestamp) as published_at,
      cast(keyword as string) as keyword,

      ----- metadata ---------------
      cast(source_name as string) as source_name,
      cast(description as string) as description,
      cast(author as string) as author,
      cast(title as string) as title,
      cast(url_hash as string) as url_hash,
      cast(ingested_at as timestamp) as ingested_at,

      ----- metrics -----------------
      cast(sentiment_compound as float) as sentiment_compound,
      cast(sentiment_positive as float) as sentiment_positive,
      cast(sentiment_negative as float) as sentiment_negative,
      cast(sentiment_neutral as float) as sentiment_neutral,
      cast(sentiment_label as float) as sentiment_label
    
    from sources
    where coin is not null and published_at is not null
)
