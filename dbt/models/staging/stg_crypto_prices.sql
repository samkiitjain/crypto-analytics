{{
    config(
    materialized = 'view',
    tags= ['staging', 'bronze', 'prices']
    )
}}


{#
    Staging model for raw OHLCV price data.
    Responsibilities at this layer:
      - Select and alias columns (no logic changes)
      - Cast types explicitly — BigQuery infers loosely from parquet
      - Add a surrogate key for downstream joins
      - Surface ingested_at for source freshness tracking
 
    Source: crypto_analytics_2026_{env}_raw.prices
    Target: crypto_analytics_2026_{env}_staging.stg_crypto_prices
#}

with source as (
    select * from {{ source('crypto_analytics', 'prices') }}
),

staged as (
    select 
        -- Key ---
        {{dbt_utils.generate_surrogate_key(['coin_id', 'timestamp_utc'])}} as price_id,

        -- Dimensions---
        cast(coin_id as string) as coin_id,
        cast(timestamp_utc as timestamp) as value_timestamp,

        ---- metrics
        cast(open_usd as float64) as open_usd,
        cast(high_usd as float64) as high_usd,
        cast(low_usd as float64) as low_usd,
        cast(close_usd as float64) as close_usd,

        -----metadata
        cast(ingested_at as timestamp) as ingested_at

    from source 
    where coin_id is not null and timestamp_utc is not null

)

select * from staged