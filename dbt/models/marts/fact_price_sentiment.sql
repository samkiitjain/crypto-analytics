{{
  config(
    materialized='table'
  )
}}

with prices as (

    select * from {{ ref('fact_prices') }}

),

news as (

    select * from {{ ref('fact_news') }}

)

select
    p.coin_id,
    p.value_timestamp,
    p.close_usd,

    n.published_at,
    n.description,

    -- sentiment columns added from fact_news
    n.sentiment_compound,
    n.sentiment_positive,
    n.sentiment_negative,
    n.sentiment_neutral,
    n.sentiment_label

from prices p
left join news n
    on p.coin_id = n.coin
    and date(p.value_timestamp) = date(n.published_at)