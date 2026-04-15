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
    p.price_timestamp,
    p.close_usd,

    n.published_at,
    n.description

from prices p
left join news n
    on p.coin_id = n.coin_id
    and date(p.price_timestamp) = date(n.published_at)