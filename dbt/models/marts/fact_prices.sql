{{
  config(
    materialized='incremental',
    unique_key='price_id',
    partition_by={
      "field": "price_date",
      "data_type": "date"
    },
    cluster_by=['coin_id']
  )
}}

with source as (

    select * 
    from {{ ref('int_crypto__prices') }}

    {% if is_incremental() %}
        where ingested_at > (
            select max(ingested_at) from {{ this }}
        )
    {% endif %}

),

final as (

    select
        price_id,
        coin_id,
        value_timestamp,
        date(value_timestamp) as price_date,

        open_usd,
        high_usd,
        low_usd,
        close_usd,

        ingested_at

    from source

)

select * from final