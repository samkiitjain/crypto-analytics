{{
  config(
    materialized='incremental',
    unique_key='news_id'
  )
}}

with source as (

    select * 
    from {{ ref('int_crypto_news_cleaned') }}

    {% if is_incremental() %}
        where ingested_at > (
            select max(ingested_at) from {{ this }}
        )
    {% endif %}

)

select * from source