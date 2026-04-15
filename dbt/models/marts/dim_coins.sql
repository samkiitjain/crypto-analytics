{{
  config(
    materialized='table'
  )
}}

select distinct
    coin_id
from {{ ref('int_crypto_prices_cleaned') }}