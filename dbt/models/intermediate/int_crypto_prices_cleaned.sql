with base as (
    select * from {{ ref('stg_crypto_prices') }}
),

deduped as (
    select *
    from (
        select *, row_number() over ( 
            partition by coin_id, value_timestamp
            order by ingested_at
        ) as rn
        from base
    ) where rn = 1
)

select * from deduped