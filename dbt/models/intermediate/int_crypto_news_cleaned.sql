with base as (
    select * from {{ ref('stg_crypto_news') }}
),

deduped as (
    select *
    from (
        select *, row_number() over ( 
            partition by coin, url_hash
            order by ingested_at
        ) as rn
        from base
    ) where rn = 1
)

select * from deduped