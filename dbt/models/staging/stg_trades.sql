-- path: dbt/models/staging/stg_trades.sql
-- Vue staging : nettoyage et normalisation des trades bruts Binance

{{
  config(
    materialized = 'view',
    description  = 'Trades Binance nettoyés — filtres qualité appliqués'
  )
}}

with source as (
    select * from {{ source('timescale', 'raw_trades') }}
),

cleaned as (
    select
        trade_id,
        upper(trim(symbol))                     as symbol,
        price::double precision                 as price,
        quantity::double precision              as quantity,
        price * quantity                        as notional_usd,
        lower(trim(side))                       as side,
        trade_time                              as trade_time,
        date_trunc('minute', trade_time)        as trade_minute,
        date_trunc('hour',   trade_time)        as trade_hour,
        date_trunc('day',    trade_time)        as trade_date,
        ingested_at,
        source,

        -- Flags qualité
        case
            when price   <= 0                     then 'invalid_price'
            when quantity <= 0                    then 'invalid_qty'
            when quantity < {{ var('min_trade_volume') }} then 'dust_trade'
            when side not in ('buy', 'sell')      then 'unknown_side'
            else 'ok'
        end                                     as quality_flag

    from source
    where
        symbol  is not null
        and price    is not null
        and quantity is not null
        and trade_time >= now() - interval '{{ var("default_lookback_days") }} days'
),

valid as (
    select * from cleaned
    where quality_flag = 'ok'
)

select * from valid