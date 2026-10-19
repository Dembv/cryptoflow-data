-- path: dbt/models/staging/stg_indicators.sql
-- Indicateurs Spark normalisés avec validation des bornes

{{
  config(
    materialized = 'view',
    description  = 'Indicateurs VWAP/RSI/Bollinger filtrés et typés'
  )
}}

with source as (
    -- Spark met à jour chaque fenêtre à chaque trigger : on garde la dernière version
    select distinct on (symbol, window_label, coalesce(window_start, computed_at)) *
    from {{ source('timescale', 'indicators') }}
    where computed_at >= now() - interval '{{ var("default_lookback_days") }} days'
    order by symbol, window_label, coalesce(window_start, computed_at), computed_at desc
),

normalized as (
    select
        computed_at,
        upper(trim(symbol))                         as symbol,
        window_label,

        -- VWAP : doit être positif
        nullif(greatest(vwap, 0), 0)                as vwap,

        -- RSI : borné entre 0 et 100
        case
            when rsi_approx < 0   then 0.0
            when rsi_approx > 100 then 100.0
            else round(rsi_approx::numeric, 4)
        end                                         as rsi,

        -- Zones RSI
        case
            when rsi_approx >= 70 then 'overbought'
            when rsi_approx <= 30 then 'oversold'
            else 'neutral'
        end                                         as rsi_zone,

        -- Bollinger Bands
        bb_upper,
        bb_middle,
        bb_lower,
        bb_bandwidth,

        -- Ratios buy/sell (entre 0 et 1)
        case
            when buy_ratio < 0 then 0.0
            when buy_ratio > 1 then 1.0
            else round(buy_ratio::numeric, 4)
        end                                         as buy_ratio,
        round((1 - coalesce(buy_ratio, 0.5))::numeric, 4) as sell_ratio,

        trade_count,
        volume

    from source
    where
        computed_at is not null
        and symbol is not null
        and window_label is not null
)

select * from normalized