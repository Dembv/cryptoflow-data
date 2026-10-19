-- path: dbt/models/marts/hourly_indicators.sql
-- Consolidation des indicateurs par heure (pivot des 3 fenêtres Spark)

{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = ['symbol', 'computed_hour'],
    description  = 'Indicateurs horaires consolidés — VWAP, RSI, Bollinger par asset'
  )
}}

with indicators as (
    select * from {{ ref('stg_indicators') }}
),

-- On garde uniquement la fenêtre 1h pour la granularité horaire
hourly as (
    select
        date_trunc('hour', computed_at)             as computed_hour,
        symbol,
        window_label,
        avg(vwap)                                   as vwap,
        avg(rsi)                                    as rsi,
        max(rsi_zone)                               as rsi_zone,  -- mode approximé
        avg(bb_upper)                               as bb_upper,
        avg(bb_middle)                              as bb_middle,
        avg(bb_lower)                               as bb_lower,
        avg(bb_bandwidth)                           as bb_bandwidth,
        avg(buy_ratio)                              as buy_ratio,
        sum(trade_count)                            as trade_count,
        sum(volume)                                 as volume,
        count(*)                                    as sample_count

    from indicators
    where window_label = '1 hour'
    {% if is_incremental() %}
      and computed_at >= (select max(computed_hour) - interval '2 hours' from {{ this }})
    {% endif %}
    group by 1, 2, 3
),

enriched as (
    select
        computed_hour,
        symbol,
        round(vwap::numeric,         8)             as vwap,
        round(rsi::numeric,          4)             as rsi,
        rsi_zone,

        -- Bollinger signal
        case
            when rsi > 70 and bb_bandwidth > 0.05 then 'strong_sell_signal'
            when rsi < 30 and bb_bandwidth > 0.05 then 'strong_buy_signal'
            when bb_bandwidth < 0.02              then 'squeeze_watch'
            else 'neutral'
        end                                         as trading_signal,

        round(bb_upper::numeric,     4)             as bb_upper,
        round(bb_middle::numeric,    4)             as bb_middle,
        round(bb_lower::numeric,     4)             as bb_lower,
        round(bb_bandwidth::numeric, 6)             as bb_bandwidth,
        round(buy_ratio::numeric,    4)             as buy_ratio,
        trade_count,
        round(volume::numeric,       8)             as volume,
        sample_count,
        current_timestamp                           as dbt_updated_at

    from hourly
)

select * from enriched