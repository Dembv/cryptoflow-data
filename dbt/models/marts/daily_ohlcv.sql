-- path: dbt/models/marts/daily_ohlcv.sql
-- OHLCV journalier agrégé depuis les trades bruts (incrémental : les 2 derniers jours sont recalculés)

{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = ['symbol', 'trade_date'],
    description          = 'OHLCV journalier par asset — source primaire pour charts'
  )
}}

with trades as (
    select * from {{ ref('stg_trades') }}
    {% if is_incremental() %}
    where trade_time >= (
        select coalesce(max(trade_date), '1970-01-01'::timestamptz) - interval '1 day' from {{ this }}
    )
    {% endif %}
),

daily_agg as (
    select
        trade_date,
        symbol,

        -- OHLCV (open/close = premier/dernier trade de la journée)
        (array_agg(price order by trade_time asc,  trade_id asc))[1]  as open,
        max(price)                                                    as high,
        min(price)                                                    as low,
        (array_agg(price order by trade_time desc, trade_id desc))[1] as close,
        sum(quantity)                                                 as volume,
        sum(notional_usd)                                             as volume_usd,
        count(*)                                                      as trade_count,

        -- Métriques enrichies
        round((sum(notional_usd) / nullif(sum(quantity), 0))::numeric, 8) as vwap_daily,
        sum(case when side = 'buy'  then quantity else 0 end)         as buy_volume,
        sum(case when side = 'sell' then quantity else 0 end)         as sell_volume,
        round(
            (sum(case when side = 'buy' then quantity else 0 end)
            / nullif(sum(quantity), 0))::numeric, 4
        )                                                             as buy_ratio

    from trades
    group by trade_date, symbol
),

-- Clôtures nécessaires au calcul de la veille : tranche recalculée + jours déjà stockés juste avant
closes as (
    select trade_date, symbol, close from daily_agg
    {% if is_incremental() %}
    union all
    select trade_date, symbol, close from {{ this }}
    where trade_date <  (select min(trade_date) from daily_agg)
      and trade_date >= (select min(trade_date) from daily_agg) - interval '7 days'
    {% endif %}
),

lagged as (
    select
        trade_date,
        symbol,
        lag(close) over (partition by symbol order by trade_date) as prev_close
    from closes
)

select
    a.trade_date,
    a.symbol,
    a.open,
    a.high,
    a.low,
    a.close,
    a.volume,
    a.volume_usd,
    a.trade_count,
    a.vwap_daily,
    a.buy_volume,
    a.sell_volume,
    a.buy_ratio,

    -- Variation journalière
    round(((a.close - l.prev_close) / nullif(l.prev_close, 0) * 100)::numeric, 4) as pct_change_1d,

    -- True Range pour ATR (greatest ignore les NULL)
    greatest(
        a.high - a.low,
        abs(a.high - l.prev_close),
        abs(a.low  - l.prev_close)
    )                                                                             as true_range,

    current_timestamp                                                             as dbt_updated_at
from daily_agg a
join lagged l using (trade_date, symbol)
where a.high >= a.low  -- garde-fou qualité
