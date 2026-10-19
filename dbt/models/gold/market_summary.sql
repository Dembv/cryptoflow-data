-- path: dbt/models/gold/market_summary.sql
-- Vue Gold consolidée — point d'entrée unique pour Streamlit/FastAPI

{{
  config(
    materialized = 'table',
    description  = 'Résumé marché Gold : derniers prix, indicateurs, signaux'
  )
}}

with latest_ohlcv as (
    select distinct on (symbol)
        symbol,
        trade_date,
        open, high, low, close,
        volume, volume_usd,
        vwap_daily,
        pct_change_1d,
        buy_ratio
    from {{ ref('daily_ohlcv') }}
    order by symbol, trade_date desc
),

latest_indicators as (
    select distinct on (symbol)
        symbol,
        computed_hour,
        vwap,
        rsi,
        rsi_zone,
        trading_signal,
        bb_upper, bb_middle, bb_lower, bb_bandwidth,
        buy_ratio as buy_ratio_1h
    from {{ ref('hourly_indicators') }}
    order by symbol, computed_hour desc
),

latest_correlations as (
    select
        btc_eth, btc_sol, btc_bnb, btc_xrp,
        eth_sol, eth_bnb, eth_xrp
    from {{ ref('correlation_matrix') }}
    where window_label = '7d'
    order by computed_date desc
    limit 1
),

combined as (
    select
        o.symbol,
        o.trade_date                                as last_trade_date,

        -- Prix OHLCV
        o.close                                     as price_usd,
        o.open, o.high, o.low,
        o.volume,
        round(o.volume_usd::numeric, 2)             as volume_usd,
        round(o.pct_change_1d::numeric, 4)          as pct_change_24h,
        round(o.vwap_daily::numeric, 8)             as vwap_24h,

        -- Indicateurs temps réel
        round(i.vwap::numeric, 8)                   as vwap_1h,
        round(i.rsi::numeric, 2)                    as rsi,
        i.rsi_zone,
        i.trading_signal,
        round(i.bb_upper::numeric, 4)               as bb_upper,
        round(i.bb_middle::numeric, 4)              as bb_middle,
        round(i.bb_lower::numeric, 4)               as bb_lower,
        round(i.bb_bandwidth::numeric, 6)           as bb_bandwidth,
        round(i.buy_ratio_1h::numeric, 4)           as buy_ratio,

        -- Corrélation BTC (pour les non-BTC assets)
        case o.symbol
            when 'ETH' then c.btc_eth
            when 'SOL' then c.btc_sol
            when 'BNB' then c.btc_bnb
            when 'XRP' then c.btc_xrp
            else null
        end                                         as corr_with_btc_7d,

        -- Score de confiance globale (0–100)
        round((
            (case when i.rsi between 30 and 70 then 30 else 0 end) +
            (case when i.bb_bandwidth > 0 then 20 else 0 end)      +
            (case when o.volume_usd > 1000 then 30 else 0 end)     +
            (case when o.trade_date = current_date then 20 else 0 end)
        )::numeric, 0)                              as data_quality_score,

        current_timestamp                           as dbt_updated_at

    from latest_ohlcv o
    left join latest_indicators i using (symbol)
    left join latest_correlations c on true
)

select * from combined
order by volume_usd desc nulls last