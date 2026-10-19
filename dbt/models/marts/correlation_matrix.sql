-- path: dbt/models/marts/correlation_matrix.sql
-- Matrice de corrélation entre assets sur fenêtres glissantes

{{
  config(
    materialized = 'table',
    description  = 'Corrélations de prix BTC/ETH/SOL/BNB/XRP (7j et 30j)'
  )
}}

with daily as (
    select
        trade_date,
        symbol,
        close,
        -- Log-returns pour corrélation plus stable
        ln(close / nullif(
            lag(close) over (partition by symbol order by trade_date), 0
        ))                                          as log_return
    from {{ ref('daily_ohlcv') }}
    where trade_date >= current_date - interval '{{ var("default_lookback_days") }} days'
),

-- Pivot des log-returns par asset
pivoted as (
    select
        trade_date,
        max(case when symbol = 'BTC' then log_return end) as btc_ret,
        max(case when symbol = 'ETH' then log_return end) as eth_ret,
        max(case when symbol = 'SOL' then log_return end) as sol_ret,
        max(case when symbol = 'BNB' then log_return end) as bnb_ret,
        max(case when symbol = 'XRP' then log_return end) as xrp_ret
    from daily
    group by trade_date
),

correlations as (
    select
        current_date                                as computed_date,
        '30d'                                       as window_label,
        round(corr(btc_ret, eth_ret)::numeric, 6)  as btc_eth,
        round(corr(btc_ret, sol_ret)::numeric, 6)  as btc_sol,
        round(corr(btc_ret, bnb_ret)::numeric, 6)  as btc_bnb,
        round(corr(btc_ret, xrp_ret)::numeric, 6)  as btc_xrp,
        round(corr(eth_ret, sol_ret)::numeric, 6)  as eth_sol,
        round(corr(eth_ret, bnb_ret)::numeric, 6)  as eth_bnb,
        round(corr(eth_ret, xrp_ret)::numeric, 6)  as eth_xrp,
        round(corr(sol_ret, bnb_ret)::numeric, 6)  as sol_bnb,
        round(corr(sol_ret, xrp_ret)::numeric, 6)  as sol_xrp,
        round(corr(bnb_ret, xrp_ret)::numeric, 6)  as bnb_xrp,
        count(*)                                    as sample_days
    from pivoted
    where btc_ret is not null
      and eth_ret is not null

    union all

    select
        current_date,
        '7d',
        round(corr(btc_ret, eth_ret)::numeric, 6),
        round(corr(btc_ret, sol_ret)::numeric, 6),
        round(corr(btc_ret, bnb_ret)::numeric, 6),
        round(corr(btc_ret, xrp_ret)::numeric, 6),
        round(corr(eth_ret, sol_ret)::numeric, 6),
        round(corr(eth_ret, bnb_ret)::numeric, 6),
        round(corr(eth_ret, xrp_ret)::numeric, 6),
        round(corr(sol_ret, bnb_ret)::numeric, 6),
        round(corr(sol_ret, xrp_ret)::numeric, 6),
        round(corr(bnb_ret, xrp_ret)::numeric, 6),
        count(*)
    from pivoted
    where trade_date >= current_date - interval '7 days'
      and btc_ret is not null
      and eth_ret is not null
)

select * from correlations