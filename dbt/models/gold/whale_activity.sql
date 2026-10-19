-- path: dbt/models/gold/whale_activity.sql
-- Activité whale quotidienne par asset

{{
  config(
    materialized = 'table',
    description  = 'Résumé des mouvements whale — ETH + tokens ERC-20'
  )
}}

with onchain as (
    select * from {{ ref('stg_onchain') }}
),

daily_whale as (
    select
        event_date,
        symbol,
        event_type,
        movement_class,
        count(*)                                    as tx_count,
        sum(value_eth)                              as total_eth,
        sum(value_token)                            as total_token,
        avg(value_eth)                              as avg_eth_per_tx,
        max(value_eth)                              as max_eth_tx,
        count(distinct from_address)               as unique_senders,
        count(distinct to_address)                 as unique_receivers
    from onchain
    where movement_class in ('whale', 'mega_whale', 'whale_transfer')
    group by 1, 2, 3, 4
)

select
    event_date,
    symbol,
    event_type,
    movement_class,
    tx_count,
    round(total_eth::numeric, 4)                   as total_eth,
    round(total_token::numeric, 2)                 as total_token,
    round(avg_eth_per_tx::numeric, 4)              as avg_eth_per_tx,
    round(max_eth_tx::numeric, 4)                  as max_eth_tx,
    unique_senders,
    unique_receivers,
    current_timestamp                              as dbt_updated_at
from daily_whale
order by event_date desc, total_eth desc