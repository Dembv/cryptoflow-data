-- path: dbt//models/staging/stg_onchain.sql
-- Events on-chain Etherscan normalisés

{{
  config(
    materialized = 'view',
    description  = 'Transactions whale ETH et transferts ERC-20'
  )
}}

with source as (
    select * from {{ source('timescale', 'onchain_events') }}
),

normalized as (
    select
        tx_hash,
        event_type,
        upper(trim(symbol))                         as symbol,
        block_number,
        timestamp_ts,
        date_trunc('hour',  timestamp_ts)           as event_hour,
        date_trunc('day',   timestamp_ts)           as event_date,
        from_address,
        to_address,

        -- ETH transactions
        coalesce(value_eth, 0)                      as value_eth,

        -- Token transfers (WBTC, USDT, USDC)
        coalesce(value_token, 0)                    as value_token,

        -- Classification du mouvement
        case
            when event_type = 'eth_transaction' and value_eth   >= 1000 then 'mega_whale'
            when event_type = 'eth_transaction' and value_eth   >= 100  then 'whale'
            when event_type = 'token_transfer'  and value_token >= 1000000 then 'whale_transfer'
            else 'normal'
        end                                         as movement_class,

        gas_used,
        ingested_at

    from source
    where
        tx_hash      is not null
        and timestamp_ts is not null
        and (value_eth > 0 or value_token > 0)
)

select * from normalized