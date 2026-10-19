-- path: dbt/tests/assert_high_gte_low.sql
-- Test : high >= low >= 0 sur toutes les bougies OHLCV

select
    symbol,
    trade_date,
    high,
    low,
    'high_lt_low' as violation_type
from {{ ref('daily_ohlcv') }}
where high < low

union all

select
    symbol,
    trade_date,
    high,
    low,
    'negative_low'
from {{ ref('daily_ohlcv') }}
where low < 0