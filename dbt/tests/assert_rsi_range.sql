-- path: dbt/tests/assert_rsi_range.sql
-- Test : RSI strictement entre 0 et 100

select
    symbol,
    computed_hour,
    rsi,
    'rsi_out_of_range' as violation_type
from {{ ref('hourly_indicators') }}
where rsi < 0 or rsi > 100