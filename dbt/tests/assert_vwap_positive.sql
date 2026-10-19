-- path: dbt/tests/assert_vwap_positive.sql
-- Test : VWAP doit être strictement positif

select
    symbol,
    computed_hour,
    vwap,
    'vwap_non_positive' as violation_type
from {{ ref('hourly_indicators') }}
where vwap is not null and vwap <= 0