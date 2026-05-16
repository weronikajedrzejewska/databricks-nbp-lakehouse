{{
    config(
        materialized='incremental',
        unique_key=['as_of_date', 'currency_a', 'currency_b'],
        incremental_strategy='merge'
    )
}}

with silver as (
    select
        rate_date,
        currency_code,
        mid_rate
    from {{ ref('stg_silver_nbp_rates') }}
),

with_returns as (
    select
        rate_date,
        currency_code,
        (mid_rate / lag(mid_rate, 1) over (
            partition by currency_code order by rate_date
        )) - 1 as return_1d
    from silver
),

-- keep last 30 observations per currency
ranked as (
    select
        rate_date,
        currency_code,
        return_1d,
        row_number() over (
            partition by currency_code order by rate_date desc
        ) as rn
    from with_returns
    where return_1d is not null
),

last30 as (
    select rate_date, currency_code, return_1d
    from ranked
    where rn <= 30
),

as_of as (
    select max(rate_date) as as_of_date
    from last30
),

-- self-join to get all currency pairs (a < b avoids duplicates)
pairs as (
    select
        a.rate_date,
        a.currency_code as currency_a,
        b.currency_code as currency_b,
        a.return_1d     as return_a,
        b.return_1d     as return_b
    from last30 a
    inner join last30 b
        on a.rate_date = b.rate_date
        and a.currency_code < b.currency_code
),

corr_agg as (
    select
        currency_a,
        currency_b,
        corr(return_a, return_b) as corr_30d,
        count(*)                 as obs_cnt
    from pairs
    group by currency_a, currency_b
)

select
    as_of.as_of_date,
    corr_agg.currency_a,
    corr_agg.currency_b,
    corr_agg.corr_30d,
    corr_agg.obs_cnt,
    corr_agg.obs_cnt >= 10 as is_statistically_reliable
from corr_agg
cross join as_of

{% if is_incremental() %}
    where as_of.as_of_date > (select max(as_of_date) from {{ this }})
{% endif %}
