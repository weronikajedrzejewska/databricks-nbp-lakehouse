{{
    config(
        materialized='incremental',
        unique_key=['date', 'currency'],
        incremental_strategy='merge'
    )
}}

with silver as (
    select
        rate_date,
        currency_code,
        mid_rate
    from {{ ref('stg_silver_nbp_rates') }}

    {% if is_incremental() %}
        where rate_date > (select max(date) from {{ this }})
    {% endif %}
),

with_returns as (
    select
        rate_date,
        currency_code,
        mid_rate,
        lag(mid_rate, 1) over (
            partition by currency_code order by rate_date
        ) as lag_1d,
        lag(mid_rate, 7) over (
            partition by currency_code order by rate_date
        ) as lag_7d
    from silver
),

with_features as (
    select
        rate_date,
        currency_code,
        mid_rate,
        (mid_rate / lag_1d) - 1                                         as return_1d,
        (mid_rate / lag_7d) - 1                                         as return_7d,
        count((mid_rate / lag_1d) - 1) over (
            partition by currency_code
            order by rate_date
            rows between 29 preceding and current row
        )                                                                as obs_cnt_30d,
        stddev_samp((mid_rate / lag_1d) - 1) over (
            partition by currency_code
            order by rate_date
            rows between 29 preceding and current row
        )                                                                as volatility_30d_raw,
        avg(abs((mid_rate / lag_1d) - 1)) over (
            partition by currency_code
            order by rate_date
            rows between 6 preceding and current row
        )                                                                as liquidity_proxy_7d
    from with_returns
)

select
    rate_date                                                            as date,
    currency_code                                                        as currency,
    return_1d,
    return_7d,
    case
        when obs_cnt_30d >= 10 then volatility_30d_raw
        else null
    end                                                                  as volatility_30d,
    obs_cnt_30d >= 10                                                    as volatility_reliable,
    liquidity_proxy_7d
from with_features
