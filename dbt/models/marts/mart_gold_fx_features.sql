select
    cast(date as date) as date,
    currency,
    cast(return_1d as double) as return_1d,
    cast(return_7d as double) as return_7d,
    cast(volatility_30d as double) as volatility_30d,
    cast(liquidity_proxy_7d as double) as liquidity_proxy_7d
from fx_lakehouse.nbp.gold_fx_features
