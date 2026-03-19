select
    cast(as_of_date as date) as as_of_date,
    currency_a,
    currency_b,
    cast(corr_30d as double) as corr_30d,
    cast(obs_cnt as bigint) as obs_cnt
from fx_lakehouse.nbp.gold_fx_correlation_30d
