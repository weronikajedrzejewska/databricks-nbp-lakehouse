select
    table_type,
    cast(rate_date as date) as rate_date,
    currency_code,
    currency_name,
    cast(mid_rate as double) as mid_rate,
    cast(ingestion_ts as timestamp) as ingestion_ts
from fx_lakehouse.nbp.silver_nbp_rates
