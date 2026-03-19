# NBP Exchange Rates Lakehouse (Databricks)

End-to-end data engineering project built on Databricks using the public NBP REST API. The pipeline follows Medallion architecture (`Bronze -> Silver -> Gold`), stores data in Delta tables under Unity Catalog, runs through Databricks Workflows, and includes dbt models with data quality tests.

## Business Goal

Build a production-style FX analytics pipeline that delivers reliable downstream datasets for BI and ML-style feature engineering.

Main feature table:

`date | currency | return_1d | return_7d | volatility_30d | liquidity_proxy_7d`

## Stack

- Databricks
- Unity Catalog
- Delta Lake
- PySpark
- dbt-databricks
- Databricks Workflows
- NBP REST API

## Architecture

`NBP API -> Bronze (raw payload) -> Silver (parsed, validated, deduplicated) -> Gold (features + correlation snapshot)`

## Implemented Layers

### Bronze

Raw NBP API payloads are stored with ingestion metadata:

- `ingestion_ts`
- `source_url`
- `table_type`
- `effective_date`
- `raw_payload`

This preserves source traceability and provides a safe landing zone for schema changes.

### Silver

The Silver layer parses and flattens FX rates into one row per currency and date with:

- typed fields
- null filtering
- business-key deduplication
- rerun-safe merge logic

Business key:

- `table_type + rate_date + currency_code`

### Gold

Two Gold outputs are implemented:

1. `gold_fx_features`
- `return_1d`
- `return_7d`
- `volatility_30d`
- `liquidity_proxy_7d`

2. `gold_fx_correlation_30d`
- `as_of_date`
- `currency_a`
- `currency_b`
- `corr_30d`
- `obs_cnt`

## What Makes It Production-Style

- Idempotent Silver processing via deterministic merge keys
- Raw payload preservation in Bronze
- Data quality filtering in Silver (`mid_rate > 0`, valid currency codes, non-null dates)
- Delta tables managed in Unity Catalog
- Orchestration with Databricks Workflows
- dbt models and tests on Silver and Gold layers

## dbt Coverage

dbt models were added for:

- `stg_silver_nbp_rates`
- `mart_gold_fx_features`
- `mart_gold_fx_correlation`

Implemented tests include:

- `not_null`
- `accepted_values`
- unique combination tests on business keys

## Databricks Objects

Catalog:

- `fx_lakehouse`

Schema:

- `nbp`

Main tables:

- `fx_lakehouse.nbp.bronze_nbp_raw`
- `fx_lakehouse.nbp.silver_nbp_rates`
- `fx_lakehouse.nbp.gold_fx_features`
- `fx_lakehouse.nbp.gold_fx_correlation_30d`

Volume:

- `fx_lakehouse.nbp.landing`

## Workflow

Databricks Workflow runs the pipeline in sequence:

1. Bronze ingest
2. Silver transform
3. Gold features build
4. Gold correlation build

## Data Range

The pipeline was validated on a 3-month historical backfill.

## Known Trade-offs

- `return_7d` is null for early observations when there is not enough history.
- `volatility_30d` is based on rolling observations, not strict calendar-day windows.
- Correlation is currently implemented as a latest snapshot, not a full daily historical correlation series.
- Bronze ingestion in Databricks uses a file loaded to a Unity Catalog Volume because direct API access was restricted in the workspace environment.

## How To Run

1. Generate raw NBP extract locally with `fetch_nbp_rates.py`.
2. Upload `nbp_raw.jsonl` to Unity Catalog Volume.
3. Run Bronze, Silver, Gold Features, and Gold Correlation notebooks in Databricks.
4. Execute dbt models and tests locally against Databricks SQL Warehouse.
5. Run the Databricks Workflow for orchestration.

## Current Status

Implemented:

- Bronze/Silver/Gold pipeline
- Databricks Workflow
- Unity Catalog tables
- dbt models and data quality tests
- 3-month historical backfill

Planned next improvements:

- stronger monitoring and alerting
- explicit schema drift checks
- incremental Gold optimization
- larger historical backfill
