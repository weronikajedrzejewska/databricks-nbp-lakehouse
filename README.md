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

## Repository Structure

```text
src/ingestion/fetch_nbp_rates.py
src/transform/bronze_to_silver_delta.py
src/features/build_gold_features.py
src/features/build_gold_correlation.py
dbt/models/staging/stg_silver_nbp_rates.sql
dbt/models/marts/mart_gold_fx_features.sql
dbt/models/marts/mart_gold_fx_correlation.sql
tests/
.github/workflows/ci.yml
```

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
- Automated local quality checks with `ruff` and `pytest`
- CI pipeline on GitHub Actions for linting and test execution

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

## Local Quality Checks

The repository includes a lightweight quality gate for portfolio and CI use:

- `ruff` for Python linting
- `pytest` for unit tests on ingestion and transformation logic
- smoke test covering a local `Bronze -> Silver -> Gold` path without external services

Run locally:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
make lint
make test
make smoke-test
```

What the smoke test proves:

- Bronze payloads can be created from NBP-style input
- Silver transformation parses, filters, and deduplicates rows correctly
- Gold feature generation runs end-to-end on a local Spark session

## Known Trade-offs

- `return_7d` is null for early observations when there is not enough history.
- `volatility_30d` is based on rolling observations, not strict calendar-day windows.
- Correlation is currently implemented as a latest snapshot, not a full daily historical correlation series.
- Bronze ingestion in Databricks uses a file loaded to a Unity Catalog Volume because direct API access was restricted in the workspace environment.

## How To Run

### 1. Install dependencies

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### 2. Generate raw NBP extract locally

```bash
python src/ingestion/fetch_nbp_rates.py \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --output-jsonl data/bronze/nbp_raw.jsonl
```

### 3. Run the local Delta path

```bash
python src/transform/bronze_to_silver_delta.py \
  --bronze-path data/bronze/nbp_raw.jsonl \
  --silver-path data/delta/silver/nbp_rates

python src/features/build_gold_features.py \
  --silver-path data/delta/silver/nbp_rates \
  --gold-path data/delta/gold/fx_features_ml

python src/features/build_gold_correlation.py \
  --silver-path data/delta/silver/nbp_rates \
  --gold-path data/delta/gold/fx_correlation_30d
```

### 4. Configure dbt for Databricks SQL Warehouse

Set the required environment variables:

```bash
export DATABRICKS_HOST=...
export DATABRICKS_HTTP_PATH=...
export DATABRICKS_TOKEN=...
export DATABRICKS_CATALOG=fx_lakehouse
export DATABRICKS_SCHEMA=nbp
```

Then copy the repo profile into your local dbt directory:

```bash
mkdir -p ~/.dbt
cp dbt/profiles.yml ~/.dbt/profiles.yml
```

Install dbt packages and run tests:

```bash
cd dbt
dbt deps
dbt test
```

### 5. Run the Databricks workflow

In the cloud variant, the pipeline sequence is:

1. Bronze ingest
2. Silver transform
3. Gold features build
4. Gold correlation build
5. dbt tests on downstream models

## Current Status

Implemented:

- Bronze/Silver/Gold pipeline
- Databricks Workflow
- Unity Catalog tables
- dbt models and data quality tests
- 3-month historical backfill
- local unit tests and smoke test
- CI workflow for linting and tests

Planned next improvements:

- stronger monitoring and alerting
- explicit schema drift checks
- incremental Gold optimization
- larger historical backfill

## Suggested Screenshots For Portfolio

To strengthen recruiter-facing proof, add screenshots of:

- Databricks Workflow run graph with all tasks green
- Unity Catalog tables in `fx_lakehouse.nbp`
- sample rows from `silver_nbp_rates`
- sample rows from `gold_fx_features`
- dbt test results
- one query or chart using Gold outputs

Recommended location in the repo:

```text
docs/images/
```

Example file names:

- `docs/images/databricks-workflow-run.png`
- `docs/images/unity-catalog-tables.png`
- `docs/images/silver-nbp-sample.png`
- `docs/images/gold-fx-features-sample.png`
- `docs/images/dbt-test-results.png`
- `docs/images/gold-query-or-chart.png`

Example Markdown snippet for `README.md`:

```md
## Screenshots

### Databricks Workflow
![Databricks Workflow run](docs/images/databricks-workflow-run.png)

### Gold Features Sample
![Gold features sample](docs/images/gold-fx-features-sample.png)
```
