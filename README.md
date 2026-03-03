# NBP Exchange Rates Lakehouse (Databricks)

End-to-end data engineering pipeline on Databricks using NBP REST API, built in Medallion architecture (`Bronze -> Silver -> Gold`) with incremental processing, idempotency, schema evolution handling, data quality tests, and ML-ready features.

## Business Goal
Deliver reliable FX analytics tables and a feature table for downstream ML/BI:
- `date | currency | return_1d | return_7d | volatility_30d | liquidity_proxy_7d`

## Stack
Databricks, Delta Lake, PySpark, dbt, Databricks Workflows, REST API, structured logging, Slack webhook alerts.

## Architecture
`NBP API -> Bronze (raw JSON) -> Silver (clean + dedup + standardize) -> Gold (marts + features)`

## What Makes It Production-Style
- **Idempotency:** deterministic `MERGE` keys, safe reruns for same date range.
- **Schema evolution:** raw payload preserved in Bronze; controlled schema checks in Silver.
- **Incremental loads:** watermark-based ingestion, optional backfill mode.
- **SCD2:** historical tracking for selected dimension (`valid_from`, `valid_to`, `is_current`).
- **Data quality:** `not_null`, `uniqueness`, `freshness`, `rate > 0`.
- **Monitoring:** structured JSON logs + Slack alerts on failure/DQ breach.
- **Cost thinking:** incremental only, date partitioning, compaction/optimize strategy.

## Gold Outputs
- `gold.currency_daily`
- `gold.currency_monthly_stats`
- `gold.currency_volatility_30d`
- `gold.fx_features_ml`

## How To Run (High-Level)
1. Configure `.env` (Databricks + API + alert settings).
2. Run historical load for selected range.
3. Trigger daily incremental workflow.
4. Run DQ tests.
5. Query Gold tables.

## Definition of Done
- Historical + incremental pipeline works.
- Rerun does not duplicate data.
- API schema change is detected/handled.
- DQ tests pass.
- Gold marts and ML feature table are available.
# databricks-nbp-lakehouse
