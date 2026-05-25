"""
Cross-layer reconciliation tests: verify that row counts and business keys
are consistent across Bronze → Silver → Gold.

These tests encode the "tie-out" discipline: every number must reconcile.
"""
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from datetime import date

from src.features.build_gold_features import build_features
from src.ingestion.fetch_nbp_rates import NbpRate, NbpTable, to_bronze_records
from src.transform.bronze_to_silver_delta import transform_bronze_to_silver_df


def _build_bronze_df(spark, tables):
    records = [r.model_dump() for r in to_bronze_records(tables, "https://api.nbp.pl/api")]
    return spark.createDataFrame(records)


def test_silver_row_count_equals_bronze_exploded_minus_rejections(spark):
    """Bronze: 2 dates × 3 currencies = 6 exploded rows. 1 invalid code + 1 zero rate = 2 rejected.
    Silver must contain exactly 4 rows."""
    tables = [
        NbpTable(
            table="A",
            effectiveDate=date(2024, 1, 2),
            rates=[
                NbpRate(currency="US Dollar", code="USD", mid=4.05),
                NbpRate(currency="Euro", code="EUR", mid=4.30),
                NbpRate(currency="Bad", code="TOOLONG", mid=1.00),  # rejected: code != 3 chars
            ],
        ),
        NbpTable(
            table="A",
            effectiveDate=date(2024, 1, 3),
            rates=[
                NbpRate(currency="US Dollar", code="USD", mid=4.10),
                NbpRate(currency="Euro", code="EUR", mid=4.35),
                NbpRate(currency="Broken", code="CHF", mid=0.0),  # rejected: zero rate
            ],
        ),
    ]

    bronze_df = _build_bronze_df(spark, tables)
    silver_df, metrics = transform_bronze_to_silver_df(bronze_df)

    assert metrics.input_rows == 6
    assert metrics.rejected_invalid_code == 1
    assert metrics.rejected_zero_mid == 1
    assert metrics.total_rejected == 2
    assert metrics.output_rows == 4
    assert silver_df.count() == 4

    # reconciliation identity: input = output + rejected + deduplicated
    assert metrics.input_rows == metrics.output_rows + metrics.total_rejected + metrics.deduplicated


def test_gold_currency_set_matches_silver_currency_set(spark):
    """Every currency in Silver must appear in Gold features for corresponding dates."""
    tables = [
        NbpTable(
            table="A",
            effectiveDate=date(2024, 1, d),
            rates=[
                NbpRate(currency="US Dollar", code="USD", mid=4.0 + d * 0.01),
                NbpRate(currency="Euro", code="EUR", mid=4.3 + d * 0.01),
                NbpRate(currency="Swiss Franc", code="CHF", mid=4.6 + d * 0.01),
            ],
        )
        for d in range(2, 10)
    ]

    bronze_df = _build_bronze_df(spark, tables)
    silver_df, _ = transform_bronze_to_silver_df(bronze_df)
    gold_df = build_features(silver_df)

    silver_currencies = {
        r.currency_code for r in silver_df.select("currency_code").distinct().collect()
    }
    gold_currencies = {r.currency for r in gold_df.select("currency").distinct().collect()}

    assert gold_currencies == silver_currencies


def test_gold_row_count_matches_silver_row_count(spark):
    """Gold features must have exactly one row per (date, currency) — same cardinality as Silver."""
    tables = [
        NbpTable(
            table="A",
            effectiveDate=date(2024, 1, d),
            rates=[
                NbpRate(currency="US Dollar", code="USD", mid=4.0 + d * 0.01),
                NbpRate(currency="Euro", code="EUR", mid=4.3 + d * 0.01),
            ],
        )
        for d in range(2, 6)
    ]

    bronze_df = _build_bronze_df(spark, tables)
    silver_df, _ = transform_bronze_to_silver_df(bronze_df)
    gold_df = build_features(silver_df)

    assert gold_df.count() == silver_df.count()


def test_gold_has_no_duplicate_date_currency_keys(spark):
    """Gold must never have duplicate (date, currency) keys regardless of input volume."""
    tables = [
        NbpTable(
            table="A",
            effectiveDate=date(2024, 1, d),
            rates=[
                NbpRate(currency="US Dollar", code="USD", mid=4.0 + d * 0.01),
                NbpRate(currency="Euro", code="EUR", mid=4.3 + d * 0.01),
            ],
        )
        for d in range(2, 12)
    ]

    bronze_df = _build_bronze_df(spark, tables)
    silver_df, _ = transform_bronze_to_silver_df(bronze_df)
    gold_df = build_features(silver_df)

    total = gold_df.count()
    distinct_keys = gold_df.select("date", "currency").distinct().count()
    assert total == distinct_keys, (
        f"Duplicate (date, currency) keys found: total={total} distinct={distinct_keys}"
    )
