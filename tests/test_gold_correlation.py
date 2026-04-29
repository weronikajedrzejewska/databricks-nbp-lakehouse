import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from src.features.build_gold_correlation import build_correlation_snapshot, validate_input


def test_validate_input_raises_for_missing_columns(spark):
    df = spark.createDataFrame([{"rate_date": "2024-01-01"}])

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_input(df, {"rate_date", "currency_code", "mid_rate"})


def test_build_correlation_snapshot_returns_expected_pairs(spark):
    rows = []
    for idx in range(1, 35):
        day = f"2024-01-{idx:02d}" if idx <= 31 else f"2024-02-{idx - 31:02d}"
        rows.extend(
            [
                {"rate_date": day, "currency_code": "USD", "mid_rate": 4.0 + (idx * 0.1)},
                {"rate_date": day, "currency_code": "EUR", "mid_rate": 4.5 + (idx * 0.1)},
                {"rate_date": day, "currency_code": "CHF", "mid_rate": 4.2 + (idx * 0.05)},
            ]
        )

    silver_df = spark.createDataFrame(rows).selectExpr(
        "to_date(rate_date) as rate_date",
        "currency_code",
        "mid_rate",
    )

    result = build_correlation_snapshot(silver_df)
    pairs = {(row.currency_a, row.currency_b) for row in result.collect()}

    assert pairs == {("CHF", "EUR"), ("CHF", "USD"), ("EUR", "USD")}
