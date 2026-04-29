import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from src.features.build_gold_features import build_features


def test_build_features_returns_expected_columns(spark):
    silver_rows = [
        {"rate_date": "2024-01-01", "currency_code": "USD", "mid_rate": 4.0},
        {"rate_date": "2024-01-02", "currency_code": "USD", "mid_rate": 4.1},
        {"rate_date": "2024-01-03", "currency_code": "USD", "mid_rate": 4.2},
        {"rate_date": "2024-01-01", "currency_code": "EUR", "mid_rate": 4.4},
        {"rate_date": "2024-01-02", "currency_code": "EUR", "mid_rate": 4.5},
        {"rate_date": "2024-01-03", "currency_code": "EUR", "mid_rate": 4.6},
    ]

    silver_df = spark.createDataFrame(silver_rows).selectExpr(
        "to_date(rate_date) as rate_date",
        "currency_code",
        "mid_rate",
    )

    features_df = build_features(silver_df)

    assert features_df.columns == [
        "date",
        "currency",
        "return_1d",
        "return_7d",
        "volatility_30d",
        "liquidity_proxy_7d",
    ]
    assert features_df.count() == 6


def test_build_features_computes_one_day_return(spark):
    silver_rows = [
        {"rate_date": "2024-01-01", "currency_code": "USD", "mid_rate": 4.0},
        {"rate_date": "2024-01-02", "currency_code": "USD", "mid_rate": 5.0},
    ]

    silver_df = spark.createDataFrame(silver_rows).selectExpr(
        "to_date(rate_date) as rate_date",
        "currency_code",
        "mid_rate",
    )

    result = build_features(silver_df).orderBy("date").collect()

    assert result[0]["return_1d"] is None
    assert result[1]["return_1d"] == 0.25
