import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from src.transform.bronze_to_silver_delta import transform_bronze_to_silver_df

_BRONZE_ROWS = [
    {
        "ingestion_ts": "2024-01-03T10:00:00+00:00",
        "source_url": "https://api.nbp.pl/api/a",
        "table_type": "A",
        "effective_date": "2024-01-02",
        "raw_payload": (
            '{"table":"A","effectiveDate":"2024-01-02","rates":['
            '{"currency":"US Dollar","code":"USD","mid":4.05},'
            '{"currency":"Euro","code":"EUR","mid":4.30},'
            '{"currency":"Bad","code":"TOOLONG","mid":1.00},'
            '{"currency":"Broken","code":"CHF","mid":0.0}]}'
        ),
    },
    {
        "ingestion_ts": "2024-01-03T11:00:00+00:00",
        "source_url": "https://api.nbp.pl/api/a",
        "table_type": "A",
        "effective_date": "2024-01-02",
        "raw_payload": (
            '{"table":"A","effectiveDate":"2024-01-02","rates":['
            '{"currency":"US Dollar","code":"USD","mid":4.10}]}'
        ),
    },
]


def test_transform_bronze_to_silver_deduplicates_and_filters_invalid_rows(spark):
    bronze_df = spark.createDataFrame(_BRONZE_ROWS)
    silver_df, _ = transform_bronze_to_silver_df(bronze_df)
    rows = {(row.currency_code, row.mid_rate) for row in silver_df.collect()}

    assert rows == {("USD", 4.10), ("EUR", 4.30)}


def test_transform_metrics_counts_rejected_rows(spark):
    bronze_df = spark.createDataFrame(_BRONZE_ROWS)
    _, metrics = transform_bronze_to_silver_df(bronze_df)

    # 5 exploded rows total: USD@10:00, EUR, TOOLONG, CHF(zero), USD@11:00
    assert metrics.input_rows == 5
    assert metrics.rejected_invalid_code == 1   # TOOLONG
    assert metrics.rejected_zero_mid == 1        # CHF mid=0.0
    assert metrics.rejected_null_date == 0
    assert metrics.rejected_null_mid == 0
    assert metrics.deduplicated == 1             # USD@10:00 superseded by USD@11:00
    assert metrics.output_rows == 2              # USD@11:00, EUR
    assert metrics.total_rejected == 2


def test_transform_metrics_zero_rejected_on_clean_data(spark):
    clean_rows = [
        {
            "ingestion_ts": "2024-01-03T10:00:00+00:00",
            "source_url": "https://api.nbp.pl/api/a",
            "table_type": "A",
            "effective_date": "2024-01-02",
            "raw_payload": (
                '{"table":"A","effectiveDate":"2024-01-02","rates":['
                '{"currency":"US Dollar","code":"USD","mid":4.05},'
                '{"currency":"Euro","code":"EUR","mid":4.30}]}'
            ),
        }
    ]
    bronze_df = spark.createDataFrame(clean_rows)
    _, metrics = transform_bronze_to_silver_df(bronze_df)

    assert metrics.total_rejected == 0
    assert metrics.deduplicated == 0
    assert metrics.output_rows == 2
