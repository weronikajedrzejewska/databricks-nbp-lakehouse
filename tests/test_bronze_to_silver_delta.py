import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from src.transform.bronze_to_silver_delta import transform_bronze_to_silver_df


def test_transform_bronze_to_silver_deduplicates_and_filters_invalid_rows(spark):
    bronze_rows = [
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

    bronze_df = spark.createDataFrame(bronze_rows)
    silver_df = transform_bronze_to_silver_df(bronze_df)
    rows = {(row.currency_code, row.mid_rate) for row in silver_df.collect()}

    assert rows == {("USD", 4.10), ("EUR", 4.30)}
