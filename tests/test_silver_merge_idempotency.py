"""
Tests proving that Silver transformation is idempotent when run multiple times
on the same Bronze input. Re-running must not create duplicate rows.
"""
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
            '{"currency":"Euro","code":"EUR","mid":4.30}]}'
        ),
    },
    {
        "ingestion_ts": "2024-01-03T10:00:00+00:00",
        "source_url": "https://api.nbp.pl/api/a",
        "table_type": "A",
        "effective_date": "2024-01-03",
        "raw_payload": (
            '{"table":"A","effectiveDate":"2024-01-03","rates":['
            '{"currency":"US Dollar","code":"USD","mid":4.10},'
            '{"currency":"Euro","code":"EUR","mid":4.35}]}'
        ),
    },
]


def test_transform_is_idempotent_same_input_same_output(spark):
    """Running transform twice on identical Bronze input produces identical Silver output."""
    bronze_df = spark.createDataFrame(_BRONZE_ROWS)

    silver_first, metrics_first = transform_bronze_to_silver_df(bronze_df)
    silver_second, metrics_second = transform_bronze_to_silver_df(bronze_df)

    rows_first = sorted(
        [(r.currency_code, r.rate_date.isoformat(), r.mid_rate) for r in silver_first.collect()]
    )
    rows_second = sorted(
        [(r.currency_code, r.rate_date.isoformat(), r.mid_rate) for r in silver_second.collect()]
    )

    assert rows_first == rows_second
    assert metrics_first.output_rows == metrics_second.output_rows


def test_transform_rerun_with_overlapping_dates_no_duplicate_rows(spark):
    """Simulates a re-run: original data + same rows re-ingested with a newer ingestion_ts.
    The deduplication window must pick the latest ingestion_ts and keep exactly one row per key."""
    original_rows = _BRONZE_ROWS
    reingested_rows = [
        {
            **row,
            "ingestion_ts": "2024-01-04T08:00:00+00:00",  # newer timestamp
        }
        for row in _BRONZE_ROWS
    ]

    bronze_df = spark.createDataFrame(original_rows + reingested_rows)
    silver_df, metrics = transform_bronze_to_silver_df(bronze_df)

    result = silver_df.collect()
    keys = [(r.currency_code, r.rate_date.isoformat()) for r in result]

    assert len(keys) == len(set(keys)), f"Duplicate keys after merge re-run: {keys}"
    assert metrics.output_rows == 4  # USD+EUR for each of 2 dates


def test_transform_rerun_picks_latest_rate_on_duplicate_key(spark):
    """When the same (table_type, rate_date, currency_code) appears twice with different rates,
    the later ingestion_ts wins."""
    rows = [
        {
            "ingestion_ts": "2024-01-03T08:00:00+00:00",
            "source_url": "https://api.nbp.pl",
            "table_type": "A",
            "effective_date": "2024-01-02",
            "raw_payload": (
                '{"table":"A","effectiveDate":"2024-01-02",'
                '"rates":[{"currency":"US Dollar","code":"USD","mid":4.00}]}'
            ),
        },
        {
            "ingestion_ts": "2024-01-03T12:00:00+00:00",  # later — should win
            "source_url": "https://api.nbp.pl",
            "table_type": "A",
            "effective_date": "2024-01-02",
            "raw_payload": (
                '{"table":"A","effectiveDate":"2024-01-02",'
                '"rates":[{"currency":"US Dollar","code":"USD","mid":4.99}]}'
            ),
        },
    ]

    bronze_df = spark.createDataFrame(rows)
    silver_df, metrics = transform_bronze_to_silver_df(bronze_df)

    result = silver_df.collect()
    assert len(result) == 1
    assert result[0].mid_rate == pytest.approx(4.99)
    assert metrics.deduplicated == 1
