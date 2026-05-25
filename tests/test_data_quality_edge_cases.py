"""
Edge case and data quality enforcement tests.
Covers: empty input, malformed JSON payload, quality flag columns, SLA breach.
"""
import logging
from datetime import date

import pytest

from src.ingestion.fetch_nbp_rates import (
    BronzeRecord,
    NbpRate,
    NbpTable,
    filter_already_ingested_jsonl,
    reconcile_date_coverage,
    to_bronze_records,
)

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from src.features.build_gold_correlation import MIN_OBS_FOR_CORR, build_correlation_snapshot
from src.features.build_gold_features import MIN_OBS_FOR_VOL, build_features
from src.transform.bronze_to_silver_delta import transform_bronze_to_silver_df

# ---------------------------------------------------------------------------
# SLA enforcement
# ---------------------------------------------------------------------------

def _make_record(table_type: str, effective_date: date) -> BronzeRecord:
    table = NbpTable(
        table=table_type,
        effectiveDate=effective_date,
        rates=[NbpRate(currency="dollar", code="USD", mid=4.0)],
    )
    return to_bronze_records([table], "https://api.nbp.pl/api")[0]


def test_reconcile_raises_when_missing_days_exceed_threshold():
    """reconcile_date_coverage raises ValueError when missing days > threshold."""
    records = [_make_record("A", date(2024, 1, 2))]
    # Request Mon–Wed (3 business days), receive only Mon — 2 missing, threshold is 1
    logger = logging.getLogger("nbp_ingestion")
    with pytest.raises(ValueError, match="SLA breach"):
        reconcile_date_coverage(records, date(2024, 1, 2), date(2024, 1, 4), logger)


def test_reconcile_does_not_raise_when_missing_within_threshold():
    """Exactly MAX_MISSING_BUSINESS_DAYS missing must log warning but not raise."""
    records = [_make_record("A", date(2024, 1, 2))]
    # Request Mon–Tue (2 business days), receive only Mon — 1 missing, at threshold
    logger = logging.getLogger("nbp_ingestion")
    reconcile_date_coverage(records, date(2024, 1, 2), date(2024, 1, 3), logger)  # no raise


# ---------------------------------------------------------------------------
# Malformed JSONL handling
# ---------------------------------------------------------------------------

def test_filter_jsonl_logs_warning_on_malformed_line(tmp_path, caplog):
    """Malformed lines in the JSONL file must be logged as WARNING, not silently dropped."""
    jsonl = tmp_path / "nbp_raw.jsonl"
    valid_record = _make_record("A", date(2024, 1, 2))
    jsonl.write_text(
        valid_record.model_dump_json() + "\n"
        + "NOT_VALID_JSON\n",
        encoding="utf-8",
    )
    logger = logging.getLogger("nbp_ingestion")
    with caplog.at_level(logging.WARNING, logger="nbp_ingestion"):
        filter_already_ingested_jsonl([_make_record("A", date(2024, 1, 3))], jsonl, logger)
    assert "malformed" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Bronze → Silver: empty and malformed input
# ---------------------------------------------------------------------------

def test_transform_raises_on_null_payload(spark):
    """If raw_payload cannot be parsed, transform must raise — not silently produce NULLs."""
    rows = [
        {
            "ingestion_ts": "2024-01-03T10:00:00+00:00",
            "source_url": "https://api.nbp.pl",
            "table_type": "A",
            "effective_date": "2024-01-02",
            "raw_payload": "THIS IS NOT JSON",
        }
    ]
    bronze_df = spark.createDataFrame(rows)
    with pytest.raises(ValueError, match="JSON parse failure"):
        transform_bronze_to_silver_df(bronze_df)


@pytest.mark.parametrize(
    "mid, rejection_field",
    [
        (0.0, "rejected_zero_mid"),
        (-1.5, "rejected_zero_mid"),
    ],
)
def test_transform_rejects_non_positive_rates(spark, mid, rejection_field):
    """Rates <= 0 must be rejected and counted in metrics."""
    rows = [
        {
            "ingestion_ts": "2024-01-03T10:00:00+00:00",
            "source_url": "https://api.nbp.pl",
            "table_type": "A",
            "effective_date": "2024-01-02",
            "raw_payload": (
                f'{{"table":"A","effectiveDate":"2024-01-02",'
                f'"rates":[{{"currency":"Test","code":"USD","mid":{mid}}}]}}'
            ),
        }
    ]
    bronze_df = spark.createDataFrame(rows)
    _, metrics = transform_bronze_to_silver_df(bronze_df)
    assert getattr(metrics, rejection_field) == 1
    assert metrics.output_rows == 0


@pytest.mark.parametrize(
    "code",
    ["TOOLONG", "X", "12", "ABCD"],
)
def test_transform_rejects_non_three_char_currency_codes(spark, code):
    """Currency codes that are not exactly 3 characters must be rejected."""
    rows = [
        {
            "ingestion_ts": "2024-01-03T10:00:00+00:00",
            "source_url": "https://api.nbp.pl",
            "table_type": "A",
            "effective_date": "2024-01-02",
            "raw_payload": (
                f'{{"table":"A","effectiveDate":"2024-01-02",'
                f'"rates":[{{"currency":"Test","code":"{code}","mid":4.0}}]}}'
            ),
        }
    ]
    bronze_df = spark.createDataFrame(rows)
    _, metrics = transform_bronze_to_silver_df(bronze_df)
    assert metrics.rejected_invalid_code == 1
    assert metrics.output_rows == 0


# ---------------------------------------------------------------------------
# Gold features: quality flag columns
# ---------------------------------------------------------------------------

def test_build_features_volatility_reliable_false_when_insufficient_obs(spark):
    """volatility_reliable must be False when obs_cnt < MIN_OBS_FOR_VOL."""
    rows = [
        {"rate_date": f"2024-01-0{d}", "currency_code": "USD", "mid_rate": 4.0 + d * 0.1}
        for d in range(1, MIN_OBS_FOR_VOL)  # one short of threshold
    ]
    silver_df = spark.createDataFrame(rows).selectExpr(
        "to_date(rate_date) as rate_date", "currency_code", "mid_rate"
    )
    result = build_features(silver_df).orderBy("date").collect()
    assert all(not r.volatility_reliable for r in result)


def test_build_features_volatility_reliable_true_when_sufficient_obs(spark):
    """volatility_reliable must be True for rows that have >= MIN_OBS_FOR_VOL observations."""
    rows = [
        {"rate_date": f"2024-01-{d:02d}", "currency_code": "USD", "mid_rate": 4.0 + d * 0.1}
        for d in range(1, MIN_OBS_FOR_VOL + 5)  # well above threshold
    ]
    silver_df = spark.createDataFrame(rows).selectExpr(
        "to_date(rate_date) as rate_date", "currency_code", "mid_rate"
    )
    result = build_features(silver_df).orderBy("date").collect()
    reliable_rows = [r for r in result if r.volatility_reliable]
    assert len(reliable_rows) > 0


def test_build_features_volatility_reliable_column_is_never_null(spark):
    """volatility_reliable must always be non-null — consumers rely on this flag."""
    rows = [
        {"rate_date": f"2024-01-{d:02d}", "currency_code": "USD", "mid_rate": 4.0 + d * 0.05}
        for d in range(1, 20)
    ]
    silver_df = spark.createDataFrame(rows).selectExpr(
        "to_date(rate_date) as rate_date", "currency_code", "mid_rate"
    )
    from pyspark.sql import functions as F
    result_df = build_features(silver_df)
    null_count = result_df.filter(F.col("volatility_reliable").isNull()).count()
    assert null_count == 0


# ---------------------------------------------------------------------------
# Gold correlation: quality flag columns
# ---------------------------------------------------------------------------

def test_build_correlation_is_statistically_reliable_false_when_low_obs(spark):
    """is_statistically_reliable must be False when obs_cnt < MIN_OBS_FOR_CORR."""
    rows = []
    for d in range(1, MIN_OBS_FOR_CORR):  # one short
        rows.extend([
            {"rate_date": f"2024-01-0{d}", "currency_code": "USD", "mid_rate": 4.0 + d * 0.1},
            {"rate_date": f"2024-01-0{d}", "currency_code": "EUR", "mid_rate": 4.5 + d * 0.1},
        ])
    silver_df = spark.createDataFrame(rows).selectExpr(
        "to_date(rate_date) as rate_date", "currency_code", "mid_rate"
    )
    result = build_correlation_snapshot(silver_df).collect()
    assert len(result) == 1
    assert result[0].is_statistically_reliable is False


def test_build_correlation_is_statistically_reliable_true_when_sufficient_obs(spark):
    """is_statistically_reliable must be True when obs_cnt >= MIN_OBS_FOR_CORR."""
    rows = []
    for d in range(1, MIN_OBS_FOR_CORR + 5):
        day = f"2024-01-{d:02d}"
        rows.extend([
            {"rate_date": day, "currency_code": "USD", "mid_rate": 4.0 + d * 0.1},
            {"rate_date": day, "currency_code": "EUR", "mid_rate": 4.5 + d * 0.1},
        ])
    silver_df = spark.createDataFrame(rows).selectExpr(
        "to_date(rate_date) as rate_date", "currency_code", "mid_rate"
    )
    result = build_correlation_snapshot(silver_df).collect()
    assert len(result) == 1
    assert result[0].is_statistically_reliable is True


def test_build_correlation_reliable_column_is_never_null(spark):
    """is_statistically_reliable must never be null."""
    rows = []
    for d in range(1, 35):
        day = f"2024-01-{d:02d}" if d <= 31 else f"2024-02-{d - 31:02d}"
        rows.extend([
            {"rate_date": day, "currency_code": "USD", "mid_rate": 4.0 + d * 0.1},
            {"rate_date": day, "currency_code": "EUR", "mid_rate": 4.5 + d * 0.1},
        ])
    silver_df = spark.createDataFrame(rows).selectExpr(
        "to_date(rate_date) as rate_date", "currency_code", "mid_rate"
    )
    from pyspark.sql import functions as F
    result_df = build_correlation_snapshot(silver_df)
    null_count = result_df.filter(F.col("is_statistically_reliable").isNull()).count()
    assert null_count == 0
