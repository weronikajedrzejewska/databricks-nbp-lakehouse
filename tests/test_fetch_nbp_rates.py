import logging
from datetime import date
from unittest.mock import MagicMock

import pytest
import requests

from src.ingestion.fetch_nbp_rates import (
    BronzeRecord,
    NbpRate,
    NbpTable,
    build_url,
    expected_business_days,
    fetch_rates,
    filter_already_ingested_jsonl,
    reconcile_date_coverage,
    to_bronze_records,
)


def test_build_url_includes_table_and_date_range():
    url = build_url("https://api.nbp.pl/api", "A", date(2024, 1, 1), date(2024, 1, 31))

    assert url == "https://api.nbp.pl/api/exchangerates/tables/A/2024-01-01/2024-01-31/?format=json"


def test_to_bronze_records_preserves_metadata_and_payload():
    table = NbpTable(
        table="A",
        effectiveDate=date(2024, 1, 2),
        rates=[NbpRate(currency="dollar", code="USD", mid=4.0)],
    )

    records = to_bronze_records([table], "https://api.nbp.pl/api/example")

    assert len(records) == 1
    assert isinstance(records[0], BronzeRecord)
    assert records[0].source_url == "https://api.nbp.pl/api/example"
    assert records[0].table_type == "A"
    assert records[0].effective_date == date(2024, 1, 2)
    assert '"code":"USD"' in records[0].raw_payload


# --- fetch_rates ---

_VALID_API_RESPONSE = [
    {
        "table": "A",
        "effectiveDate": "2024-01-02",
        "rates": [
            {"currency": "US Dollar", "code": "USD", "mid": 4.05},
            {"currency": "Euro", "code": "EUR", "mid": 4.30},
        ],
    }
]


def _mock_session(payload, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    response.raise_for_status = MagicMock()
    session = MagicMock()
    session.get.return_value = response
    return session


def test_fetch_rates_returns_parsed_tables():
    session = _mock_session(_VALID_API_RESPONSE)
    result = fetch_rates("https://api.nbp.pl/api/example", session)

    assert len(result) == 1
    assert result[0].table == "A"
    assert result[0].effectiveDate == date(2024, 1, 2)
    assert len(result[0].rates) == 2
    assert result[0].rates[0].code == "USD"


def test_fetch_rates_raises_on_http_error():
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError("404")
    session = MagicMock()
    session.get.return_value = response

    with pytest.raises(requests.HTTPError):
        fetch_rates("https://api.nbp.pl/api/example", session)


def test_fetch_rates_raises_on_non_list_response():
    session = _mock_session({"error": "unexpected"})
    with pytest.raises(ValueError, match="expected JSON list"):
        fetch_rates("https://api.nbp.pl/api/example", session)


def test_fetch_rates_raises_on_invalid_table_schema():
    from pydantic import ValidationError
    session = _mock_session([{"table": "X", "effectiveDate": "2024-01-02", "rates": []}])
    with pytest.raises(ValidationError):
        fetch_rates("https://api.nbp.pl/api/example", session)


# ---


def _make_record(table_type: str, effective_date: date) -> BronzeRecord:
    table = NbpTable(
        table=table_type,
        effectiveDate=effective_date,
        rates=[NbpRate(currency="dollar", code="USD", mid=4.0)],
    )
    return to_bronze_records([table], "https://api.nbp.pl/api/example")[0]


def test_filter_already_ingested_jsonl_no_existing_file(tmp_path):
    records = [_make_record("A", date(2024, 1, 2))]
    result = filter_already_ingested_jsonl(records, tmp_path / "nbp_raw.jsonl", logging.getLogger())
    assert result == records


def test_filter_already_ingested_jsonl_skips_duplicate(tmp_path):
    jsonl = tmp_path / "nbp_raw.jsonl"
    record = _make_record("A", date(2024, 1, 2))
    jsonl.write_text(record.model_dump_json() + "\n", encoding="utf-8")

    result = filter_already_ingested_jsonl([record], jsonl, logging.getLogger())
    assert result == []


def test_filter_already_ingested_jsonl_passes_new_date(tmp_path):
    jsonl = tmp_path / "nbp_raw.jsonl"
    existing = _make_record("A", date(2024, 1, 2))
    new = _make_record("A", date(2024, 1, 3))
    jsonl.write_text(existing.model_dump_json() + "\n", encoding="utf-8")

    result = filter_already_ingested_jsonl([existing, new], jsonl, logging.getLogger())
    assert len(result) == 1
    assert result[0].effective_date == date(2024, 1, 3)


def test_filter_already_ingested_jsonl_different_table_type_is_not_duplicate(tmp_path):
    jsonl = tmp_path / "nbp_raw.jsonl"
    record_a = _make_record("A", date(2024, 1, 2))
    record_b = _make_record("B", date(2024, 1, 2))
    jsonl.write_text(record_a.model_dump_json() + "\n", encoding="utf-8")

    result = filter_already_ingested_jsonl([record_a, record_b], jsonl, logging.getLogger())
    assert len(result) == 1
    assert result[0].table_type == "B"


def test_nbp_table_rejects_invalid_table_type():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        NbpTable(table="X", effectiveDate=date(2024, 1, 2), rates=[])


def test_nbp_rate_rejects_non_numeric_mid():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        NbpRate(currency="dollar", code="USD", mid="not-a-number")


# --- expected_business_days ---

def test_expected_business_days_excludes_weekends():
    # 2024-01-01 is Monday, 2024-01-07 is Sunday
    result = expected_business_days(date(2024, 1, 1), date(2024, 1, 7))
    assert result == {
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    }


def test_expected_business_days_single_weekend_day_returns_empty():
    # 2024-01-06 is Saturday
    assert expected_business_days(date(2024, 1, 6), date(2024, 1, 6)) == set()


def test_expected_business_days_single_weekday():
    assert expected_business_days(date(2024, 1, 2), date(2024, 1, 2)) == {date(2024, 1, 2)}


# --- reconcile_date_coverage ---

def test_reconcile_logs_warning_for_missing_business_day(caplog):
    # 2024-01-02 (Tue) and 2024-01-03 (Wed) are business days — only 01-02 received
    records = [_make_record("A", date(2024, 1, 2))]
    log = logging.getLogger("nbp_ingestion")
    with caplog.at_level(logging.WARNING, logger="nbp_ingestion"):
        reconcile_date_coverage(records, date(2024, 1, 2), date(2024, 1, 3), log)
    assert "2024-01-03" in caplog.text


def test_reconcile_no_warning_when_coverage_complete(caplog):
    records = [_make_record("A", date(2024, 1, 2)), _make_record("A", date(2024, 1, 3))]
    log = logging.getLogger("nbp_ingestion")
    with caplog.at_level(logging.WARNING, logger="nbp_ingestion"):
        reconcile_date_coverage(records, date(2024, 1, 2), date(2024, 1, 3), log)
    assert "Missing" not in caplog.text


def test_reconcile_logs_warning_for_date_outside_range(caplog):
    # request 2024-01-02 only, but API returns 2024-01-05 as well
    records = [_make_record("A", date(2024, 1, 2)), _make_record("A", date(2024, 1, 5))]
    log = logging.getLogger("nbp_ingestion")
    with caplog.at_level(logging.WARNING, logger="nbp_ingestion"):
        reconcile_date_coverage(records, date(2024, 1, 2), date(2024, 1, 2), log)
    assert "outside requested range" in caplog.text


def test_reconcile_missing_only_weekend_no_warning(caplog):
    # Friday to Monday — Sat/Sun missing is expected, no warning
    records = [_make_record("A", date(2024, 1, 5)), _make_record("A", date(2024, 1, 8))]
    log = logging.getLogger("nbp_ingestion")
    with caplog.at_level(logging.WARNING, logger="nbp_ingestion"):
        reconcile_date_coverage(records, date(2024, 1, 5), date(2024, 1, 8), log)
    assert "Missing" not in caplog.text
