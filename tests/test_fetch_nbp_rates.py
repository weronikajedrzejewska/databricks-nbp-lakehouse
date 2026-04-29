from datetime import date

from src.ingestion.fetch_nbp_rates import (
    BronzeRecord,
    NbpRate,
    NbpTable,
    build_url,
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
