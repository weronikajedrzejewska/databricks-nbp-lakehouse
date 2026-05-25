import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import requests
from pydantic import BaseModel, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

JSONL_DATE_FORMAT = "%Y-%m-%d"


class NbpRate(BaseModel):
    currency: str
    code: str
    mid: float


class NbpTable(BaseModel):
    table: Literal["A", "B", "C"]
    effectiveDate: date
    rates: list[NbpRate]


class BronzeRecord(BaseModel):
    ingestion_ts: str
    source_url: str
    table_type: Literal["A", "B", "C"]
    effective_date: date
    raw_payload: str


def setup_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return logging.getLogger("nbp_ingestion")


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD"
        ) from exc


def create_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def build_url(
    base_url: str,
    table: Literal["A", "B", "C"],
    start_date: date,
    end_date: date,
) -> str:
    return (
        f"{base_url}/exchangerates/tables/{table}/"
        f"{start_date.isoformat()}/{end_date.isoformat()}/?format=json"
    )


def fetch_rates(url: str, session: requests.Session) -> list[NbpTable]:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    raw = response.json()

    if not isinstance(raw, list):
        raise ValueError("Unexpected API response shape: expected JSON list")

    return [NbpTable.model_validate(item) for item in raw]


def to_bronze_records(tables: list[NbpTable], source_url: str) -> list[BronzeRecord]:
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    return [
        BronzeRecord(
            ingestion_ts=ingestion_ts,
            source_url=source_url,
            table_type=table_obj.table,
            effective_date=table_obj.effectiveDate,
            raw_payload=table_obj.model_dump_json(),
        )
        for table_obj in tables
    ]


def expected_business_days(start: date, end: date) -> set[date]:
    """Return the set of Mon–Fri dates in [start, end]."""
    days: set[date] = set()
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.add(current)
        current += timedelta(days=1)
    return days


MAX_MISSING_BUSINESS_DAYS = 1


def reconcile_date_coverage(
    records: list[BronzeRecord],
    start: date,
    end: date,
    logger: logging.Logger,
) -> None:
    received = {r.effective_date for r in records}
    expected = expected_business_days(start, end)
    missing = expected - received
    unexpected = received - expected  # dates outside the requested range

    logger.info(
        "Date coverage: requested=%s–%s expected_business_days=%d received=%d missing=%d",
        start.isoformat(),
        end.isoformat(),
        len(expected),
        len(received),
        len(missing),
    )
    if missing:
        missing_list = sorted(missing)
        logger.warning(
            "Missing %d business day(s) from API response: %s%s",
            len(missing_list),
            ", ".join(d.isoformat() for d in missing_list[:10]),
            " ..." if len(missing_list) > 10 else "",
        )
        if len(missing_list) > MAX_MISSING_BUSINESS_DAYS:
            raise ValueError(
                f"SLA breach: {len(missing_list)} business day(s) missing from API response "
                f"(threshold={MAX_MISSING_BUSINESS_DAYS}). Missing: "
                + ", ".join(d.isoformat() for d in missing_list[:10])
                + (" ..." if len(missing_list) > 10 else "")
            )
    if unexpected:
        logger.warning(
            "API returned %d date(s) outside requested range: %s",
            len(unexpected),
            ", ".join(d.isoformat() for d in sorted(unexpected)),
        )


def filter_already_ingested_jsonl(
    records: list[BronzeRecord],
    output_path: Path,
    logger: logging.Logger,
) -> list[BronzeRecord]:
    """Drop records whose (table_type, effective_date) already exist in the JSONL file."""
    if not output_path.exists():
        return records

    existing: set[tuple[str, str]] = set()
    with output_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = BronzeRecord.model_validate_json(line)
                existing.add((row.table_type, row.effective_date.strftime(JSONL_DATE_FORMAT)))
            except Exception:
                logger.warning("Skipping malformed JSONL line (parse error): %.120s", line)

    new_records = [
        r for r in records
        if (r.table_type, r.effective_date.strftime(JSONL_DATE_FORMAT)) not in existing
    ]
    skipped = len(records) - len(new_records)
    if skipped:
        logger.info("Idempotency check: skipped=%d already-ingested records", skipped)
    return new_records


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBP exchange rate tables into bronze.")
    parser.add_argument("--base-url", default="https://api.nbp.pl/api", help="NBP API base URL")
    parser.add_argument("--table", default="A", choices=["A", "B", "C"], help="NBP table type")
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_iso_date,
        help="Start date YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_iso_date,
        help="End date YYYY-MM-DD",
    )
    parser.add_argument(
        "--output-jsonl",
        default="data/bronze/nbp_raw.jsonl",
        help="Local JSONL output path",
    )
    parser.add_argument(
        "--output-table",
        default=None,
        help="Unity Catalog table, e.g. fx_lakehouse.nbp.bronze_nbp_raw",
    )
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("--start-date must be <= --end-date")

    logger = setup_logger()
    session = create_session()
    url = build_url(args.base_url, args.table, args.start_date, args.end_date)

    try:
        logger.info("Fetching NBP data from %s", url)
        tables = fetch_rates(url, session)
        records = to_bronze_records(tables, url)
        reconcile_date_coverage(records, args.start_date, args.end_date, logger)

        if args.output_table:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            all_df = spark.createDataFrame([r.model_dump() for r in records])

            if spark.catalog.tableExists(args.output_table):
                existing_keys = spark.read.table(args.output_table).select(
                    "table_type", "effective_date"
                ).distinct()
                new_df = all_df.join(
                    existing_keys,
                    on=["table_type", "effective_date"],
                    how="left_anti",
                )
                skipped = all_df.count() - new_df.count()
                if skipped:
                    logger.info("Idempotency check: skipped=%d already-ingested records", skipped)
            else:
                new_df = all_df

            row_count = new_df.count()
            if row_count:
                new_df.write.format("delta").mode("append").saveAsTable(args.output_table)
            logger.info(
                "Ingestion completed. written=%d skipped=%d table=%s",
                row_count,
                len(records) - row_count,
                args.output_table,
            )
        else:
            output_path = Path(args.output_jsonl)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            new_records = filter_already_ingested_jsonl(records, output_path, logger)
            with output_path.open("a", encoding="utf-8") as f:
                for row in new_records:
                    f.write(row.model_dump_json() + "\n")
            logger.info(
                "Ingestion completed. written=%d skipped=%d output=%s",
                len(new_records),
                len(records) - len(new_records),
                output_path,
            )

    except (requests.RequestException, ValidationError, ValueError, OSError):
        logger.exception("Ingestion failed for url=%s", url)
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
