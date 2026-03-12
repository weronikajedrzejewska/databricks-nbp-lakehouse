import argparse
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

import requests
from pydantic import BaseModel, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


def build_url(base_url: str, table: Literal["A", "B", "C"], start_date: date, end_date: date) -> str:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBP exchange rate tables into bronze.")
    parser.add_argument("--base-url", default="https://api.nbp.pl/api", help="NBP API base URL")
    parser.add_argument("--table", default="A", choices=["A", "B", "C"], help="NBP table type")
    parser.add_argument("--start-date", required=True, type=parse_iso_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, type=parse_iso_date, help="End date YYYY-MM-DD")
    parser.add_argument("--output-jsonl", default="data/bronze/nbp_raw.jsonl", help="Local JSONL output path")
    parser.add_argument("--output-table", default=None, help="Unity Catalog table, e.g. fx_lakehouse.nbp.bronze_nbp_raw")
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

        if args.output_table:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            df = spark.createDataFrame([r.model_dump() for r in records])
            df.write.format("delta").mode("append").saveAsTable(args.output_table)
            logger.info("Ingestion completed. rows=%s table=%s", len(records), args.output_table)
        else:
            output_path = Path(args.output_jsonl)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("w", encoding="utf-8") as f:
                for row in records:
                    f.write(row.model_dump_json() + "\n")
            logger.info("Ingestion completed. rows=%s output=%s", len(records), output_path)

    except (requests.RequestException, ValidationError, ValueError, OSError):
        logger.exception("Ingestion failed for url=%s", url)
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
