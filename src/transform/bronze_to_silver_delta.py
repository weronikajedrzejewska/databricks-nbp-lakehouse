import argparse
import logging
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


def setup_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return logging.getLogger("bronze_to_silver")


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("bronze_to_silver_nbp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform NBP bronze into silver Delta.")
    parser.add_argument("--bronze-path", default="data/bronze/nbp_raw.jsonl")
    parser.add_argument("--silver-path", default="data/delta/silver/nbp_rates")
    parser.add_argument(
        "--bronze-table",
        default=None,
        help="UC table, e.g. fx_lakehouse.nbp.bronze_nbp_raw",
    )
    parser.add_argument(
        "--silver-table",
        default=None,
        help="UC table, e.g. fx_lakehouse.nbp.silver_nbp_rates",
    )
    return parser.parse_args()


BRONZE_SCHEMA = StructType(
    [
        StructField("ingestion_ts", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("table_type", StringType(), True),
        StructField("effective_date", StringType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

RATE_SCHEMA = StructType(
    [
        StructField("currency", StringType(), True),
        StructField("code", StringType(), True),
        StructField("mid", DoubleType(), True),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("table", StringType(), True),
        StructField("effectiveDate", StringType(), True),
        StructField("rates", ArrayType(RATE_SCHEMA), True),
    ]
)


def transform_bronze_to_silver_df(bronze_df):
    parsed = (
        bronze_df.withColumn("payload", F.from_json(F.col("raw_payload"), PAYLOAD_SCHEMA))
        .withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))
        .select("table_type", "effective_date", "ingestion_ts", "payload")
    )

    exploded = (
        parsed.withColumn("rate", F.explode(F.col("payload.rates")))
        .select(
            F.col("table_type"),
            F.to_date(
                F.coalesce(F.col("effective_date"), F.col("payload.effectiveDate"))
            ).alias("rate_date"),
            F.col("rate.code").alias("currency_code"),
            F.col("rate.currency").alias("currency_name"),
            F.col("rate.mid").alias("mid_rate"),
            F.col("ingestion_ts"),
        )
        .filter(F.col("rate_date").isNotNull())
        .filter(F.length(F.col("currency_code")) == 3)
        .filter(F.col("mid_rate").isNotNull())
        .filter(F.col("mid_rate") > 0)
    )

    w = Window.partitionBy("table_type", "rate_date", "currency_code").orderBy(
        F.col("ingestion_ts").desc()
    )
    return exploded.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


def main() -> None:
    logger = setup_logger()
    args = parse_args()

    spark = get_spark()

    try:
        if args.bronze_table:
            logger.info("Reading bronze table %s", args.bronze_table)
            bronze = spark.read.table(args.bronze_table)
        else:
            logger.info("Reading bronze path %s", args.bronze_path)
            bronze = spark.read.schema(BRONZE_SCHEMA).json(args.bronze_path)

        silver_batch = transform_bronze_to_silver_df(bronze)

        if args.silver_table:
            if not spark.catalog.tableExists(args.silver_table):
                logger.info("Creating silver table %s", args.silver_table)
                (
                    silver_batch.write.format("delta")
                    .mode("overwrite")
                    .partitionBy("rate_date")
                    .saveAsTable(args.silver_table)
                )
            else:
                logger.info("Merging into silver table %s", args.silver_table)
                target = DeltaTable.forName(spark, args.silver_table)
                (
                    target.alias("t")
                    .merge(
                        silver_batch.alias("s"),
                        """
                        t.table_type = s.table_type
                        AND t.rate_date = s.rate_date
                        AND t.currency_code = s.currency_code
                        """,
                    )
                    .whenMatchedUpdate(
                        set={
                            "currency_name": "s.currency_name",
                            "mid_rate": "s.mid_rate",
                            "ingestion_ts": "s.ingestion_ts",
                        }
                    )
                    .whenNotMatchedInsert(
                        values={
                            "table_type": "s.table_type",
                            "rate_date": "s.rate_date",
                            "currency_code": "s.currency_code",
                            "currency_name": "s.currency_name",
                            "mid_rate": "s.mid_rate",
                            "ingestion_ts": "s.ingestion_ts",
                        }
                    )
                    .execute()
                )
        else:
            silver_path = Path(args.silver_path)
            delta_exists = (silver_path / "_delta_log").exists()

            if not delta_exists:
                logger.info("Creating silver Delta at %s", args.silver_path)
                (
                    silver_batch.write.format("delta")
                    .mode("overwrite")
                    .partitionBy("rate_date")
                    .save(args.silver_path)
                )
            else:
                logger.info("Merging into silver Delta at %s", args.silver_path)
                target = DeltaTable.forPath(spark, args.silver_path)
                (
                    target.alias("t")
                    .merge(
                        silver_batch.alias("s"),
                        """
                        t.table_type = s.table_type
                        AND t.rate_date = s.rate_date
                        AND t.currency_code = s.currency_code
                        """,
                    )
                    .whenMatchedUpdate(
                        set={
                            "currency_name": "s.currency_name",
                            "mid_rate": "s.mid_rate",
                            "ingestion_ts": "s.ingestion_ts",
                        }
                    )
                    .whenNotMatchedInsert(
                        values={
                            "table_type": "s.table_type",
                            "rate_date": "s.rate_date",
                            "currency_code": "s.currency_code",
                            "currency_name": "s.currency_name",
                            "mid_rate": "s.mid_rate",
                            "ingestion_ts": "s.ingestion_ts",
                        }
                    )
                    .execute()
                )

        logger.info("Silver transformation completed successfully")
    except Exception:
        logger.exception("Silver transformation failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
