import argparse
import logging
from typing import Tuple

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

SILVER_PATH_DEFAULT = "data/delta/silver/nbp_rates"
GOLD_FEATURES_PATH_DEFAULT = "data/delta/gold/fx_features_ml"

RETURN_1D_LAG = 1
RETURN_7D_LAG = 7
VOLATILITY_WINDOW = 30
LIQUIDITY_WINDOW = 7
MIN_OBS_FOR_VOL = 10


def setup_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return logging.getLogger("gold_features")


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("build_gold_features")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold feature table from silver FX rates.")
    parser.add_argument("--silver-path", default=SILVER_PATH_DEFAULT)
    parser.add_argument("--gold-path", default=GOLD_FEATURES_PATH_DEFAULT)
    return parser.parse_args()


def build_windows() -> Tuple[Window, Window, Window]:
    w_order = Window.partitionBy("currency_code").orderBy("rate_date")
    w_30d = w_order.rowsBetween(-(VOLATILITY_WINDOW - 1), 0)
    w_7d = w_order.rowsBetween(-(LIQUIDITY_WINDOW - 1), 0)
    return w_order, w_30d, w_7d


def build_features(df: DataFrame) -> DataFrame:
    w_order, w_30d, w_7d = build_windows()

    lag_1 = F.lag("mid_rate", RETURN_1D_LAG).over(w_order)
    lag_7 = F.lag("mid_rate", RETURN_7D_LAG).over(w_order)

    base = (
        df.select("rate_date", "currency_code", "mid_rate")
        .withColumn("return_1d", (F.col("mid_rate") / lag_1) - 1)
        .withColumn("return_7d", (F.col("mid_rate") / lag_7) - 1)
        .withColumn("obs_cnt_30d", F.count("return_1d").over(w_30d))
        .withColumn("volatility_30d_raw", F.stddev_samp("return_1d").over(w_30d))
        .withColumn("liquidity_proxy_7d", F.avg(F.abs("return_1d")).over(w_7d))
    )

    return (
        base.withColumn(
            "volatility_30d",
            F.when(F.col("obs_cnt_30d") >= MIN_OBS_FOR_VOL, F.col("volatility_30d_raw")),
        )
        .select(
            F.col("rate_date").alias("date"),
            F.col("currency_code").alias("currency"),
            "return_1d",
            "return_7d",
            "volatility_30d",
            "liquidity_proxy_7d",
        )
    )


def read_silver(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_gold(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def main() -> None:
    logger = setup_logger()
    args = parse_args()
    spark = get_spark()

    try:
        logger.info("Reading silver from %s", args.silver_path)
        silver = read_silver(spark, args.silver_path)

        logger.info("Building gold features")
        features = build_features(silver)

        logger.info("Writing gold to %s", args.gold_path)
        write_gold(features, args.gold_path)

        logger.info("Gold features completed")
    except Exception:
        logger.exception("Gold features failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
