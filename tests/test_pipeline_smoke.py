from datetime import date

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from src.features.build_gold_features import build_features
from src.ingestion.fetch_nbp_rates import NbpRate, NbpTable, to_bronze_records
from src.transform.bronze_to_silver_delta import transform_bronze_to_silver_df


def test_local_pipeline_smoke(spark):
    tables = [
        NbpTable(
            table="A",
            effectiveDate=date(2024, 1, 1),
            rates=[
                NbpRate(currency="US Dollar", code="USD", mid=4.0),
                NbpRate(currency="Euro", code="EUR", mid=4.4),
            ],
        ),
        NbpTable(
            table="A",
            effectiveDate=date(2024, 1, 2),
            rates=[
                NbpRate(currency="US Dollar", code="USD", mid=4.1),
                NbpRate(currency="Euro", code="EUR", mid=4.5),
            ],
        ),
    ]

    bronze_records = [record.model_dump() for record in to_bronze_records(tables, "https://api.nbp.pl/api")]
    bronze_df = spark.createDataFrame(bronze_records)

    silver_df = transform_bronze_to_silver_df(bronze_df)
    gold_df = build_features(silver_df)

    assert silver_df.count() == 4
    assert gold_df.count() == 4
    assert set(gold_df.columns) == {
        "date",
        "currency",
        "return_1d",
        "return_7d",
        "volatility_30d",
        "liquidity_proxy_7d",
    }
