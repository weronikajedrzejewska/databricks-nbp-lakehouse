import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    SparkSession = pyspark.sql.SparkSession
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("databricks-nbp-lakehouse-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark session is not available in this environment: {exc}")
    yield session
    session.stop()
