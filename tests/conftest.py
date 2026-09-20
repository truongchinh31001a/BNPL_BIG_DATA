import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


SPARK_ROOT = str(Path(__file__).resolve().parents[1] / "spark")
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("bnpl-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
