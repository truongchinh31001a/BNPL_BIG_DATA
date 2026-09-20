"""Publish the flat ML feature contract as a Gold Delta table."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, delta_upsert, get_settings, lake_path
from bnpl_common.schemas import ML_FEATURE_COLUMNS


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-build-ml-features")
    enriched = spark.read.format("delta").load(lake_path("gold/shared/enriched_transactions"))
    current = enriched.filter(F.col("_batch_id") == settings.batch_id)
    delta_upsert(
        spark,
        current.select(*ML_FEATURE_COLUMNS),
        lake_path("gold/ml/ml_bnpl_features"),
        ["transaction_id"],
    )
    spark.stop()


if __name__ == "__main__":
    main()
