"""Apply the reusable feature contract to incremental Silver records."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import add_bnpl_features, create_spark, delta_upsert, get_settings, lake_path


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-feature-engineering")
    silver = spark.read.format("delta").load(lake_path("silver/transactions"))
    current = silver.filter(F.col("_batch_id") == settings.batch_id)
    delta_upsert(
        spark,
        add_bnpl_features(current),
        lake_path("gold/shared/enriched_transactions"),
        ["transaction_id"],
    )
    spark.stop()


if __name__ == "__main__":
    main()
