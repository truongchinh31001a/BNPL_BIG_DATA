"""Merge quality-gated records into the trusted Silver Delta table."""

import os
import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from bnpl_common import create_spark, delta_upsert, get_settings, lake_path
from bnpl_common.postgres import upsert_batch_status
from bnpl_common.validation import valid_silver_rows


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-bronze-to-silver-delta")
    validated = spark.read.parquet(
        lake_path(f"staging/validated_transactions/{settings.pipeline_run_id}")
    )
    valid = valid_silver_rows(validated)
    delta_upsert(spark, valid, lake_path("silver/transactions"), ["transaction_id"])

    if os.getenv("PIPELINE_REGISTRY_ENABLED", "true").lower() == "true":
        upsert_batch_status(
            settings.batch_id,
            settings.source_dataset_id,
            settings.pipeline_run_id,
            "silver_status",
            "SUCCESS",
            valid.count(),
        )
    spark.stop()


if __name__ == "__main__":
    main()
