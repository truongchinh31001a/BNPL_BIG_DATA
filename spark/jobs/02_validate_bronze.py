"""Apply explicit schema parsing and the Data Quality Gate to Bronze."""

import os
import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import (
    apply_quality_gate,
    create_spark,
    get_settings,
    lake_path,
    parse_bronze_events,
    safe_identifier,
)


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-validate-bronze")
    source_slug = safe_identifier(settings.source_dataset_id)
    bronze_path = lake_path(
        "bronze/historical_transactions/"
        f"source_slug={source_slug}/batch_key={settings.batch_id}"
    )
    selected = spark.read.parquet(bronze_path)
    validated = apply_quality_gate(
        parse_bronze_events(selected), settings.pipeline_run_id, settings.batch_id
    ).cache()

    validated.write.mode("overwrite").parquet(
        lake_path(f"staging/validated_transactions/{settings.pipeline_run_id}")
    )
    (
        validated.filter(F.col("_validation_status") == "FAIL")
        .select(
            "value",
            "message_key",
            "_source_dataset",
            "_source_split",
            "_batch_id",
            "_pipeline_run_id",
            "_validation_status",
            "_validation_errors",
            "_unexpected_columns",
            "_rejected_at",
            "_ingested_at",
        )
        .write.mode("overwrite")
        .parquet(lake_path(f"rejected/transactions/{settings.pipeline_run_id}"))
    )
    print(
        f"run={settings.pipeline_run_id} total={validated.count()} "
        f"rejected={validated.filter(F.col('_validation_status') == 'FAIL').count()}"
    )
    validated.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
