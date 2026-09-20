"""Compute per-run Data Quality monitoring metrics after the quality gate."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, delta_upsert, get_settings, lake_path
from bnpl_common.postgres import upsert_quality_metrics


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-data-quality-metrics")
    df = spark.read.parquet(lake_path(f"staging/validated_transactions/{settings.pipeline_run_id}"))
    critical = ["transaction_id", "purchase_date", "principal_ngn", "credit_score"]
    null_cells = sum(F.when(F.col(column).isNull(), 1).otherwise(0) for column in critical)

    summary = df.agg(
        F.count("*").alias("row_count"),
        F.sum(null_cells).alias("null_count"),
        F.sum(F.when(F.array_contains("_validation_errors", "duplicate_transaction_id"), 1).otherwise(0)).alias("duplicate_count"),
        F.sum(F.when(F.array_contains("_validation_errors", "invalid_credit_score"), 1).otherwise(0)).alias("invalid_credit_score_count"),
        F.sum(F.when(F.array_contains("_validation_errors", "invalid_principal_range") | F.array_contains("_validation_errors", "invalid_principal_type"), 1).otherwise(0)).alias("invalid_principal_count"),
        F.sum(F.when(F.array_contains("_validation_errors", "invalid_purchase_date"), 1).otherwise(0)).alias("invalid_date_count"),
        F.sum(F.when(F.col("_validation_status") == "FAIL", 1).otherwise(0)).alias("rejected_record_count"),
    ).first()

    row_count = int(summary.row_count)
    values = {
        "row_count": row_count,
        "null_count": int(summary.null_count or 0),
        "null_rate": float(summary.null_count or 0) / (row_count * len(critical)) if row_count else 0.0,
        "duplicate_count": int(summary.duplicate_count or 0),
        "duplicate_rate": float(summary.duplicate_count or 0) / row_count if row_count else 0.0,
        "invalid_credit_score_count": int(summary.invalid_credit_score_count or 0),
        "invalid_principal_count": int(summary.invalid_principal_count or 0),
        "invalid_date_count": int(summary.invalid_date_count or 0),
        "rejected_record_count": int(summary.rejected_record_count or 0),
        "valid_record_rate": (row_count - int(summary.rejected_record_count or 0)) / row_count if row_count else 0.0,
    }
    rows = [
        (
            settings.pipeline_run_id,
            settings.batch_id,
            settings.source_dataset_id,
            "bronze_quality_gate",
            name,
            float(value),
        )
        for name, value in values.items()
    ]
    metrics = spark.createDataFrame(
        rows,
        ["pipeline_run_id", "batch_id", "source", "layer", "metric_name", "metric_value"],
    ).withColumn("created_at", F.current_timestamp())
    delta_upsert(
        spark,
        metrics,
        lake_path("gold/data_quality/metrics"),
        ["pipeline_run_id", "batch_id", "source", "layer", "metric_name"],
    )
    upsert_quality_metrics(rows)
    spark.stop()


if __name__ == "__main__":
    main()
