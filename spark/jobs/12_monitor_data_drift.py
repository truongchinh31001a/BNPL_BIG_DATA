"""Compare historical training features with valid streaming traffic using PSI."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import (
    add_bnpl_features,
    add_drift_buckets,
    apply_quality_gate,
    calculate_psi,
    create_spark,
    delta_upsert,
    get_settings,
    lake_path,
    parse_bronze_events,
)
from bnpl_common.postgres import upsert_quality_metrics


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-streaming-data-drift")

    baseline = add_drift_buckets(
        spark.read.format("delta").load(lake_path("gold/ml/ml_bnpl_features"))
    )
    streaming_bronze = (
        spark.read.option("basePath", lake_path(settings.streaming_bronze_path))
        .parquet(lake_path(settings.streaming_bronze_path))
    )
    validated = apply_quality_gate(
        parse_bronze_events(streaming_bronze),
        settings.pipeline_run_id,
        settings.batch_id,
    )
    current = add_drift_buckets(
        add_bnpl_features(validated.filter(F.col("_validation_status") == "PASS"))
    )

    drift = (
        calculate_psi(baseline, current)
        .withColumn("evaluation_id", F.lit(settings.pipeline_run_id))
        .withColumn("evaluated_at", F.current_timestamp())
        .select(
            "evaluation_id",
            "feature",
            "bucket",
            "baseline_count",
            "current_count",
            "baseline_share",
            "current_share",
            "psi_component",
            "psi",
            "drift_level",
            "evaluated_at",
        )
    ).cache()
    delta_upsert(
        spark,
        drift,
        lake_path("gold/monitoring/data_drift"),
        ["evaluation_id", "feature", "bucket"],
    )

    metric_rows = [
        (
            settings.pipeline_run_id,
            settings.batch_id,
            "streaming_vs_historical",
            "monitoring",
            f"psi_{row.feature}",
            float(row.psi),
        )
        for row in drift.select("feature", "psi").distinct().collect()
    ]
    upsert_quality_metrics(metric_rows)
    drift.orderBy(F.col("psi").desc(), "feature", "bucket").show(200, truncate=False)
    drift.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
