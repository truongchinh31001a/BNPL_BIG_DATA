"""Aggregate official benchmark runs and export a compact CSV summary."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, lake_path


def main() -> None:
    spark = create_spark("bnpl-benchmark-summary")
    results = spark.read.format("delta").load(lake_path("benchmarks/results"))
    official = results.filter(F.col("run_number") > 0)
    summary = (
        official.groupBy("dataset_size", "processing_mode", "worker_count")
        .agg(
            F.count("*").alias("runs"),
            F.expr("percentile_approx(runtime_seconds, 0.5, 10000)").alias(
                "median_runtime_seconds"
            ),
            F.expr("percentile_approx(records_per_second, 0.5, 10000)").alias(
                "median_records_per_second"
            ),
            F.expr("percentile_approx(processed_records, 0.5, 10000)").alias(
                "median_processed_records"
            ),
        )
        .orderBy("processing_mode", "worker_count", "dataset_size")
    )
    summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        lake_path("benchmarks/summary_csv")
    )
    print("BENCHMARK_SUMMARY_BEGIN")
    for row in summary.collect():
        print(row.asDict())
    print("BENCHMARK_SUMMARY_END")
    spark.stop()


if __name__ == "__main__":
    main()
