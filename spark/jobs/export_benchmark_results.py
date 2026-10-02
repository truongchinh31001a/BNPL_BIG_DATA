"""Aggregate both official benchmark suites into a compact CSV summary."""

import sys
from functools import reduce
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, lake_path


def path_exists(spark, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    file_system = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    return file_system.exists(hadoop_path)


def main() -> None:
    spark = create_spark("bnpl-benchmark-summary")
    sources = [
        ("scalability", lake_path("benchmarks/results")),
        ("incremental", lake_path("benchmarks/incremental_results")),
    ]
    frames = []
    for suite, path in sources:
        if path_exists(spark, path):
            frames.append(
                spark.read.format("delta")
                .load(path)
                .select(
                    F.lit(suite).alias("benchmark_suite"),
                    "dataset_size",
                    "processed_records",
                    "worker_count",
                    "run_number",
                    "processing_mode",
                    "runtime_seconds",
                    "records_per_second",
                )
            )
    if not frames:
        raise ValueError("No benchmark Delta results were found")
    results = reduce(lambda left, right: left.unionByName(right), frames)
    official = results.filter(F.col("run_number") > 0)
    summary = (
        official.groupBy(
            "benchmark_suite", "dataset_size", "processing_mode", "worker_count"
        )
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
        .orderBy(
            "benchmark_suite", "processing_mode", "worker_count", "dataset_size"
        )
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
