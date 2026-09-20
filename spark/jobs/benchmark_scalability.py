"""Reproducible read/filter/group/join/aggregate/write Spark benchmark."""

import os
import sys
import time
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, delta_upsert, lake_path


def main() -> None:
    size = int(os.getenv("BENCHMARK_SIZE", "100000"))
    workers = int(os.getenv("BENCHMARK_WORKER_COUNT", "2"))
    run_number = int(os.getenv("BENCHMARK_RUN_NUMBER", "1"))
    mode = os.getenv("BENCHMARK_MODE", "full").lower()
    if mode not in {"full", "incremental"}:
        raise ValueError("BENCHMARK_MODE must be full or incremental")

    spark = create_spark(f"bnpl-benchmark-{size}-{workers}-{run_number}-{mode}")
    started = time.perf_counter()
    source = spark.read.format("delta").load(lake_path("gold/ml/ml_bnpl_features"))
    sample = source.limit(size)
    if mode == "incremental":
        sample = sample.filter(F.pmod(F.xxhash64("transaction_id"), F.lit(4)) == 0)
    input_count = sample.count()

    provider_totals = sample.groupBy("provider").agg(
        F.sum("principal_ngn").alias("provider_principal")
    )
    workload = (
        sample.filter(F.col("principal_ngn") > 0)
        .groupBy("provider", "customer_state", "merchant_category")
        .agg(
            F.count("*").alias("transactions"),
            F.avg("credit_score").alias("average_credit_score"),
            F.sum("principal_ngn").alias("principal_ngn"),
        )
        .join(provider_totals, "provider")
    )
    workload.write.mode("overwrite").parquet(
        lake_path(f"benchmarks/output/{mode}/{size}/{workers}/{run_number}")
    )
    runtime = time.perf_counter() - started
    result = spark.createDataFrame(
        [(size, input_count, workers, run_number, mode, runtime, input_count / runtime if runtime else 0.0)],
        ["dataset_size", "processed_records", "worker_count", "run_number", "processing_mode", "runtime_seconds", "records_per_second"],
    ).withColumn("created_at", F.current_timestamp())
    delta_upsert(
        spark,
        result,
        lake_path("benchmarks/results"),
        ["dataset_size", "worker_count", "run_number", "processing_mode"],
    )
    result.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
