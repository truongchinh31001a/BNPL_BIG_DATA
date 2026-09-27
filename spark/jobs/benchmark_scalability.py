"""Reproducible read/filter/group/join/aggregate/write Spark benchmark."""

import os
import sys
import time
from math import ceil
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

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
    read_started = time.perf_counter()
    source = (
        spark.read.format("delta")
        .load(lake_path("gold/ml/ml_bnpl_features"))
        .select(
            "transaction_id",
            "provider",
            "customer_state",
            "merchant_category",
            "principal_ngn",
            "credit_score",
        )
    ).persist(StorageLevel.DISK_ONLY)
    source_count = source.count()
    source_read_seconds = time.perf_counter() - read_started
    if source_count == 0:
        raise ValueError("Benchmark source is empty")

    preparation_started = time.perf_counter()
    replica_count = ceil(size / source_count)
    replicas = F.broadcast(spark.range(replica_count).withColumnRenamed("id", "_replica"))
    sample = (
        source.crossJoin(replicas)
        .withColumn(
            "transaction_id",
            F.concat_ws("_B", "transaction_id", F.col("_replica")),
        )
        .drop("_replica")
        .limit(size)
    )
    if mode == "incremental":
        sample = sample.filter(F.pmod(F.xxhash64("transaction_id"), F.lit(4)) == 0)
    sample = sample.persist(StorageLevel.DISK_ONLY)
    input_count = sample.count()
    preparation_seconds = time.perf_counter() - preparation_started

    transformation_started = time.perf_counter()
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
    ).persist(StorageLevel.DISK_ONLY)
    output_count = workload.count()
    transformation_seconds = time.perf_counter() - transformation_started

    write_started = time.perf_counter()
    workload.write.mode("overwrite").parquet(
        lake_path(f"benchmarks/output/{mode}/{size}/{workers}/{run_number}")
    )
    write_seconds = time.perf_counter() - write_started
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
    phase_rows = [
        ("source_read", source_read_seconds, source_count),
        ("sample_preparation", preparation_seconds, input_count),
        ("transform_shuffle", transformation_seconds, output_count),
        ("output_write", write_seconds, output_count),
        ("total", runtime, input_count),
    ]
    phase_results = spark.createDataFrame(
        [
            (size, workers, run_number, mode, phase, seconds, records)
            for phase, seconds, records in phase_rows
        ],
        [
            "dataset_size",
            "worker_count",
            "run_number",
            "processing_mode",
            "phase",
            "runtime_seconds",
            "record_count",
        ],
    ).withColumn("created_at", F.current_timestamp())
    delta_upsert(
        spark,
        phase_results,
        lake_path("benchmarks/phase_results"),
        ["dataset_size", "worker_count", "run_number", "processing_mode", "phase"],
    )
    result.show(truncate=False)
    phase_results.orderBy("phase").show(truncate=False)
    workload.unpersist()
    sample.unpersist()
    source.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
