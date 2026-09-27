"""Compare a full Gold rebuild with a real transaction-key Delta MERGE."""

import os
import sys
import time
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from bnpl_common import add_bnpl_features, create_spark, delta_upsert, lake_path


RESULT_SCHEMA = StructType(
    [
        StructField("dataset_size", IntegerType(), False),
        StructField("baseline_records", LongType(), False),
        StructField("incremental_records", LongType(), False),
        StructField("processed_records", LongType(), False),
        StructField("worker_count", IntegerType(), False),
        StructField("run_number", IntegerType(), False),
        StructField("processing_mode", StringType(), False),
        StructField("runtime_seconds", DoubleType(), False),
        StructField("records_per_second", DoubleType(), False),
        StructField("final_row_count", LongType(), False),
        StructField("idempotency_verified", BooleanType(), False),
    ]
)


def split_counts(actual_count: int, incremental_fraction: float) -> tuple[int, int]:
    if actual_count < 2:
        raise ValueError("Silver source needs at least two transactions")
    if not 0 < incremental_fraction < 1:
        raise ValueError("BENCHMARK_INCREMENTAL_FRACTION must be between 0 and 1")
    incremental_count = max(1, min(actual_count - 1, round(actual_count * incremental_fraction)))
    return actual_count - incremental_count, incremental_count


def _result(size, baseline_count, incremental_count, processed, workers, run_number, mode, runtime, final_count, verified):
    return (
        size, baseline_count, incremental_count, processed, workers, run_number,
        mode, runtime, processed / runtime if runtime else 0.0, final_count, verified,
    )


def main() -> None:
    size = int(os.getenv("BENCHMARK_SIZE", "100000"))
    workers = int(os.getenv("BENCHMARK_WORKER_COUNT", "2"))
    run_number = int(os.getenv("BENCHMARK_RUN_NUMBER", "1"))
    fraction = float(os.getenv("BENCHMARK_INCREMENTAL_FRACTION", "0.25"))
    if size < 2 or workers < 1 or run_number < 1:
        raise ValueError("Benchmark size must be >=2; worker count and run number must be >=1")

    spark = create_spark(f"bnpl-incremental-{size}-{workers}-{run_number}")
    silver = spark.read.format("delta").load(lake_path("silver/transactions"))
    sample = silver.orderBy("transaction_id").limit(size).cache()
    actual_count = sample.count()
    baseline_count, incremental_count = split_counts(actual_count, fraction)
    if sample.select("transaction_id").distinct().count() != actual_count:
        raise ValueError("Benchmark sample must have unique transaction_id values")

    indexed = sample.withColumn(
        "_benchmark_row_number", F.row_number().over(Window.orderBy("transaction_id"))
    ).cache()
    indexed.count()  # Materialize deterministic split outside both timed sections.
    baseline = indexed.filter(F.col("_benchmark_row_number") <= baseline_count).drop("_benchmark_row_number")
    new_batch = indexed.filter(F.col("_benchmark_row_number") > baseline_count).drop("_benchmark_row_number")
    baseline_target = lake_path(f"benchmarks/incremental_targets/{size}/{workers}/{run_number}/merge")
    full_target = lake_path(f"benchmarks/incremental_targets/{size}/{workers}/{run_number}/full")

    # Existing production state: its initialization is deliberately untimed.
    add_bnpl_features(baseline).write.format("delta").mode("overwrite").save(baseline_target)

    started = time.perf_counter()
    delta_upsert(spark, add_bnpl_features(new_batch), baseline_target, ["transaction_id"])
    merged_ids = spark.read.format("delta").load(baseline_target).select("transaction_id").cache()
    merged_count = merged_ids.count()  # Force the completed target read before stopping the timer.
    incremental_runtime = time.perf_counter() - started
    if merged_count != actual_count:
        raise AssertionError(f"MERGE row count {merged_count} != expected {actual_count}")
    expected_ids = sample.select("transaction_id")
    if expected_ids.exceptAll(merged_ids).limit(1).count() or merged_ids.exceptAll(expected_ids).limit(1).count():
        raise AssertionError("MERGE target transaction IDs do not match the source sample")

    # Retry is verification only; it is not part of the primary runtime.
    delta_upsert(spark, add_bnpl_features(new_batch), baseline_target, ["transaction_id"])
    retried_ids = spark.read.format("delta").load(baseline_target).select("transaction_id")
    idempotent = (
        retried_ids.count() == merged_count
        and merged_ids.exceptAll(retried_ids).limit(1).count() == 0
        and retried_ids.exceptAll(merged_ids).limit(1).count() == 0
    )
    merged_ids.unpersist()
    if not idempotent:
        raise AssertionError("Retry changed the final transaction IDs")

    started = time.perf_counter()
    add_bnpl_features(indexed.drop("_benchmark_row_number")).write.format("delta").mode("overwrite").save(full_target)
    full_count = spark.read.format("delta").load(full_target).count()
    full_runtime = time.perf_counter() - started
    if full_count != actual_count:
        raise AssertionError(f"Full reload row count {full_count} != expected {actual_count}")
    full_ids = spark.read.format("delta").load(full_target).select("transaction_id")
    if expected_ids.exceptAll(full_ids).limit(1).count() or full_ids.exceptAll(expected_ids).limit(1).count():
        raise AssertionError("Full reload transaction IDs do not match the source sample")

    rows = [
        _result(size, baseline_count, incremental_count, incremental_count, workers, run_number,
                "incremental", incremental_runtime, merged_count, True),
        _result(size, baseline_count, incremental_count, actual_count, workers, run_number,
                "full", full_runtime, full_count, False),
    ]
    result = spark.createDataFrame(rows, RESULT_SCHEMA).withColumn("created_at", F.current_timestamp())
    delta_upsert(
        spark, result, lake_path("benchmarks/incremental_results"),
        ["dataset_size", "worker_count", "run_number", "processing_mode"],
    )
    result.show(truncate=False)
    indexed.unpersist()
    sample.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
