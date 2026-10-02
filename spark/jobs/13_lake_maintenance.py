"""Offline Parquet compaction plus Delta compaction and safe retention."""

import os
import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from bnpl_common import create_spark, delta_upsert, get_settings, lake_path


DEFAULT_DELTA_PATHS = (
    "silver/transactions",
    "gold/shared/enriched_transactions",
    "gold/ml/ml_bnpl_features",
    "gold/monitoring/data_drift",
)


def _filesystem(spark, path: str):
    jvm = spark.sparkContext._jvm
    return jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(path), spark.sparkContext._jsc.hadoopConfiguration()
    )


def _exists(spark, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    return bool(_filesystem(spark, path).exists(jvm.org.apache.hadoop.fs.Path(path)))


def compact_partitioned_parquet(spark, path: str, run_id: str, max_records: int):
    """Rewrite an offline event-date/hour dataset and swap paths only after a successful write."""

    if not _exists(spark, path):
        return None
    metadata_path = f"{path.rstrip('/')}/_spark_metadata"
    if _exists(spark, metadata_path):
        raise RuntimeError(
            f"Refusing to compact legacy file-stream sink {path}: _spark_metadata is present. "
            "Migrate the writer to foreachBatch and remove the legacy metadata before retrying."
        )
    source = spark.read.option("basePath", path).parquet(path)
    before_files = source.select(F.input_file_name().alias("file")).distinct().count()
    frame = source.cache()
    row_count = frame.count()
    if row_count == 0:
        frame.unpersist()
        return (path, before_files, before_files, row_count, "EMPTY")

    temporary = f"{path}__compact_{run_id}"
    backup = f"{path}__backup_{run_id}"
    filesystem = _filesystem(spark, path)
    jvm = spark.sparkContext._jvm
    temporary_path = jvm.org.apache.hadoop.fs.Path(temporary)
    backup_path = jvm.org.apache.hadoop.fs.Path(backup)
    target_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem.delete(temporary_path, True)
    filesystem.delete(backup_path, True)

    (
        frame.repartition("event_date", "event_hour")
        .write.mode("overwrite")
        .option("maxRecordsPerFile", max_records)
        .partitionBy("event_date", "event_hour")
        .parquet(temporary)
    )
    frame.unpersist()
    after_files = (
        spark.read.option("basePath", temporary)
        .parquet(temporary)
        .select(F.input_file_name().alias("file"))
        .distinct()
        .count()
    )

    if not filesystem.rename(target_path, backup_path):
        filesystem.delete(temporary_path, True)
        raise RuntimeError(f"Could not move {path} to its compaction backup")
    if not filesystem.rename(temporary_path, target_path):
        filesystem.rename(backup_path, target_path)
        raise RuntimeError(f"Could not promote compacted data for {path}")
    filesystem.delete(backup_path, True)
    return (path, before_files, after_files, row_count, "COMPACTED")


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-lake-maintenance")
    maintenance_rows = []

    parquet_paths = [settings.streaming_bronze_path, settings.streaming_rejected_path]
    for relative_path in parquet_paths:
        result = compact_partitioned_parquet(
            spark,
            lake_path(relative_path),
            settings.pipeline_run_id,
            settings.streaming_max_records_per_file,
        )
        if result:
            maintenance_rows.append(result)

    retention_hours = int(os.getenv("DELTA_VACUUM_RETENTION_HOURS", "168"))
    if retention_hours < 168 and os.getenv("ALLOW_UNSAFE_VACUUM", "false").lower() != "true":
        raise ValueError("Retention below 168 hours requires ALLOW_UNSAFE_VACUUM=true")
    configured_paths = os.getenv("DELTA_MAINTENANCE_PATHS")
    delta_paths = (
        tuple(path.strip() for path in configured_paths.split(",") if path.strip())
        if configured_paths
        else DEFAULT_DELTA_PATHS
    )
    for relative_path in delta_paths:
        path = lake_path(relative_path)
        if DeltaTable.isDeltaTable(spark, path):
            table = DeltaTable.forPath(spark, path)
            table.optimize().executeCompaction()
            table.vacuum(retention_hours)

    if maintenance_rows:
        report = (
            spark.createDataFrame(
                maintenance_rows,
                ["path", "files_before", "files_after", "row_count", "status"],
            )
            .withColumn("maintenance_run_id", F.lit(settings.pipeline_run_id))
            .withColumn("completed_at", F.current_timestamp())
        )
        delta_upsert(
            spark,
            report,
            lake_path("gold/monitoring/file_maintenance"),
            ["maintenance_run_id", "path"],
        )
        report.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
