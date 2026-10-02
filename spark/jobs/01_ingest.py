"""Stage Hugging Face sources as Parquet, then build Bronze with distributed Spark reads."""

import os
import sys
from itertools import islice
from pathlib import Path
from tempfile import TemporaryDirectory

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset
from pyspark.sql import functions as F

from bnpl_common import create_spark, get_settings, lake_path, safe_identifier
from bnpl_common.postgres import upsert_batch_status


def _chunks(rows, size: int):
    iterator = iter(rows)
    while batch := list(islice(iterator, size)):
        yield batch


def _hadoop_filesystem(spark, path: str):
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    return jvm.org.apache.hadoop.fs.FileSystem.get(
        uri, spark.sparkContext._jsc.hadoopConfiguration()
    )


def _path_exists(spark, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return bool(_hadoop_filesystem(spark, path).exists(hadoop_path))


def _stage_dataset(spark, dataset_id: str, stage_path: str, settings) -> int:
    """Download once to Parquet on MinIO so executors can read files in parallel."""

    if settings.ingest_stage_reuse and _path_exists(spark, stage_path):
        count = spark.read.option("mergeSchema", "true").parquet(stage_path).count()
        print(f"source={dataset_id} reused_stage={stage_path} rows={count}")
        return count

    jvm = spark.sparkContext._jvm
    filesystem = _hadoop_filesystem(spark, stage_path)
    destination = jvm.org.apache.hadoop.fs.Path(stage_path)
    if filesystem.exists(destination):
        filesystem.delete(destination, True)
    filesystem.mkdirs(destination)

    stream = load_dataset(dataset_id, split=settings.dataset_split, streaming=True)
    rows = iter(stream)
    if settings.ingest_max_rows:
        rows = islice(rows, settings.ingest_max_rows)

    total_rows = 0
    with TemporaryDirectory(prefix="bnpl-hf-stage-") as temporary_directory:
        for batch_number, batch in enumerate(_chunks(rows, settings.ingest_batch_size), start=1):
            local_path = Path(temporary_directory) / f"part-{batch_number:05d}.parquet"
            pq.write_table(pa.Table.from_pylist(batch), local_path, compression="snappy")
            remote_path = jvm.org.apache.hadoop.fs.Path(
                f"{stage_path}/part-{batch_number:05d}.parquet"
            )
            filesystem.copyFromLocalFile(
                False,
                True,
                jvm.org.apache.hadoop.fs.Path(local_path.as_uri()),
                remote_path,
            )
            total_rows += len(batch)
            print(f"source={dataset_id} staged_rows={total_rows}")

    if total_rows == 0:
        raise RuntimeError(f"Hugging Face source returned no rows: {dataset_id}")
    return total_rows


def _bronze_frame(spark, stage_path: str, dataset_id: str, settings):
    source = spark.read.option("mergeSchema", "true").parquet(stage_path)
    source_columns = source.columns
    value = F.to_json(F.struct(*[F.col(name) for name in source_columns]))
    key_candidates = [
        F.col(name).cast("string")
        for name in ("transaction_id", "loan_id", "id")
        if name in source_columns
    ]
    record_key = F.coalesce(*(key_candidates + [F.sha2(value, 256)]))
    return source.select(
        F.concat(F.lit(f"{dataset_id}:"), record_key).alias("message_key"),
        value.alias("value"),
        F.lit(dataset_id).alias("_source_dataset"),
        F.lit(settings.dataset_split).alias("_source_split"),
        F.lit("huggingface").alias("_source"),
        F.lit("batch").alias("_ingestion_type"),
        F.lit(settings.batch_id).alias("_batch_id"),
        F.lit(settings.pipeline_run_id).alias("_pipeline_run_id"),
        F.current_timestamp().alias("_ingested_at"),
    )


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-historical-to-bronze")

    for dataset_id in settings.dataset_ids:
        source_slug = safe_identifier(dataset_id)
        stage_path = lake_path(
            f"staging/source_snapshots/source_slug={source_slug}/batch_key={settings.batch_id}"
        )
        target = lake_path(
            f"bronze/historical_transactions/source_slug={source_slug}/batch_key={settings.batch_id}"
        )
        total_rows = _stage_dataset(spark, dataset_id, stage_path, settings)
        bronze = _bronze_frame(spark, stage_path, dataset_id, settings)
        (
            bronze.repartition(settings.ingest_output_partitions)
            .write.mode("overwrite")
            .option("maxRecordsPerFile", settings.ingest_batch_size)
            .parquet(target)
        )
        print(f"source={dataset_id} batch={settings.batch_id} bronze_rows={total_rows}")

        if os.getenv("PIPELINE_REGISTRY_ENABLED", "true").lower() == "true":
            upsert_batch_status(
                settings.batch_id,
                dataset_id,
                settings.pipeline_run_id,
                "bronze_status",
                "SUCCESS",
                total_rows,
            )

    spark.stop()


if __name__ == "__main__":
    main()
