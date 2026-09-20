"""Small, schema-safe Delta Lake helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def is_delta_table(spark: SparkSession, path: str) -> bool:
    delta_log = spark._jvm.org.apache.hadoop.fs.Path(f"{path.rstrip('/')}/_delta_log")
    filesystem = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(filesystem.exists(delta_log))


def delta_upsert(spark: SparkSession, df: DataFrame, path: str, keys: list[str]) -> None:
    """Create a Delta table or idempotently merge by its business key."""

    if not keys:
        raise ValueError("At least one merge key is required")
    incoming = df.dropDuplicates(keys)
    if not is_delta_table(spark, path):
        incoming.write.format("delta").mode("overwrite").save(path)
        return

    incoming.createOrReplaceTempView("incoming_delta_rows")
    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` AS target
        USING incoming_delta_rows AS source
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
