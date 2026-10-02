"""Verify that Spark can write to and read from the configured HDFS lake."""

from __future__ import annotations

from pyspark.sql import SparkSession

from bnpl_common import get_settings, lake_path


def main() -> None:
    settings = get_settings()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("bnpl-hdfs-smoke")
        .config("spark.hadoop.fs.defaultFS", settings.hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    path = lake_path("_smoke/spark-parquet")

    try:
        spark.range(3).write.mode("overwrite").parquet(path)
        row_count = spark.read.parquet(path).count()
        if row_count != 3:
            raise AssertionError(f"Expected 3 rows, found {row_count}")
        print(f"Spark HDFS smoke passed: {path}, rows={row_count}")
    finally:
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(lake_path("_smoke"))
        file_system = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        file_system.delete(hadoop_path, True)
        spark.stop()


if __name__ == "__main__":
    main()
