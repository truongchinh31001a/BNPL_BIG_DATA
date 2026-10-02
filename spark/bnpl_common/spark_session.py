"""Spark session and HDFS lake path configuration."""

from __future__ import annotations

from pyspark.sql import SparkSession

from .config import get_settings


def create_spark(app_name: str) -> SparkSession:
    settings = get_settings()
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.defaultFS", settings.hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def lake_path(path: str) -> str:
    settings = get_settings()
    return f"{settings.hdfs_uri}{settings.hdfs_base_path}/{path.strip('/')}"
