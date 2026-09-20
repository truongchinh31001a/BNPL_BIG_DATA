"""Long-running Kafka -> Bronze Parquet Structured Streaming job."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import KAFKA_EVENT_SCHEMA, create_spark, get_settings, lake_path


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-kafka-to-bronze-stream")
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    value = F.col("value").cast("string")
    bronze = kafka.select(
        F.col("key").cast("string").alias("message_key"),
        value.alias("value"),
        F.from_json(value, KAFKA_EVENT_SCHEMA).alias("_parsed_event"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.lit("kafka").alias("_source"),
        F.lit("stream").alias("_ingestion_type"),
        F.lit(None).cast("string").alias("_source_dataset"),
        F.lit(None).cast("string").alias("_source_split"),
        F.current_timestamp().alias("_ingested_at"),
    ).withColumn("_schema_parse_ok", F.col("_parsed_event").isNotNull())

    query = (
        bronze.writeStream.format("parquet")
        .option("path", lake_path("bronze/streaming_transactions"))
        .option("checkpointLocation", lake_path("_checkpoints/kafka_to_bronze"))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
