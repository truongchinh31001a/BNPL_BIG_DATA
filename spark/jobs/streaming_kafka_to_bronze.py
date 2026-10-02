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
        .option("maxOffsetsPerTrigger", settings.kafka_max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )
    value = F.col("value").cast("string")
    parsed_event = F.from_json(value, KAFKA_EVENT_SCHEMA)
    bronze = kafka.select(
        F.col("key").cast("string").alias("message_key"),
        value.alias("value"),
        parsed_event.alias("_parsed_event"),
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
    bronze = (
        bronze.withColumn(
            "_event_timestamp",
            F.coalesce(F.to_timestamp(F.col("_parsed_event.purchase_date")), F.col("_kafka_timestamp")),
        )
        .withColumn("event_date", F.to_date("_event_timestamp"))
        .withColumn("event_hour", F.date_format("_event_timestamp", "HH"))
    )

    bronze_path = lake_path(settings.streaming_bronze_path)

    def write_bronze_batch(batch, _batch_id: int) -> None:
        (
            batch.repartition("event_date", "event_hour")
            .write.mode("append")
            .option("maxRecordsPerFile", settings.streaming_max_records_per_file)
            .partitionBy("event_date", "event_hour")
            .parquet(bronze_path)
        )

    query = (
        bronze.writeStream.foreachBatch(write_bronze_batch)
        .option(
            "checkpointLocation",
            lake_path(
                f"_checkpoints/kafka_to_bronze_{settings.streaming_checkpoint_version}"
            ),
        )
        .outputMode("append")
        .trigger(processingTime=f"{settings.streaming_trigger_seconds} seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
