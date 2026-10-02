"""Near-real-time inference using the selected 30D and 90D Spark models."""

import sys
import time
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from psycopg2.extras import execute_values
from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from bnpl_common import BRONZE_STREAMING_SCHEMA, add_bnpl_features, apply_quality_gate, create_spark, lake_path, parse_bronze_events
from bnpl_common.postgres import postgres_connection


PREDICTION_UPSERT = """
    INSERT INTO ml.predictions
        (transaction_id, customer_id, model_name, model_version, prediction_horizon,
         predicted_default, default_probability, risk_level, event_timestamp,
         kafka_partition, kafka_offset, prediction_timestamp)
    VALUES %s
    ON CONFLICT (transaction_id, model_name, model_version, prediction_horizon)
    DO UPDATE SET
        predicted_default = EXCLUDED.predicted_default,
        default_probability = EXCLUDED.default_probability,
        risk_level = EXCLUDED.risk_level,
        event_timestamp = EXCLUDED.event_timestamp,
        kafka_partition = EXCLUDED.kafka_partition,
        kafka_offset = EXCLUDED.kafka_offset,
        prediction_timestamp = EXCLUDED.prediction_timestamp
"""


def _write_prediction_partition(rows) -> None:
    """Write one Spark partition at a time; task retries are safe through upsert."""

    settings = get_settings()
    pending = []
    with postgres_connection() as connection, connection.cursor() as cursor:
        for row in rows:
            pending.append(tuple(row))
            if len(pending) >= settings.postgres_write_batch_size:
                execute_values(cursor, PREDICTION_UPSERT, pending)
                pending.clear()
        if pending:
            execute_values(cursor, PREDICTION_UPSERT, pending)


def _stream_source(spark, settings):
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", settings.kafka_max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("value"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit("kafka").alias("_source"),
            F.lit("stream").alias("_ingestion_type"),
            F.lit(None).cast("string").alias("_source_dataset"),
            F.lit(None).cast("string").alias("_source_split"),
        )
    )
    parsed = F.from_json(F.col("value"), KAFKA_EVENT_SCHEMA)
    return (
        kafka.withColumn("_parsed_event", parsed)
        .withColumn(
            "_event_timestamp",
            F.coalesce(
                F.to_timestamp(F.col("_parsed_event.purchase_date")),
                F.col("_kafka_timestamp"),
            ),
        )
        .withColumn(
            "_dedup_key",
            F.coalesce(
                F.col("_parsed_event.transaction_id"),
                F.concat_ws(
                    ":",
                    F.col("_kafka_topic"),
                    F.col("_kafka_partition"),
                    F.col("_kafka_offset"),
                ),
            ),
        )
        .withWatermark("_event_timestamp", settings.streaming_watermark)
        .dropDuplicatesWithinWatermark(["_dedup_key"])
    )


def _enforce_horizon_consistency(scored):
    transaction_window = Window.partitionBy("transaction_id")
    probability_30d = F.max(
        F.when(F.col("prediction_horizon") == "30D", F.col("default_probability"))
    ).over(transaction_window)
    return scored.withColumn(
        "default_probability",
        F.when(
            (F.col("prediction_horizon") == "90D") & probability_30d.isNotNull(),
            F.greatest(F.col("default_probability"), probability_30d),
        ).otherwise(F.col("default_probability")),
    ).withColumn("predicted_default", F.col("default_probability") >= F.lit(0.5))


def _wait_for_bronze(spark, path: str, attempts: int = 30, interval_seconds: int = 2) -> None:
    """Wait a bounded time for the Bronze output path, then retry via Compose."""

    bronze_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = bronze_path.getFileSystem(spark._jsc.hadoopConfiguration())
    for attempt in range(attempts):
        if filesystem.exists(bronze_path):
            return
        if attempt < attempts - 1:
            time.sleep(interval_seconds)
    raise TimeoutError(f"Bronze streaming path did not appear: {path}")


def main() -> None:
    spark = create_spark("bnpl-streaming-prediction")
    registry = spark.read.format("delta").load(lake_path("gold/ml/model_registry")).collect()
    models = [(row, PipelineModel.load(row.model_path)) for row in registry]

    bronze_path = lake_path("bronze/streaming_transactions")
    _wait_for_bronze(spark, bronze_path)
    bronze = spark.readStream.schema(BRONZE_STREAMING_SCHEMA).parquet(bronze_path)

    def predict_batch(batch, epoch_id: int) -> None:
        if batch.rdd.isEmpty():
            return
        validated = apply_quality_gate(
            parse_bronze_events(batch), f"stream_{epoch_id}", f"stream_{epoch_id}"
        ).cache()
        rejected = (
            validated.filter(F.col("_validation_status") == "FAIL")
            .withColumn("event_date", F.to_date("_event_timestamp"))
            .withColumn("event_hour", F.date_format("_event_timestamp", "HH"))
        )
        (
            rejected.write.mode("append")
            .option("maxRecordsPerFile", settings.streaming_max_records_per_file)
            .partitionBy("event_date", "event_hour")
            .parquet(lake_path(settings.streaming_rejected_path))
        )
        valid = validated.filter(F.col("_validation_status") == "PASS")
        featured = (
            add_bnpl_features(valid)
            .withColumn("first_time_customer_num", F.col("first_time_customer").cast("double"))
            .cache()
        )

        scored_frames = []
        for metadata, model in models:
            scored_frames.append(
                model.transform(featured)
                .withColumn("default_probability", vector_to_array("probability")[1])
                .select(
                    "transaction_id",
                    "customer_id",
                    F.lit(metadata.model_name).alias("model_name"),
                    F.lit(metadata.model_version).alias("model_version"),
                    F.lit(metadata.prediction_horizon).alias("prediction_horizon"),
                    F.col("prediction").cast("boolean").alias("predicted_default"),
                    "default_probability",
                    F.lit("LOW").alias("risk_level"),
                    F.col("_event_timestamp").alias("event_timestamp"),
                    F.col("_kafka_partition").alias("kafka_partition"),
                    F.col("_kafka_offset").alias("kafka_offset"),
                    F.current_timestamp().alias("prediction_timestamp"),
                )
            )

        if scored_frames:
            scored = reduce(lambda left, right: left.unionByName(right), scored_frames)
            if settings.enforce_horizon_monotonicity:
                scored = _enforce_horizon_consistency(scored)
            scored = scored.withColumn(
                "risk_level",
                F.when(F.col("default_probability") >= 0.7, "HIGH")
                .when(F.col("default_probability") >= 0.4, "MEDIUM")
                .otherwise("LOW"),
            )
            scored.foreachPartition(_write_prediction_partition)
        featured.unpersist()
        validated.unpersist()

    query = (
        bronze.writeStream.foreachBatch(predict_batch)
        .option("checkpointLocation", lake_path("_checkpoints/bronze_to_prediction_v1"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
