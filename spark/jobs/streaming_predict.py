"""Near-real-time inference using the selected 30D and 90D Spark models."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from psycopg2.extras import execute_values
from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import functions as F

from bnpl_common import add_bnpl_features, apply_quality_gate, create_spark, get_settings, lake_path, parse_bronze_events
from bnpl_common.postgres import postgres_connection


def _write_predictions(rows: list[tuple]) -> None:
    if not rows:
        return
    statement = """
        INSERT INTO ml.predictions
            (transaction_id, customer_id, model_name, model_version, prediction_horizon,
             predicted_default, default_probability, risk_level, prediction_timestamp)
        VALUES %s
        ON CONFLICT (transaction_id, model_name, model_version, prediction_horizon)
        DO UPDATE SET
            predicted_default = EXCLUDED.predicted_default,
            default_probability = EXCLUDED.default_probability,
            risk_level = EXCLUDED.risk_level,
            prediction_timestamp = EXCLUDED.prediction_timestamp
    """
    with postgres_connection() as connection, connection.cursor() as cursor:
        execute_values(cursor, statement, rows)


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-streaming-prediction")
    registry = spark.read.format("delta").load(lake_path("gold/ml/model_registry")).collect()
    models = [(row, PipelineModel.load(row.model_path)) for row in registry]

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("value"),
            F.col("timestamp").alias("_ingested_at"),
            F.lit("kafka").alias("_source"),
            F.lit("stream").alias("_ingestion_type"),
            F.lit(None).cast("string").alias("_source_dataset"),
            F.lit(None).cast("string").alias("_source_split"),
        )
    )

    def predict_batch(batch, epoch_id: int) -> None:
        if batch.rdd.isEmpty():
            return
        validated = apply_quality_gate(
            parse_bronze_events(batch), f"stream_{epoch_id}", f"stream_{epoch_id}"
        ).cache()
        rejected = validated.filter(F.col("_validation_status") == "FAIL")
        rejected.write.mode("append").parquet(lake_path("rejected/streaming_transactions"))
        valid = validated.filter(F.col("_validation_status") == "PASS")
        featured = add_bnpl_features(valid).withColumn(
            "first_time_customer_num", F.col("first_time_customer").cast("double")
        )

        for metadata, model in models:
            scored = (
                model.transform(featured)
                .withColumn("default_probability", vector_to_array("probability")[1])
                .withColumn(
                    "risk_level",
                    F.when(F.col("default_probability") >= 0.7, "HIGH")
                    .when(F.col("default_probability") >= 0.4, "MEDIUM")
                    .otherwise("LOW"),
                )
                .select(
                    "transaction_id",
                    "customer_id",
                    F.lit(metadata.model_name).alias("model_name"),
                    F.lit(metadata.model_version).alias("model_version"),
                    F.lit(metadata.prediction_horizon).alias("prediction_horizon"),
                    F.col("prediction").cast("boolean").alias("predicted_default"),
                    "default_probability",
                    "risk_level",
                    F.current_timestamp().alias("prediction_timestamp"),
                )
            )
            _write_predictions([tuple(row) for row in scored.collect()])
        validated.unpersist()

    query = (
        kafka.writeStream.foreachBatch(predict_batch)
        .option("checkpointLocation", lake_path("_checkpoints/streaming_prediction"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
