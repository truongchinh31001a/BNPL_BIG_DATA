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

from bnpl_common import BRONZE_STREAMING_SCHEMA, add_bnpl_features, apply_quality_gate, create_spark, lake_path, parse_bronze_events
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
        bronze.writeStream.foreachBatch(predict_batch)
        .option("checkpointLocation", lake_path("_checkpoints/bronze_to_prediction_v1"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
