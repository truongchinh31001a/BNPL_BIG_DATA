"""Train reproducible Spark ML candidates for both default horizons."""

import sys
import time
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, delta_upsert, get_settings, lake_path
from bnpl_common.ml import build_pipeline, classifier_candidates


HORIZONS = {"30D": "default_30d", "90D": "default_90d"}


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-train-default-models")
    features = (
        spark.read.format("delta")
        .load(lake_path("gold/ml/ml_bnpl_features"))
        .withColumn("first_time_customer_num", F.col("first_time_customer").cast("double"))
    )
    training_rows = []

    for horizon, target in HORIZONS.items():
        labelled = features.filter(F.col(target).isNotNull()).withColumn(
            "label", F.col(target).cast("double")
        )
        train, test = (frame.cache() for frame in labelled.randomSplit([0.8, 0.2], seed=42))
        training_count = train.count()
        test_count = test.count()
        test.write.format("delta").mode("overwrite").save(
            lake_path(f"gold/ml/test_sets/{horizon.lower()}/{settings.model_version}")
        )

        for model_name, classifier in classifier_candidates().items():
            started_at = time.perf_counter()
            model = build_pipeline(classifier).fit(train)
            model_path = lake_path(
                f"models/{target}/{model_name}/{settings.model_version}"
            )
            model.write().overwrite().save(model_path)
            training_rows.append(
                (
                    model_name,
                    settings.model_version,
                    horizon,
                    target,
                    model_path,
                    float(time.perf_counter() - started_at),
                    training_count,
                    test_count,
                )
            )
        train.unpersist()
        test.unpersist()

    training_runs = spark.createDataFrame(
        training_rows,
        [
            "model_name",
            "model_version",
            "prediction_horizon",
            "target_column",
            "model_path",
            "training_time_seconds",
            "training_row_count",
            "test_row_count",
        ],
    ).withColumn("created_at", F.current_timestamp())
    delta_upsert(
        spark,
        training_runs,
        lake_path("gold/ml/training_runs"),
        ["model_name", "model_version", "prediction_horizon"],
    )
    spark.stop()


if __name__ == "__main__":
    main()
