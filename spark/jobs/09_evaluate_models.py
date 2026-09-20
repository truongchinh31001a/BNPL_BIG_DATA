"""Evaluate candidates and select one serving model per prediction horizon."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from bnpl_common import create_spark, delta_upsert, get_settings, lake_path


def evaluate(predictions) -> tuple[float, float, float, float, float]:
    accuracy = MulticlassClassificationEvaluator(metricName="accuracy").evaluate(predictions)
    precision = MulticlassClassificationEvaluator(
        metricName="precisionByLabel", metricLabel=1.0
    ).evaluate(predictions)
    recall = MulticlassClassificationEvaluator(
        metricName="recallByLabel", metricLabel=1.0
    ).evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(metricName="f1").evaluate(predictions)
    roc_auc = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(predictions)
    return tuple(float(value) for value in (accuracy, precision, recall, f1, roc_auc))


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-evaluate-default-models")
    runs = (
        spark.read.format("delta")
        .load(lake_path("gold/ml/training_runs"))
        .filter(F.col("model_version") == settings.model_version)
        .collect()
    )
    metric_rows = []
    for run in runs:
        test = spark.read.format("delta").load(
            lake_path(
                f"gold/ml/test_sets/{run.prediction_horizon.lower()}/{run.model_version}"
            )
        )
        predictions = PipelineModel.load(run.model_path).transform(test)
        accuracy, precision, recall, f1, roc_auc = evaluate(predictions)
        metric_rows.append(
            (
                run.model_name,
                run.model_version,
                run.prediction_horizon,
                run.target_column,
                run.model_path,
                accuracy,
                precision,
                recall,
                f1,
                roc_auc,
                float(run.training_time_seconds),
            )
        )

    metrics = spark.createDataFrame(
        metric_rows,
        [
            "model_name",
            "model_version",
            "prediction_horizon",
            "target_column",
            "model_path",
            "accuracy",
            "precision_score",
            "recall_score",
            "f1_score",
            "roc_auc",
            "training_time_seconds",
        ],
    ).withColumn("created_at", F.current_timestamp())
    delta_upsert(
        spark,
        metrics,
        lake_path("gold/ml/model_metrics"),
        ["model_name", "model_version", "prediction_horizon"],
    )

    ranking = Window.partitionBy("prediction_horizon").orderBy(
        F.col("recall_score").desc(), F.col("roc_auc").desc(), F.col("model_name")
    )
    registry = (
        metrics.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .withColumn("selected_at", F.current_timestamp())
    )
    delta_upsert(
        spark,
        registry,
        lake_path("gold/ml/model_registry"),
        ["prediction_horizon"],
    )
    spark.stop()


if __name__ == "__main__":
    main()
