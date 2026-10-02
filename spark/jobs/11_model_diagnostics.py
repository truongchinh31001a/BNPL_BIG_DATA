"""Report confusion-matrix diagnostics for the selected serving models."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import functions as F

from bnpl_common import create_spark, get_settings, lake_path


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-selected-model-diagnostics")
    selected_models = (
        spark.read.format("delta")
        .load(lake_path("gold/ml/model_registry"))
        .filter(F.col("model_version") == settings.model_version)
        .collect()
    )

    rows = []
    for selected in selected_models:
        test = spark.read.format("delta").load(
            lake_path(
                f"gold/ml/test_sets/{selected.prediction_horizon.lower()}/"
                f"{selected.model_version}"
            )
        )
        predictions = PipelineModel.load(selected.model_path).transform(test)
        counts = {
            (int(row.label), int(row.prediction)): row["count"]
            for row in predictions.groupBy("label", "prediction").count().collect()
        }
        true_positive = counts.get((1, 1), 0)
        false_negative = counts.get((1, 0), 0)
        false_positive = counts.get((0, 1), 0)
        true_negative = counts.get((0, 0), 0)
        rows.append(
            (
                selected.prediction_horizon,
                selected.model_name,
                true_positive,
                false_negative,
                false_positive,
                true_negative,
                true_positive / (true_positive + false_negative),
            )
        )

    spark.createDataFrame(
        rows,
        [
            "prediction_horizon",
            "model_name",
            "true_positive",
            "false_negative",
            "false_positive",
            "true_negative",
            "default_recall",
        ],
    ).orderBy("prediction_horizon").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
