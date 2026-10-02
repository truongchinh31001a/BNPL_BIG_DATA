"""Print row counts for the materialized HDFS Bronze, Silver, and Gold tables."""

from __future__ import annotations

import sys
from pathlib import Path


SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from bnpl_common import create_spark, lake_path


TABLES = [
    (
        "bronze_bnpl",
        "parquet",
        "bronze/historical_transactions/"
        "source_slug=electricsheepafrica_africa-synth-banking-bnpl-nigeria",
    ),
    (
        "bronze_personal_loans",
        "parquet",
        "bronze/historical_transactions/"
        "source_slug=electricsheepafrica_nigerian-banking-personal-loans",
    ),
    ("silver_transactions", "delta", "silver/transactions"),
    ("enriched_transactions", "delta", "gold/shared/enriched_transactions"),
    ("dim_customer", "delta", "gold/analytics/dim_customer"),
    ("dim_date", "delta", "gold/analytics/dim_date"),
    ("dim_location", "delta", "gold/analytics/dim_location"),
    ("dim_merchant", "delta", "gold/analytics/dim_merchant"),
    ("dim_provider", "delta", "gold/analytics/dim_provider"),
    ("fact_bnpl_transaction", "delta", "gold/analytics/fact_bnpl_transaction"),
    ("data_quality_metrics", "delta", "gold/data_quality/metrics"),
    ("ml_bnpl_features", "delta", "gold/ml/ml_bnpl_features"),
    ("model_metrics", "delta", "gold/ml/model_metrics"),
    ("model_registry", "delta", "gold/ml/model_registry"),
    ("test_set_30d", "delta", "gold/ml/test_sets/30d/v1"),
    ("test_set_90d", "delta", "gold/ml/test_sets/90d/v1"),
    ("training_runs", "delta", "gold/ml/training_runs"),
]


def main() -> None:
    spark = create_spark("bnpl-inspect-lakehouse")
    try:
        for name, storage_format, relative_path in TABLES:
            row_count = (
                spark.read.format(storage_format)
                .load(lake_path(relative_path))
                .count()
            )
            print(f"LAKEHOUSE_COUNT|{name}|{storage_format}|{row_count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
