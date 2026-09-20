"""Incrementally upsert Gold serving tables into PostgreSQL."""

import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from psycopg2 import sql

from bnpl_common import create_spark, get_settings, is_delta_table, lake_path
from bnpl_common.postgres import postgres_connection


TABLES = {
    "dim_customer": (
        "analytics",
        ["customer_key", "customer_id", "first_time_customer"],
        ["customer_key"],
    ),
    "dim_date": (
        "analytics",
        ["date_key", "full_date", "day", "month", "month_name", "quarter", "year", "day_of_week", "week_of_year", "is_weekend"],
        ["date_key"],
    ),
    "dim_merchant": (
        "analytics",
        ["merchant_key", "merchant_name", "merchant_category"],
        ["merchant_key"],
    ),
    "dim_provider": ("analytics", ["provider_key", "provider_name"], ["provider_key"]),
    "dim_location": ("analytics", ["location_key", "customer_state"], ["location_key"]),
    "fact_bnpl_transaction": (
        "analytics",
        ["transaction_key", "transaction_id", "customer_key", "date_key", "merchant_key", "provider_key", "location_key", "principal_ngn", "interest_rate_monthly", "tenor_days", "num_installments", "credit_score", "estimated_interest", "estimated_total_payment", "installment_amount", "default_30d", "default_90d"],
        ["transaction_id"],
    ),
}


def _stage(spark, dataframe, name: str) -> None:
    settings = get_settings()
    (
        dataframe.write.format("jdbc")
        .mode("overwrite")
        .option("url", settings.postgres_jdbc_url)
        .option("dbtable", f"staging.{name}")
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .save()
    )


def _merge_staging(name: str, schema: str, columns: list[str], keys: list[str]) -> None:
    updates = [column for column in columns if column not in keys]
    statement = sql.SQL(
        "INSERT INTO {}.{} ({}) SELECT {} FROM staging.{} "
        "ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(name),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.Identifier(name),
        sql.SQL(", ").join(map(sql.Identifier, keys)),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
            for column in updates
        ),
    )
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute(statement)


def main() -> None:
    spark = create_spark("bnpl-load-postgres")
    for name, (schema, columns, keys) in TABLES.items():
        frame = spark.read.format("delta").load(lake_path(f"gold/analytics/{name}")).select(*columns)
        _stage(spark, frame, name)
        _merge_staging(name, schema, columns, keys)

    metrics_path = lake_path("gold/ml/model_metrics")
    if is_delta_table(spark, metrics_path):
        metrics = spark.read.format("delta").load(metrics_path).select(
            "model_name", "model_version", "prediction_horizon", "accuracy", "precision_score",
            "recall_score", "f1_score", "roc_auc", "training_time_seconds", "created_at",
        )
        _stage(spark, metrics, "model_metrics")
        with postgres_connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ml.model_metrics
                    (model_name, model_version, prediction_horizon, accuracy, precision_score,
                     recall_score, f1_score, roc_auc, training_time_seconds, created_at)
                SELECT model_name, model_version, prediction_horizon, accuracy, precision_score,
                       recall_score, f1_score, roc_auc, training_time_seconds, created_at
                FROM staging.model_metrics
                ON CONFLICT (model_name, model_version, prediction_horizon) DO UPDATE SET
                    accuracy = EXCLUDED.accuracy,
                    precision_score = EXCLUDED.precision_score,
                    recall_score = EXCLUDED.recall_score,
                    f1_score = EXCLUDED.f1_score,
                    roc_auc = EXCLUDED.roc_auc,
                    training_time_seconds = EXCLUDED.training_time_seconds,
                    created_at = EXCLUDED.created_at
                """
            )
    spark.stop()


if __name__ == "__main__":
    main()
