"""Build idempotent Gold Delta star-schema tables."""

import os
import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, delta_upsert, get_settings, lake_path
from bnpl_common.postgres import upsert_batch_status


def _key(*columns):
    return F.xxhash64(*[F.coalesce(F.col(column).cast("string"), F.lit("")) for column in columns])


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-build-star-schema")
    df = spark.read.format("delta").load(lake_path("gold/shared/enriched_transactions"))
    current = df.filter(F.col("_batch_id") == settings.batch_id)

    dim_customer = current.select("customer_id", "first_time_customer").dropDuplicates(["customer_id"]).withColumn(
        "customer_key", _key("customer_id")
    )
    dim_date = (
        current.select(F.col("purchase_date").alias("full_date"))
        .dropDuplicates()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    )
    dim_merchant = current.select("merchant_name", "merchant_category").dropDuplicates().withColumn(
        "merchant_key", _key("merchant_name", "merchant_category")
    )
    dim_provider = current.select(F.col("provider").alias("provider_name")).dropDuplicates().withColumn(
        "provider_key", _key("provider_name")
    )
    dim_location = current.select("customer_state").dropDuplicates().withColumn(
        "location_key", _key("customer_state")
    )

    fact = (
        current.withColumn("customer_key", _key("customer_id"))
        .withColumn("date_key", F.date_format("purchase_date", "yyyyMMdd").cast("int"))
        .withColumn("merchant_key", _key("merchant_name", "merchant_category"))
        .withColumn("provider_key", _key("provider"))
        .withColumn("location_key", _key("customer_state"))
        .select(
            "transaction_id", "customer_key", "date_key", "merchant_key", "provider_key", "location_key",
            "principal_ngn", "interest_rate_monthly", "tenor_days", "num_installments", "credit_score",
            "estimated_interest", "estimated_total_payment", "installment_amount", "default_30d", "default_90d",
        )
        .withColumn("transaction_key", _key("transaction_id"))
    )

    tables = [
        (dim_customer, "dim_customer", ["customer_key"]),
        (dim_date, "dim_date", ["date_key"]),
        (dim_merchant, "dim_merchant", ["merchant_key"]),
        (dim_provider, "dim_provider", ["provider_key"]),
        (dim_location, "dim_location", ["location_key"]),
        (fact, "fact_bnpl_transaction", ["transaction_id"]),
    ]
    for table, name, keys in tables:
        delta_upsert(spark, table, lake_path(f"gold/analytics/{name}"), keys)

    if os.getenv("PIPELINE_REGISTRY_ENABLED", "true").lower() == "true":
        upsert_batch_status(
            settings.batch_id,
            settings.source_dataset_id,
            settings.pipeline_run_id,
            "gold_status",
            "SUCCESS",
            fact.count(),
        )
    spark.stop()


if __name__ == "__main__":
    main()
