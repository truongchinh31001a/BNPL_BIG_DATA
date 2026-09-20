"""Explicit parsing, schema checks, and the Bronze-to-Silver quality gate."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

from .schemas import EXPECTED_SOURCE_COLUMNS, REQUIRED_SOURCE_COLUMNS, SILVER_COLUMNS


def _map_value(name: str):
    return F.element_at(F.col("_raw_map"), F.lit(name))


def _boolean_value(column):
    normalized = F.lower(F.trim(column.cast("string")))
    return F.when(normalized.isin("true", "1", "t", "yes", "y"), F.lit(True)).when(
        normalized.isin("false", "0", "f", "no", "n"), F.lit(False)
    )


def parse_bronze_events(bronze: DataFrame) -> DataFrame:
    """Parse raw JSON without allowing Spark to infer or evolve the schema."""

    parsed = bronze.withColumn(
        "_raw_map",
        F.from_json(F.col("value"), MapType(StringType(), StringType())),
    )
    known_columns = F.array(*[F.lit(name) for name in sorted(EXPECTED_SOURCE_COLUMNS)])
    parsed = parsed.withColumn(
        "_unexpected_columns",
        F.array_sort(F.array_except(F.map_keys("_raw_map"), known_columns)),
    )

    raw_names = sorted(REQUIRED_SOURCE_COLUMNS | {"default_30d", "default_90d"})
    for name in raw_names:
        parsed = parsed.withColumn(f"_raw_{name}", _map_value(name))

    purchase_text = F.trim(F.col("_raw_purchase_date"))
    parsed = (
        parsed.withColumn("transaction_id", F.trim(F.col("_raw_transaction_id")))
        .withColumn("purchase_date", F.to_date(F.to_timestamp(purchase_text)))
        .withColumn("customer_id", F.trim(F.col("_raw_customer_id")))
        .withColumn("merchant_name", F.initcap(F.trim(F.col("_raw_merchant_name"))))
        .withColumn("merchant_category", F.lower(F.trim(F.col("_raw_merchant_category"))))
        .withColumn("customer_state", F.initcap(F.trim(F.col("_raw_customer_state"))))
        .withColumn("principal_ngn", F.col("_raw_principal_ngn").cast("double"))
        .withColumn("interest_rate_monthly", F.col("_raw_interest_rate_monthly").cast("double"))
        .withColumn("tenor_days", F.col("_raw_tenor_days").cast("int"))
        .withColumn("num_installments", F.col("_raw_num_installments").cast("int"))
        .withColumn("provider", F.initcap(F.trim(F.col("_raw_provider"))))
        .withColumn("credit_score", F.col("_raw_credit_score").cast("int"))
        .withColumn("first_time_customer", _boolean_value(F.col("_raw_first_time_customer")))
        .withColumn("default_30d", _boolean_value(F.col("_raw_default_30d")))
        .withColumn("default_90d", _boolean_value(F.col("_raw_default_90d")))
        .withColumn(
            "_source_dataset",
            F.coalesce(F.col("_source_dataset"), _map_value("_source_dataset")),
        )
        .withColumn("_source_split", F.coalesce(F.col("_source_split"), _map_value("_source_split")))
    )
    return parsed


def apply_quality_gate(df: DataFrame, pipeline_run_id: str, batch_id: str) -> DataFrame:
    """Attach validation errors; target NULLs are valid for streaming inference."""

    duplicate_window = Window.partitionBy("transaction_id").orderBy(
        F.col("_ingested_at").asc_nulls_last(), F.col("message_key").asc_nulls_last()
    )
    checked = df.withColumn("_duplicate_rank", F.row_number().over(duplicate_window))

    bool_values = ("true", "false", "1", "0", "t", "f", "yes", "no", "y", "n")
    checks = [
        F.when(F.col("_raw_map").isNull(), F.lit("malformed_json")),
        F.when(F.col("transaction_id").isNull() | (F.col("transaction_id") == ""), "missing_transaction_id"),
        F.when(F.col("purchase_date").isNull(), "invalid_purchase_date"),
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing_customer_id"),
        F.when(F.col("merchant_name").isNull() | (F.col("merchant_name") == ""), "missing_merchant_name"),
        F.when(F.col("merchant_category").isNull() | (F.col("merchant_category") == ""), "missing_merchant_category"),
        F.when(F.col("customer_state").isNull() | (F.col("customer_state") == ""), "missing_customer_state"),
        F.when(F.col("provider").isNull() | (F.col("provider") == ""), "missing_provider"),
        F.when(F.col("principal_ngn").isNull(), "invalid_principal_type"),
        F.when(F.col("principal_ngn") <= 0, "invalid_principal_range"),
        F.when(F.col("interest_rate_monthly").isNull(), "invalid_interest_rate_type"),
        F.when(F.col("interest_rate_monthly") < 0, "invalid_interest_rate_range"),
        F.when(F.col("tenor_days").isNull() | (F.col("tenor_days") <= 0), "invalid_tenor_days"),
        F.when(F.col("num_installments").isNull() | (F.col("num_installments") <= 0), "invalid_num_installments"),
        F.when(F.col("credit_score").isNull() | ~F.col("credit_score").between(300, 850), "invalid_credit_score"),
        F.when(F.col("first_time_customer").isNull(), "invalid_first_time_customer"),
        F.when(
            F.col("_raw_default_30d").isNotNull()
            & ~F.lower(F.trim(F.col("_raw_default_30d"))).isin(*bool_values),
            "invalid_default_30d",
        ),
        F.when(
            F.col("_raw_default_90d").isNotNull()
            & ~F.lower(F.trim(F.col("_raw_default_90d"))).isin(*bool_values),
            "invalid_default_90d",
        ),
        F.when(F.col("_duplicate_rank") > 1, "duplicate_transaction_id"),
    ]
    errors = F.filter(F.array(*checks), lambda value: value.isNotNull())
    return (
        checked.withColumn("_validation_errors", errors)
        .withColumn(
            "_validation_status",
            F.when(F.size("_validation_errors") == 0, "PASS").otherwise("FAIL"),
        )
        .withColumn(
            "_schema_status",
            F.when(F.size("_unexpected_columns") == 0, "EXPECTED").otherwise("COMPATIBLE_EXTRA_COLUMNS"),
        )
        .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn(
            "_rejected_at",
            F.when(F.col("_validation_status") == "FAIL", F.current_timestamp()),
        )
    )


def valid_silver_rows(validated: DataFrame) -> DataFrame:
    metadata = ["_source_dataset", "_source_split", "_ingested_at", "_pipeline_run_id", "_batch_id"]
    return validated.filter(F.col("_validation_status") == "PASS").select(*(SILVER_COLUMNS + metadata))
