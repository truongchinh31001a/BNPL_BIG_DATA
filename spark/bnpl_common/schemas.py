"""Canonical BNPL schemas and controlled schema-evolution policy."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


REQUIRED_SOURCE_COLUMNS = {
    "transaction_id",
    "purchase_date",
    "customer_id",
    "merchant_name",
    "merchant_category",
    "customer_state",
    "principal_ngn",
    "interest_rate_monthly",
    "tenor_days",
    "num_installments",
    "provider",
    "credit_score",
    "first_time_customer",
}

OPTIONAL_SOURCE_COLUMNS = {
    "first_payment_due",
    "default_30d",
    "default_90d",
    "_source_dataset",
    "_source_split",
}

EXPECTED_SOURCE_COLUMNS = REQUIRED_SOURCE_COLUMNS | OPTIONAL_SOURCE_COLUMNS

KAFKA_EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("purchase_date", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), False),
        StructField("customer_state", StringType(), False),
        StructField("principal_ngn", DoubleType(), False),
        StructField("interest_rate_monthly", DoubleType(), False),
        StructField("tenor_days", IntegerType(), False),
        StructField("num_installments", IntegerType(), False),
        StructField("provider", StringType(), False),
        StructField("credit_score", IntegerType(), False),
        StructField("first_time_customer", BooleanType(), False),
    ]
)

SILVER_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("purchase_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_name", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("customer_state", StringType(), False),
        StructField("principal_ngn", DoubleType(), False),
        StructField("interest_rate_monthly", DoubleType(), False),
        StructField("tenor_days", IntegerType(), False),
        StructField("num_installments", IntegerType(), False),
        StructField("provider", StringType(), False),
        StructField("credit_score", IntegerType(), False),
        StructField("first_time_customer", BooleanType(), False),
        StructField("default_30d", BooleanType(), True),
        StructField("default_90d", BooleanType(), True),
    ]
)

SILVER_COLUMNS = [field.name for field in SILVER_SCHEMA.fields]

ML_FEATURE_COLUMNS = [
    "transaction_id",
    "customer_id",
    "principal_ngn",
    "interest_rate_monthly",
    "tenor_days",
    "num_installments",
    "credit_score",
    "first_time_customer",
    "merchant_category",
    "provider",
    "customer_state",
    "credit_score_band",
    "loan_size_category",
    "estimated_interest",
    "estimated_total_payment",
    "installment_amount",
    "purchase_month",
    "purchase_quarter",
    "purchase_day_of_week",
    "default_30d",
    "default_90d",
    "_batch_id",
    "_pipeline_run_id",
    "_source_dataset",
    "_ingested_at",
]
