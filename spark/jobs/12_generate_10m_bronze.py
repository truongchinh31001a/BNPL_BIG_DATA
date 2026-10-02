"""Generate a deterministic 10-million-row raw Bronze dataset with PySpark."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, lake_path


DEFAULT_ROWS = 10_000_000
DEFAULT_OUTPUT = "bronze/generated_streaming_transactions/dataset_version=v2_10m"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--partitions", type=int, default=96)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    args = parser.parse_args()
    if args.rows < 1 or args.partitions < 1:
        parser.error("--rows and --partitions must be positive")
    return args


def categorical(column, values: list[str]):
    return F.element_at(
        F.array(*[F.lit(value) for value in values]),
        (column % len(values) + 1).cast("int"),
    )


def generated_events(spark, rows: int, partitions: int):
    identifier = F.col("id")
    event_timestamp = F.to_timestamp(
        F.from_unixtime(F.lit(1_767_225_600) + identifier % (30 * 86_400))
    )
    base = spark.range(0, rows, 1, partitions)
    events = base.select(
        F.format_string("TX_V2_%010d", identifier).alias("transaction_id"),
        F.date_format(event_timestamp, "yyyy-MM-dd HH:mm:ss").alias("purchase_date"),
        F.format_string("CU_V2_%08d", identifier % 2_000_000).alias("customer_id"),
        F.format_string("Merchant_%05d", identifier % 50_000).alias("merchant_name"),
        categorical(
            identifier,
            [
                "electronics",
                "fashion",
                "groceries",
                "health",
                "home",
                "education",
                "travel",
                "services",
            ],
        ).alias("merchant_category"),
        categorical(
            identifier,
            ["Lagos", "Abuja", "Kano", "Rivers", "Oyo", "Kaduna"],
        ).alias("customer_state"),
        (F.lit(5_000.0) + (identifier * 7_919 % 495_000)).cast("double").alias(
            "principal_ngn"
        ),
        (F.lit(1.0) + (identifier % 50) / 10.0).cast("double").alias(
            "interest_rate_monthly"
        ),
        ((identifier % 12 + 1) * 30).cast("int").alias("tenor_days"),
        (identifier % 12 + 1).cast("int").alias("num_installments"),
        categorical(
            identifier,
            ["Carbon", "Fairmoney", "Branch", "Aella", "Renmoney"],
        ).alias("provider"),
        (F.lit(300) + identifier % 551).cast("int").alias("credit_score"),
        (identifier % 4 == 0).alias("first_time_customer"),
        event_timestamp.alias("event_timestamp"),
        identifier,
    )
    event_columns = [
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
    ]
    event_struct = F.struct(*[F.col(name) for name in event_columns])
    return events.select(
        F.col("transaction_id").alias("message_key"),
        F.to_json(event_struct).alias("value"),
        event_struct.alias("_parsed_event"),
        F.lit("bnpl.transactions.generated.v2").alias("_kafka_topic"),
        (identifier % 12).cast("int").alias("_kafka_partition"),
        identifier.cast("long").alias("_kafka_offset"),
        F.col("event_timestamp").alias("_kafka_timestamp"),
        F.lit("pyspark-generator").alias("_source"),
        F.lit("generated_stream").alias("_ingestion_type"),
        F.lit("synthetic/bnpl-v2-10m").alias("_source_dataset"),
        F.lit("generated").alias("_source_split"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(True).alias("_schema_parse_ok"),
        F.to_date("event_timestamp").alias("event_date"),
        F.date_format("event_timestamp", "HH").alias("event_hour"),
    )


def main() -> None:
    args = parse_args()
    spark = create_spark("bnpl-generate-10m-bronze")
    target = lake_path(args.output)
    generated = generated_events(spark, args.rows, args.partitions)
    (
        generated.repartition(args.partitions, "event_date", "event_hour")
        .write.mode(args.mode)
        .partitionBy("event_date", "event_hour")
        .parquet(target)
    )
    actual_rows = spark.read.parquet(target).count()
    if actual_rows != args.rows:
        raise AssertionError(f"Generated {actual_rows} rows, expected {args.rows}")
    print(f"GENERATED_BRONZE_ROWS={actual_rows}")
    print(f"GENERATED_BRONZE_PATH={target}")
    spark.stop()


if __name__ == "__main__":
    main()
