"""The persisted Bronze file contract feeds the existing quality/feature path."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from bnpl_common import BRONZE_STREAMING_SCHEMA, KAFKA_EVENT_SCHEMA, add_bnpl_features, apply_quality_gate, parse_bronze_events
from tests.helpers import bronze_frame, valid_event


def test_bronze_streaming_schema_matches_writer_columns():
    expected = {
        "message_key": StringType,
        "value": StringType,
        "_parsed_event": StructType,
        "_kafka_topic": StringType,
        "_kafka_partition": IntegerType,
        "_kafka_offset": LongType,
        "_kafka_timestamp": TimestampType,
        "_source": StringType,
        "_ingestion_type": StringType,
        "_source_dataset": StringType,
        "_source_split": StringType,
        "_ingested_at": TimestampType,
        "_schema_parse_ok": BooleanType,
        "event_date": DateType,
        "event_hour": StringType,
    }
    assert {field.name: type(field.dataType) for field in BRONZE_STREAMING_SCHEMA} == expected
    assert BRONZE_STREAMING_SCHEMA["_parsed_event"].dataType == KAFKA_EVENT_SCHEMA


def test_label_free_bronze_event_reuses_gate_and_features(spark):
    event = valid_event()
    del event["default_30d"]
    del event["default_90d"]
    raw = bronze_frame(spark, [event]).withColumn(
        "_parsed_event", F.from_json("value", KAFKA_EVENT_SCHEMA)
    )
    row = add_bnpl_features(
        apply_quality_gate(parse_bronze_events(raw), "stream-test", "batch-test")
    ).first()
    assert row._validation_status == "PASS"
    assert row.default_30d is None and row.default_90d is None
    assert row.estimated_total_payment > event["principal_ngn"]
