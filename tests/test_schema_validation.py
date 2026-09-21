import json

from pyspark.sql import functions as F

from bnpl_common.schemas import KAFKA_EVENT_SCHEMA
from bnpl_common.validation import apply_quality_gate, parse_bronze_events
from tests.helpers import bronze_frame, valid_event


def validate(spark, events):
    return apply_quality_gate(parse_bronze_events(bronze_frame(spark, events)), "run-1", "batch-1")


def test_valid_record_passes_and_nullable_targets_are_allowed(spark):
    row = validate(spark, [valid_event()]).first()
    assert row._validation_status == "PASS"
    assert row.default_30d is None
    assert row.default_90d is None


def test_credit_score_and_principal_validation(spark):
    rows = validate(
        spark,
        [
            valid_event(transaction_id="TX-BAD-SCORE", credit_score=900),
            valid_event(transaction_id="TX-BAD-PRINCIPAL", principal_ngn=-10),
        ],
    ).collect()
    by_id = {row.transaction_id: row for row in rows}
    assert "invalid_credit_score" in by_id["TX-BAD-SCORE"]._validation_errors
    assert "invalid_principal_range" in by_id["TX-BAD-PRINCIPAL"]._validation_errors


def test_malformed_date_and_duplicate_are_quarantined(spark):
    rows = validate(
        spark,
        [valid_event(purchase_date="bad-date"), valid_event()],
    ).orderBy("message_key").collect()
    assert "invalid_purchase_date" in rows[0]._validation_errors
    assert "duplicate_transaction_id" in rows[1]._validation_errors


def test_streaming_schema_parses_label_free_event(spark):
    event = valid_event()
    event.pop("default_30d")
    event.pop("default_90d")
    frame = spark.createDataFrame([(json.dumps(event),)], ["value"])
    parsed = frame.select(F.from_json("value", KAFKA_EVENT_SCHEMA).alias("event")).first().event
    assert parsed.transaction_id == "TX-001"
    assert "default_30d" not in parsed.asDict()


def test_label_free_streaming_events_pass_or_quarantine(spark):
    valid = valid_event(transaction_id="TX-STREAM-VALID")
    invalid = valid_event(transaction_id="TX-STREAM-INVALID", credit_score=999)
    for event in (valid, invalid):
        event.pop("default_30d")
        event.pop("default_90d")

    rows = {
        row.transaction_id: row
        for row in validate(spark, [valid, invalid]).collect()
    }

    assert rows["TX-STREAM-VALID"]._validation_status == "PASS"
    assert rows["TX-STREAM-INVALID"]._validation_status == "FAIL"
    assert "invalid_credit_score" in rows["TX-STREAM-INVALID"]._validation_errors
