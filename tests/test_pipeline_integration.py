from bnpl_common.features import add_bnpl_features
from bnpl_common.schemas import ML_FEATURE_COLUMNS
from bnpl_common.validation import apply_quality_gate, parse_bronze_events, valid_silver_rows
from tests.helpers import bronze_frame, valid_event


def test_bronze_to_silver_to_gold_contract(spark):
    bronze = bronze_frame(
        spark,
        [
            valid_event(transaction_id="TX-GOOD"),
            valid_event(transaction_id="TX-BAD", principal_ngn=-1),
        ],
    )
    validated = apply_quality_gate(
        parse_bronze_events(bronze), "integration-run", "integration-batch"
    )

    silver = valid_silver_rows(validated)
    gold = add_bnpl_features(silver).select(*ML_FEATURE_COLUMNS)

    assert validated.filter("_validation_status = 'FAIL'").count() == 1
    assert silver.count() == 1
    row = gold.first()
    assert row.transaction_id == "TX-GOOD"
    assert row.credit_score_band == "Fair"
    assert row.estimated_total_payment > row.principal_ngn
    assert row._batch_id == "integration-batch"


def test_replayed_batch_keeps_one_row_per_business_key(spark):
    bronze = bronze_frame(spark, [valid_event(transaction_id="TX-RETRY")])
    current = valid_silver_rows(
        apply_quality_gate(parse_bronze_events(bronze), "run-1", "batch-1")
    )
    replay = valid_silver_rows(
        apply_quality_gate(parse_bronze_events(bronze), "run-2", "batch-1")
    )

    merged_business_keys = current.unionByName(replay).dropDuplicates(["transaction_id"])

    assert current.count() == 1
    assert replay.count() == 1
    assert merged_business_keys.count() == 1
