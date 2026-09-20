import json
from datetime import datetime


def valid_event(**overrides):
    event = {
        "transaction_id": "TX-001",
        "purchase_date": "2026-09-17T16:45:20",
        "customer_id": "CUS-001",
        "merchant_name": "Example Store",
        "merchant_category": "electronics",
        "customer_state": "Lagos",
        "principal_ngn": 125000.0,
        "interest_rate_monthly": 0.04,
        "tenor_days": 60,
        "num_installments": 3,
        "provider": "Provider A",
        "credit_score": 612,
        "first_time_customer": False,
        "default_30d": None,
        "default_90d": None,
    }
    event.update(overrides)
    return event


def bronze_frame(spark, events):
    rows = [
        (
            f"key-{index}",
            json.dumps(event),
            "test/source",
            "train",
            datetime(2026, 9, 17, 12, 0, index),
        )
        for index, event in enumerate(events)
    ]
    return spark.createDataFrame(
        rows,
        ["message_key", "value", "_source_dataset", "_source_split", "_ingested_at"],
    )
