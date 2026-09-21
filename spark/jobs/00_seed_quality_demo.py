"""Seed a small Bronze batch with deterministic valid and invalid records."""

import json
import sys
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from pyspark.sql import functions as F

from bnpl_common import create_spark, get_settings, lake_path, safe_identifier


def _event(transaction_id: str, **overrides):
    event = {
        "transaction_id": transaction_id,
        "purchase_date": "2026-09-21",
        "customer_id": "CUS-QUALITY-DEMO",
        "merchant_name": "Quality Demo Store",
        "merchant_category": "electronics",
        "customer_state": "Lagos",
        "principal_ngn": 25000.0,
        "interest_rate_monthly": 0.03,
        "tenor_days": 30,
        "num_installments": 2,
        "provider": "Demo Provider",
        "credit_score": 680,
        "first_time_customer": False,
        "default_30d": False,
        "default_90d": False,
    }
    event.update(overrides)
    return event


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-seed-quality-demo")
    source_slug = safe_identifier(settings.source_dataset_id)
    target = lake_path(
        f"bronze/historical_transactions/source_slug={source_slug}/batch_key={settings.batch_id}"
    )

    events = [
        _event("TX-QUALITY-VALID"),
        _event(
            "TX-QUALITY-INVALID",
            purchase_date="not-a-date",
            principal_ngn=-100.0,
            credit_score=999,
        ),
        _event("TX-QUALITY-DUPLICATE"),
        _event("TX-QUALITY-DUPLICATE"),
    ]
    rows = [
        {
            "message_key": f"quality-demo-{index}",
            "value": json.dumps(event, separators=(",", ":")),
            "_source_dataset": settings.source_dataset_id,
            "_source_split": "quality_demo",
            "_source": "quality_demo",
            "_ingestion_type": "batch",
            "_batch_id": settings.batch_id,
            "_pipeline_run_id": settings.pipeline_run_id,
        }
        for index, event in enumerate(events, start=1)
    ]
    (
        spark.createDataFrame(rows)
        .withColumn("_ingested_at", F.current_timestamp())
        .write.mode("overwrite")
        .parquet(target)
    )
    print(f"quality_demo_batch={settings.batch_id} rows={len(rows)} target={target}")
    spark.stop()


if __name__ == "__main__":
    main()
