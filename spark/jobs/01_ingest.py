"""Historical Hugging Face sources -> persistent Bronze Parquet."""

import json
import os
import sys
from itertools import islice
from pathlib import Path

SPARK_ROOT = str(Path(__file__).resolve().parents[1])
if SPARK_ROOT not in sys.path:
    sys.path.insert(0, SPARK_ROOT)

from datasets import load_dataset
from pyspark.sql import functions as F

from bnpl_common import create_spark, get_settings, lake_path, safe_identifier
from bnpl_common.postgres import upsert_batch_status


def _chunks(rows, size: int):
    iterator = iter(rows)
    while batch := list(islice(iterator, size)):
        yield batch


def _record_key(row: dict, row_number: int) -> str:
    return str(row.get("transaction_id") or row.get("loan_id") or row.get("id") or row_number)


def main() -> None:
    settings = get_settings()
    spark = create_spark("bnpl-historical-to-bronze")

    for dataset_id in settings.dataset_ids:
        source_slug = safe_identifier(dataset_id)
        target = lake_path(
            f"bronze/historical_transactions/source_slug={source_slug}/batch_key={settings.batch_id}"
        )
        stream = load_dataset(dataset_id, split=settings.dataset_split, streaming=True)
        rows = iter(stream)
        if settings.ingest_max_rows:
            rows = islice(rows, settings.ingest_max_rows)

        total_rows = 0
        for batch_number, batch in enumerate(_chunks(rows, settings.ingest_batch_size), start=1):
            raw_rows = [
                {
                    "message_key": f"{dataset_id}:{_record_key(row, total_rows + index)}",
                    "value": json.dumps(row, default=str, separators=(",", ":")),
                    "_source_dataset": dataset_id,
                    "_source_split": settings.dataset_split,
                    "_source": "huggingface",
                    "_ingestion_type": "batch",
                    "_batch_id": settings.batch_id,
                    "_pipeline_run_id": settings.pipeline_run_id,
                }
                for index, row in enumerate(batch)
            ]
            mode = "overwrite" if batch_number == 1 else "append"
            (
                spark.createDataFrame(raw_rows)
                .withColumn("_ingested_at", F.current_timestamp())
                .write.mode(mode)
                .parquet(target)
            )
            total_rows += len(batch)
            print(f"source={dataset_id} batch={settings.batch_id} rows={total_rows}")

        if os.getenv("PIPELINE_REGISTRY_ENABLED", "true").lower() == "true":
            upsert_batch_status(
                settings.batch_id,
                dataset_id,
                settings.pipeline_run_id,
                "bronze_status",
                "SUCCESS",
                total_rows,
            )

    spark.stop()


if __name__ == "__main__":
    main()
