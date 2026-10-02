"""Environment-backed configuration shared by every pipeline entry point."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone


DEFAULT_BNPL_DATASET_ID = "electricsheepafrica/africa-synth-banking-bnpl-nigeria"
DEFAULT_AUX_DATASET_ID = "electricsheepafrica/nigerian-banking-personal-loans"


def _integer(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to zero")
    return value


def _positive_integer(name: str, default: int) -> int:
    value = _integer(name, default)
    if value == 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _boolean(name: str, default: bool) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false", "1", "0", "yes", "no"}:
        raise ValueError(f"{name} must be a boolean value")
    return value in {"true", "1", "yes"}


def safe_identifier(value: str) -> str:
    """Return a value safe for HDFS partition and model paths."""

    normalized = re.sub(r"[^a-zA-Z0-9_.-]+", "_", value.strip())
    return normalized.strip("._-") or "unknown"


@dataclass(frozen=True)
class Settings:
    hdfs_uri: str
    hdfs_base_path: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    dataset_ids: tuple[str, ...]
    source_dataset_id: str
    dataset_split: str
    ingest_batch_size: int
    ingest_max_rows: int
    ingest_stage_reuse: bool
    ingest_output_partitions: int
    pipeline_run_id: str
    batch_id: str
    model_version: str
    kafka_max_offsets_per_trigger: int
    streaming_trigger_seconds: int
    streaming_watermark: str
    streaming_bronze_path: str
    streaming_rejected_path: str
    streaming_checkpoint_version: str
    streaming_max_records_per_file: int
    postgres_write_batch_size: int
    enforce_horizon_monotonicity: bool

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )


def get_settings() -> Settings:
    configured_ids = os.getenv("HF_DATASET_IDS") or os.getenv("HF_DATASET_ID")
    dataset_ids = tuple(
        value.strip()
        for value in (
            configured_ids.split(",")
            if configured_ids
            else [DEFAULT_BNPL_DATASET_ID, DEFAULT_AUX_DATASET_ID]
        )
        if value.strip()
    )
    if not dataset_ids:
        raise ValueError("At least one Hugging Face dataset must be configured")

    now = datetime.now(timezone.utc)
    run_id = os.getenv("PIPELINE_RUN_ID", now.strftime("manual_%Y%m%dT%H%M%SZ"))
    batch_id = os.getenv("BATCH_ID", now.strftime("batch_%Y%m%d"))

    return Settings(
        hdfs_uri=os.getenv("HDFS_URI", "hdfs://namenode:8020").rstrip("/"),
        hdfs_base_path="/" + os.getenv("HDFS_BASE_PATH", "/bnpl-data").strip("/"),
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
        postgres_port=_integer("POSTGRES_PORT", 5432),
        postgres_db=os.getenv("POSTGRES_DB", "bnpl_dw"),
        postgres_user=os.getenv("POSTGRES_USER", "bnpl"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "bnpl_password"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "bnpl.transactions.raw"),
        dataset_ids=dataset_ids,
        source_dataset_id=os.getenv("BNPL_SOURCE_DATASET_ID", DEFAULT_BNPL_DATASET_ID),
        dataset_split=os.getenv("HF_DATASET_SPLIT", "train"),
        ingest_batch_size=_positive_integer("INGEST_BATCH_SIZE", 50_000),
        ingest_max_rows=_integer("INGEST_MAX_ROWS", 0),
        ingest_stage_reuse=_boolean("INGEST_STAGE_REUSE", True),
        ingest_output_partitions=_positive_integer("INGEST_OUTPUT_PARTITIONS", 4),
        pipeline_run_id=safe_identifier(run_id),
        batch_id=safe_identifier(batch_id),
        model_version=safe_identifier(os.getenv("MODEL_VERSION", "v1")),
        kafka_max_offsets_per_trigger=_positive_integer(
            "KAFKA_MAX_OFFSETS_PER_TRIGGER", 50_000
        ),
        streaming_trigger_seconds=_positive_integer("STREAMING_TRIGGER_SECONDS", 30),
        streaming_watermark=os.getenv("STREAMING_WATERMARK", "10 minutes"),
        streaming_bronze_path=os.getenv(
            "STREAMING_BRONZE_PATH", "bronze/streaming_transactions_v2"
        ).strip("/"),
        streaming_rejected_path=os.getenv(
            "STREAMING_REJECTED_PATH", "rejected/streaming_transactions_v2"
        ).strip("/"),
        streaming_checkpoint_version=safe_identifier(
            os.getenv("STREAMING_CHECKPOINT_VERSION", "v2")
        ),
        streaming_max_records_per_file=_positive_integer(
            "STREAMING_MAX_RECORDS_PER_FILE", 100_000
        ),
        postgres_write_batch_size=_positive_integer("POSTGRES_WRITE_BATCH_SIZE", 1_000),
        enforce_horizon_monotonicity=_boolean("ENFORCE_HORIZON_MONOTONICITY", True),
    )
