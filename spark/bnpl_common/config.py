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


def safe_identifier(value: str) -> str:
    """Return a value safe for object-store partition and model paths."""

    normalized = re.sub(r"[^a-zA-Z0-9_.-]+", "_", value.strip())
    return normalized.strip("._-") or "unknown"


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
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
    pipeline_run_id: str
    batch_id: str
    model_version: str

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
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        minio_bucket=os.getenv("MINIO_BUCKET", "bnpl-data"),
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
        postgres_port=_integer("POSTGRES_PORT", 5432),
        postgres_db=os.getenv("POSTGRES_DB", "bnpl_dw"),
        postgres_user=os.getenv("POSTGRES_USER", "bnpl"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "bnpl_password"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "bnpl.transactions.raw"),
        dataset_ids=dataset_ids,
        source_dataset_id=os.getenv("BNPL_SOURCE_DATASET_ID", DEFAULT_BNPL_DATASET_ID),
        dataset_split=os.getenv("HF_DATASET_SPLIT", "train"),
        ingest_batch_size=_integer("INGEST_BATCH_SIZE", 50_000),
        ingest_max_rows=_integer("INGEST_MAX_ROWS", 0),
        pipeline_run_id=safe_identifier(run_id),
        batch_id=safe_identifier(batch_id),
        model_version=safe_identifier(os.getenv("MODEL_VERSION", "v1")),
    )
