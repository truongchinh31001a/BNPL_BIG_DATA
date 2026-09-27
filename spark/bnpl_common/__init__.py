"""Shared building blocks for BNPL batch and streaming Spark jobs."""

from .config import Settings, get_settings, safe_identifier
from .delta import delta_upsert, is_delta_table
from .drift import DRIFT_FEATURES, add_drift_buckets, calculate_psi
from .features import add_bnpl_features
from .schemas import KAFKA_EVENT_SCHEMA, SILVER_COLUMNS
from .spark_session import create_spark, lake_path
from .validation import apply_quality_gate, parse_bronze_events

__all__ = [
    "KAFKA_EVENT_SCHEMA",
    "SILVER_COLUMNS",
    "DRIFT_FEATURES",
    "Settings",
    "add_bnpl_features",
    "add_drift_buckets",
    "apply_quality_gate",
    "create_spark",
    "calculate_psi",
    "delta_upsert",
    "get_settings",
    "is_delta_table",
    "lake_path",
    "parse_bronze_events",
    "safe_identifier",
]
