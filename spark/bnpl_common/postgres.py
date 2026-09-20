"""PostgreSQL helpers for low-volume observability metadata and JDBC exports."""

from __future__ import annotations

from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values

from .config import get_settings


@contextmanager
def postgres_connection():
    connection = psycopg2.connect(get_settings().postgres_dsn)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def upsert_quality_metrics(rows: list[tuple]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO data_quality.data_quality_metrics
            (pipeline_run_id, batch_id, source, layer, metric_name, metric_value)
        VALUES %s
        ON CONFLICT (pipeline_run_id, batch_id, source, layer, metric_name)
        DO UPDATE SET metric_value = EXCLUDED.metric_value, created_at = CURRENT_TIMESTAMP
    """
    with postgres_connection() as connection, connection.cursor() as cursor:
        execute_values(cursor, sql, rows)


def upsert_batch_status(
    batch_id: str,
    source: str,
    pipeline_run_id: str,
    status_column: str,
    status: str,
    row_count: int | None = None,
) -> None:
    allowed = {"status", "bronze_status", "silver_status", "gold_status"}
    if status_column not in allowed:
        raise ValueError(f"Unsupported batch status column: {status_column}")
    sql = f"""
        INSERT INTO pipeline.pipeline_batches
            (batch_id, source, pipeline_run_id, status, {status_column}, row_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id, source) DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            {status_column} = EXCLUDED.{status_column},
            status = EXCLUDED.status,
            row_count = COALESCE(EXCLUDED.row_count, pipeline.pipeline_batches.row_count),
            processed_at = CASE WHEN EXCLUDED.status = 'SUCCESS' THEN CURRENT_TIMESTAMP ELSE NULL END
    """
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute(sql, (batch_id, source, pipeline_run_id, status, status, row_count))
