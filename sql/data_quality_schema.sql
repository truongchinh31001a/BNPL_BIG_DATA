CREATE TABLE IF NOT EXISTS data_quality.data_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source TEXT NOT NULL,
    layer TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pipeline_run_id, batch_id, source, layer, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_batch
    ON data_quality.data_quality_metrics(batch_id, source);
