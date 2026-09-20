CREATE TABLE IF NOT EXISTS pipeline.pipeline_batches (
    batch_id TEXT NOT NULL,
    source TEXT NOT NULL,
    source_file TEXT,
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_count BIGINT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    bronze_status TEXT NOT NULL DEFAULT 'PENDING',
    silver_status TEXT NOT NULL DEFAULT 'PENDING',
    gold_status TEXT NOT NULL DEFAULT 'PENDING',
    pipeline_run_id TEXT NOT NULL,
    processed_at TIMESTAMP,
    PRIMARY KEY (batch_id, source)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_batches_run
    ON pipeline.pipeline_batches(pipeline_run_id);
