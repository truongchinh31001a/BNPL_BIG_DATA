CREATE TABLE IF NOT EXISTS ml.predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    customer_id TEXT,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    prediction_horizon TEXT NOT NULL CHECK (prediction_horizon IN ('30D', '90D')),
    predicted_default BOOLEAN NOT NULL,
    default_probability NUMERIC(8, 6) NOT NULL CHECK (default_probability BETWEEN 0 AND 1),
    risk_level TEXT NOT NULL,
    event_timestamp TIMESTAMP,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    prediction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_predictions_model_horizon
        UNIQUE (transaction_id, model_name, model_version, prediction_horizon)
);

ALTER TABLE ml.predictions ADD COLUMN IF NOT EXISTS prediction_horizon TEXT;
ALTER TABLE ml.predictions ADD COLUMN IF NOT EXISTS event_timestamp TIMESTAMP;
ALTER TABLE ml.predictions ADD COLUMN IF NOT EXISTS kafka_partition INTEGER;
ALTER TABLE ml.predictions ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;
UPDATE ml.predictions SET prediction_horizon = '90D' WHERE prediction_horizon IS NULL;
ALTER TABLE ml.predictions ALTER COLUMN prediction_horizon SET NOT NULL;
ALTER TABLE ml.predictions DROP CONSTRAINT IF EXISTS predictions_transaction_id_model_name_model_version_key;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_predictions_model_horizon'
          AND conrelid = 'ml.predictions'::regclass
    ) THEN
        ALTER TABLE ml.predictions ADD CONSTRAINT uq_predictions_model_horizon
            UNIQUE (transaction_id, model_name, model_version, prediction_horizon);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ml.model_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    prediction_horizon TEXT NOT NULL CHECK (prediction_horizon IN ('30D', '90D')),
    accuracy NUMERIC(8, 6),
    precision_score NUMERIC(8, 6),
    recall_score NUMERIC(8, 6),
    f1_score NUMERIC(8, 6),
    roc_auc NUMERIC(8, 6),
    training_time_seconds NUMERIC(18, 3),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_model_metrics_horizon
        UNIQUE (model_name, model_version, prediction_horizon)
);

ALTER TABLE ml.model_metrics ADD COLUMN IF NOT EXISTS prediction_horizon TEXT;
UPDATE ml.model_metrics SET prediction_horizon = '90D' WHERE prediction_horizon IS NULL;
ALTER TABLE ml.model_metrics ALTER COLUMN prediction_horizon SET NOT NULL;
ALTER TABLE ml.model_metrics DROP CONSTRAINT IF EXISTS model_metrics_model_name_model_version_key;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_model_metrics_horizon'
          AND conrelid = 'ml.model_metrics'::regclass
    ) THEN
        ALTER TABLE ml.model_metrics ADD CONSTRAINT uq_model_metrics_horizon
            UNIQUE (model_name, model_version, prediction_horizon);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_predictions_transaction_id
    ON ml.predictions(transaction_id);

CREATE INDEX IF NOT EXISTS idx_predictions_model
    ON ml.predictions(model_name, model_version, prediction_horizon);

CREATE OR REPLACE VIEW ml.vw_prediction_monitoring AS
SELECT
    prediction_horizon,
    risk_level,
    predicted_default,
    COUNT(*) AS transaction_count,
    AVG(default_probability) AS average_default_probability,
    MAX(prediction_timestamp) AS latest_prediction_at,
    AVG(EXTRACT(EPOCH FROM (prediction_timestamp - event_timestamp)))
        FILTER (WHERE event_timestamp IS NOT NULL) AS average_latency_seconds
FROM ml.predictions
GROUP BY prediction_horizon, risk_level, predicted_default;

CREATE OR REPLACE VIEW ml.vw_streaming_latency AS
SELECT
    prediction_horizon,
    COUNT(*) AS prediction_count,
    percentile_cont(0.50) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (prediction_timestamp - event_timestamp))
    ) AS latency_p50_seconds,
    percentile_cont(0.95) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (prediction_timestamp - event_timestamp))
    ) AS latency_p95_seconds,
    percentile_cont(0.99) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (prediction_timestamp - event_timestamp))
    ) AS latency_p99_seconds,
    MAX(prediction_timestamp) AS latest_prediction_at
FROM ml.predictions
WHERE event_timestamp IS NOT NULL
GROUP BY prediction_horizon;

CREATE OR REPLACE VIEW ml.vw_streaming_latency_recent AS
SELECT
    prediction_horizon,
    COUNT(*) AS prediction_count,
    percentile_cont(0.50) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (prediction_timestamp - event_timestamp))
    ) AS latency_p50_seconds,
    percentile_cont(0.95) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (prediction_timestamp - event_timestamp))
    ) AS latency_p95_seconds,
    percentile_cont(0.99) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (prediction_timestamp - event_timestamp))
    ) AS latency_p99_seconds,
    MAX(prediction_timestamp) AS latest_prediction_at
FROM ml.predictions
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
GROUP BY prediction_horizon;

CREATE OR REPLACE VIEW ml.vw_horizon_consistency AS
SELECT
    COUNT(*) AS paired_transactions,
    COUNT(*) FILTER (WHERE p30.default_probability > p90.default_probability)
        AS probability_violations,
    COUNT(*) FILTER (WHERE p30.predicted_default AND NOT p90.predicted_default)
        AS prediction_violations
FROM ml.predictions p30
JOIN ml.predictions p90
  ON p90.transaction_id = p30.transaction_id
 AND p90.model_version = p30.model_version
 AND p90.prediction_horizon = '90D'
WHERE p30.prediction_horizon = '30D';
