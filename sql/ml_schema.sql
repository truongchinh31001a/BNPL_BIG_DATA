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
    prediction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_predictions_model_horizon
        UNIQUE (transaction_id, model_name, model_version, prediction_horizon)
);

ALTER TABLE ml.predictions ADD COLUMN IF NOT EXISTS prediction_horizon TEXT;
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
    MAX(prediction_timestamp) AS latest_prediction_at
FROM ml.predictions
GROUP BY prediction_horizon, risk_level, predicted_default;
