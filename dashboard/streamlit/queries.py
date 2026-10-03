"""Read-only serving queries for the BNPL presentation dashboard."""

OVERVIEW = """
SELECT
    COUNT(*)::bigint AS total_transactions,
    COALESCE(SUM(principal_ngn), 0)::double precision AS total_bnpl_amount,
    COALESCE(AVG(principal_ngn), 0)::double precision AS average_loan,
    COUNT(DISTINCT customer_id)::bigint AS total_customers,
    COALESCE(AVG(CASE WHEN default_30d THEN 1.0 ELSE 0.0 END), 0)::double precision
        AS default_30d_rate,
    COALESCE(AVG(CASE WHEN default_90d THEN 1.0 ELSE 0.0 END), 0)::double precision
        AS default_90d_rate
FROM analytics.vw_bnpl_transactions
"""

MONTHLY_TREND = """
SELECT
    date_trunc('month', full_date)::date AS month,
    COUNT(*)::bigint AS transactions,
    SUM(principal_ngn)::double precision AS principal_ngn,
    AVG(CASE WHEN default_90d THEN 1.0 ELSE 0.0 END)::double precision AS default_rate
FROM analytics.vw_bnpl_transactions
GROUP BY 1
ORDER BY 1
"""

PROVIDER_PERFORMANCE = """
SELECT
    provider_name,
    COUNT(*)::bigint AS transactions,
    SUM(principal_ngn)::double precision AS principal_ngn,
    AVG(principal_ngn)::double precision AS average_loan,
    AVG(CASE WHEN default_90d THEN 1.0 ELSE 0.0 END)::double precision AS default_rate
FROM analytics.vw_bnpl_transactions
GROUP BY provider_name
ORDER BY principal_ngn DESC
"""

CATEGORY_PERFORMANCE = """
SELECT
    merchant_category,
    COUNT(*)::bigint AS transactions,
    SUM(principal_ngn)::double precision AS principal_ngn,
    AVG(CASE WHEN default_90d THEN 1.0 ELSE 0.0 END)::double precision AS default_rate
FROM analytics.vw_bnpl_transactions
GROUP BY merchant_category
ORDER BY transactions DESC
LIMIT 12
"""

STATE_PERFORMANCE = """
SELECT
    customer_state,
    COUNT(*)::bigint AS transactions,
    SUM(principal_ngn)::double precision AS principal_ngn,
    AVG(CASE WHEN default_90d THEN 1.0 ELSE 0.0 END)::double precision AS default_rate
FROM analytics.vw_bnpl_transactions
GROUP BY customer_state
ORDER BY principal_ngn DESC
LIMIT 12
"""

CREDIT_BANDS = """
SELECT
    credit_score_band,
    COUNT(*)::bigint AS transactions,
    AVG(CASE WHEN default_90d THEN 1.0 ELSE 0.0 END)::double precision AS default_rate,
    AVG(principal_ngn)::double precision AS average_loan
FROM analytics.vw_bnpl_transactions
GROUP BY credit_score_band
ORDER BY MIN(credit_score)
"""

MODEL_METRICS = """
SELECT
    model_name,
    model_version,
    prediction_horizon,
    accuracy::double precision AS accuracy,
    precision_score::double precision AS precision,
    recall_score::double precision AS recall,
    f1_score::double precision AS f1,
    roc_auc::double precision AS roc_auc,
    training_time_seconds::double precision AS training_seconds,
    created_at
FROM ml.model_metrics
ORDER BY prediction_horizon, roc_auc DESC
"""

PREDICTION_SUMMARY = """
SELECT
    prediction_horizon,
    risk_level,
    COUNT(*)::bigint AS predictions,
    AVG(default_probability)::double precision AS average_probability,
    MAX(prediction_timestamp) AS latest_prediction
FROM ml.predictions
GROUP BY prediction_horizon, risk_level
ORDER BY prediction_horizon, risk_level
"""

PREDICTION_KPIS = """
SELECT
    COUNT(*)::bigint AS total_predictions,
    COUNT(*) FILTER (WHERE risk_level = 'HIGH')::bigint AS high_risk,
    AVG(default_probability)::double precision AS average_probability,
    MAX(prediction_timestamp) AS latest_prediction
FROM ml.predictions
"""

RECENT_PREDICTIONS = """
SELECT
    transaction_id,
    customer_id,
    prediction_horizon,
    model_name,
    default_probability::double precision AS default_probability,
    risk_level,
    prediction_timestamp
FROM ml.predictions
ORDER BY prediction_timestamp DESC, prediction_id DESC
LIMIT 100
"""

PIPELINE_BATCHES = """
SELECT
    batch_id,
    source,
    row_count,
    status,
    bronze_status,
    silver_status,
    gold_status,
    pipeline_run_id,
    processed_at
FROM pipeline.pipeline_batches
ORDER BY COALESCE(processed_at, received_at) DESC
LIMIT 20
"""

DATA_QUALITY = """
SELECT
    metric_name,
    metric_value,
    layer,
    batch_id,
    created_at
FROM data_quality.data_quality_metrics
ORDER BY created_at DESC, metric_name
LIMIT 30
"""

AIRFLOW_RUN = """
SELECT dag_id, run_id, state, start_date, end_date
FROM public.dag_run
WHERE dag_id = 'bnpl_batch_pipeline'
ORDER BY execution_date DESC
LIMIT 1
"""
