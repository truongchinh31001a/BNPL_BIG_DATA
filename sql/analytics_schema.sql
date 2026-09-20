CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE,
    first_time_customer BOOLEAN
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_merchant (
    merchant_key BIGSERIAL PRIMARY KEY,
    merchant_name TEXT NOT NULL,
    merchant_category TEXT,
    UNIQUE (merchant_name, merchant_category)
);

CREATE TABLE IF NOT EXISTS analytics.dim_provider (
    provider_key BIGSERIAL PRIMARY KEY,
    provider_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS analytics.dim_location (
    location_key BIGSERIAL PRIMARY KEY,
    customer_state TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS analytics.fact_bnpl_transaction (
    transaction_key BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    customer_key BIGINT NOT NULL REFERENCES analytics.dim_customer(customer_key),
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    merchant_key BIGINT NOT NULL REFERENCES analytics.dim_merchant(merchant_key),
    provider_key BIGINT NOT NULL REFERENCES analytics.dim_provider(provider_key),
    location_key BIGINT NOT NULL REFERENCES analytics.dim_location(location_key),
    principal_ngn NUMERIC(18, 2) NOT NULL CHECK (principal_ngn > 0),
    interest_rate_monthly NUMERIC(9, 6) NOT NULL CHECK (interest_rate_monthly >= 0),
    tenor_days INTEGER NOT NULL CHECK (tenor_days > 0),
    num_installments INTEGER NOT NULL CHECK (num_installments > 0),
    credit_score INTEGER CHECK (credit_score BETWEEN 300 AND 850),
    estimated_interest NUMERIC(18, 2),
    estimated_total_payment NUMERIC(18, 2),
    installment_amount NUMERIC(18, 2),
    default_30d BOOLEAN,
    default_90d BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_fact_bnpl_date_key
    ON analytics.fact_bnpl_transaction(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_bnpl_customer_key
    ON analytics.fact_bnpl_transaction(customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_bnpl_provider_key
    ON analytics.fact_bnpl_transaction(provider_key);

CREATE OR REPLACE VIEW analytics.vw_bnpl_transactions AS
SELECT
    f.*,
    d.full_date,
    d.month,
    d.quarter,
    d.year,
    m.merchant_name,
    m.merchant_category,
    p.provider_name,
    l.customer_state,
    c.customer_id,
    c.first_time_customer,
    CASE
        WHEN f.credit_score < 580 THEN 'Poor'
        WHEN f.credit_score < 670 THEN 'Fair'
        WHEN f.credit_score < 740 THEN 'Good'
        WHEN f.credit_score < 800 THEN 'Very Good'
        ELSE 'Excellent'
    END AS credit_score_band,
    CASE
        WHEN f.principal_ngn < 50000 THEN 'Small'
        WHEN f.principal_ngn < 200000 THEN 'Medium'
        ELSE 'Large'
    END AS loan_size_category
FROM analytics.fact_bnpl_transaction f
JOIN analytics.dim_date d ON d.date_key = f.date_key
JOIN analytics.dim_merchant m ON m.merchant_key = f.merchant_key
JOIN analytics.dim_provider p ON p.provider_key = f.provider_key
JOIN analytics.dim_location l ON l.location_key = f.location_key
JOIN analytics.dim_customer c ON c.customer_key = f.customer_key;

CREATE OR REPLACE VIEW analytics.vw_bnpl_overview AS
SELECT
    COUNT(*) AS total_transactions,
    SUM(principal_ngn) AS total_bnpl_amount,
    AVG(principal_ngn) AS average_loan,
    AVG(CASE WHEN default_30d THEN 1.0 ELSE 0.0 END) AS default_30d_rate,
    AVG(CASE WHEN default_90d THEN 1.0 ELSE 0.0 END) AS default_90d_rate
FROM analytics.fact_bnpl_transaction;
