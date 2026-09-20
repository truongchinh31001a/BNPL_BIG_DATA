# Data model

## Core grain

Một record Silver/Gold fact bằng một BNPL transaction, định danh bởi `transaction_id`.

## Bronze

Historical metadata: `_ingested_at`, `_source`, `_source_dataset`, `_source_split`, `_batch_id`, `_pipeline_run_id`, `_ingestion_type`.

Streaming bổ sung `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` và `_schema_parse_ok`. Raw JSON luôn ở `value` để audit/reprocess.

## Silver canonical schema

Các trường transaction/customer/merchant/location, financial terms, credit score và hai nullable targets `default_30d`, `default_90d`. Target NULL là hợp lệ cho live streaming events.

## Gold Analytics

- `fact_bnpl_transaction`: transaction key, dimension keys, point-in-time credit score, amounts, engineered measures và targets.
- `dim_customer`: customer ID và first-time flag.
- `dim_date`, `dim_merchant`, `dim_provider`, `dim_location`.

Dimension keys dùng deterministic `xxhash64` để không thay đổi khi incremental batches đến.

## Gold ML

`ml_bnpl_features` chứa identifiers, numeric/categorical features, engineered features, date features và cả hai labels. Feature contract nằm trong `spark/bnpl_common/features.py` và được dùng lại cho streaming inference.

## PostgreSQL

- `analytics.*`: star schema và Power BI views.
- `ml.predictions`, `ml.model_metrics` có `prediction_horizon` 30D/90D.
- `data_quality.data_quality_metrics`: metric theo run/batch/source/layer.
- `pipeline.pipeline_batches`: trạng thái Bronze/Silver/Gold để audit và retry.
