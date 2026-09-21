# Architecture and repository audit

## Audit trước refactor

### CURRENT

- Đã có Docker services cho MinIO, PostgreSQL, Spark master + 2 workers, Airflow và Redpanda.
- Đã có Spark transformations cơ bản, star schema, ba ML candidates và JDBC export.
- Đã có Hugging Face streaming ingestion và Kafka-to-Bronze prototype.
- Chỉ train `default_90d`; feature logic nằm trực tiếp trong một job.
- Historical dataset bị publish qua Kafka, khiến batch và streaming responsibility bị trộn.
- Bronze, Silver và Gold đều ghi Parquet; Silver dùng overwrite toàn bộ và silently filter bad rows.
- Không có schema contract tập trung, quarantine, DQ metrics, batch registry hoặc Delta MERGE.
- Airflow khởi động finite Kafka consumer thay vì orchestrate batch-only workflow.
- PostgreSQL export overwrite table có nguy cơ phá constraints.
- Các legacy filenames và shared config bị trùng trách nhiệm.

### TARGET đã thực hiện

- Giữ Docker services, SQL star schema và các ML algorithm hợp lệ.
- Historical ingestion đi trực tiếp vào `bronze/historical_transactions`.
- Fake producer + Kafka + long-running Structured Streaming là streaming path riêng.
- Bronze dùng Parquet; Silver/Gold dùng Delta MERGE với automatic schema evolution tắt.
- Shared package `spark/bnpl_common` quản lý config, Spark, schemas, validation, feature logic, Delta và PostgreSQL metadata.
- Rejected records, DQ metrics, batch registry, 30D/90D models và model registry được bổ sung.
- Redpanda Console phục vụ demo topic; Prometheus/Grafana theo dõi Redpanda và Spark qua profile riêng.
- GitHub Actions chạy bộ Spark tests bằng cùng Docker image với môi trường local.
- Legacy job names tương thích được giữ dưới dạng wrapper. Historical-to-Kafka job cũ bị loại vì xung đột kiến trúc rõ ràng.

## Component view

```text
Airflow batch DAG
  -> Spark cluster
      -> MinIO Bronze Parquet
      -> Gate -> Quarantine / Silver Delta
      -> Gold Delta -> PostgreSQL

Fake producer -> Redpanda -> Spark streaming -> MinIO Bronze Parquet
                                      |
                                      +-> shared validation/features
                                          -> saved models -> PostgreSQL

Redpanda/Spark metrics -> Prometheus -> Grafana
Redpanda topic metadata/messages -> Redpanda Console
```

## Storage decisions

- Bronze immutable/raw-oriented Parquet, phân tách historical và streaming.
- Silver chứa canonical trusted transaction schema.
- Gold Analytics giữ transaction grain và deterministic dimension keys.
- Gold ML là flat table cho Spark MLlib.
- PostgreSQL chỉ phục vụ BI, predictions, model/DQ metadata và registry batch.

## Schema evolution

`schemas.py` chia source columns thành required và optional. Missing/invalid required values bị reject. Extra nullable fields được giữ ở raw, ghi `_unexpected_columns`, và đánh dấu `COMPATIBLE_EXTRA_COLUMNS`; chúng không tự động được merge vào Silver. Breaking contract phải được cập nhật có chủ đích trong schema module và tests.
